// The JSON config store: <data>/config.json, with the same rules as the
// relay-config workflow (workflows/configRules.ts). Only the relay process
// writes config.json, and its calls wait in one queue, so no lock between
// processes is needed. `relay pair-code` (another process) writes only
// <data>/pair-code.json, each time with a new ID; the relay takes a code with
// a new ID as its pairing code at its next call. No process removes the file,
// so no write of one process can undo a write of the other.
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import * as rules from "../../workflows/configRules.js";
import type { TokenPair } from "../../workflows/configRules.js";
import { initialRelayConfigState, type RelayConfigState, type StoredClient, type StoredToken } from "../../workflows/types.js";
import type { RelayConfigStore } from "../types.js";
import { readText, writeAtomic, type WriteHooks } from "./files.js";

interface ConfigFile {
  version: 1;
  state: RelayConfigState;
  /** The ID of the last pair-code.json that the relay took. */
  pairCodeId?: string;
}

/** A pairing code from `relay pair-code`. */
interface PairCodeFile {
  version: 1;
  id: string;
  hash: string;
  expiresAt: number;
}
export class ConfigFileError extends Error {
  constructor() {
    super("The relay cannot read config.json in RELAY_DATA_DIR. Correct the file, or remove it to pair the Mac again.");
  }
}

/** A change to a data folder with no config.json: the relay never started on this folder. */
export class ConfigMissingError extends Error {
  constructor() {
    super("RELAY_DATA_DIR has no config.json. Start the relay on this folder first, or use the relay's RELAY_DATA_DIR.");
  }
}

export class JsonConfigStore implements RelayConfigStore {
  /** The calls of this process that change config.json wait here, one after the other. */
  private queue: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly o: { dir: string; deployId: string; now?: () => number; writeHooks?: WriteHooks },
  ) {}

  get path(): string {
    return join(this.o.dir, "config.json");
  }

  get pairCodePath(): string {
    return join(this.o.dir, "pair-code.json");
  }

  private now(): number {
    return this.o.now?.() ?? Date.now();
  }

  /** config.json, or null when the file does not exist. */
  private async readFile(): Promise<ConfigFile | null> {
    const text = await readText(this.path);
    if (text === null) return null;
    try {
      const file = JSON.parse(text) as ConfigFile;
      if (file?.version !== 1 || !file.state || !Array.isArray(file.state.agents)) throw new Error("not a config file");
      return file;
    } catch {
      throw new ConfigFileError();
    }
  }

  /** The pairing code from `relay pair-code`, or null. A file that does not parse counts as none. */
  private async readPairCode(): Promise<PairCodeFile | null> {
    const text = await readText(this.pairCodePath);
    if (text === null) return null;
    try {
      const file = JSON.parse(text) as PairCodeFile;
      if (file?.version !== 1 || typeof file.id !== "string" || typeof file.hash !== "string" || typeof file.expiresAt !== "number") {
        return null;
      }
      return file;
    } catch {
      return null;
    }
  }

  /** config.json with a new code from `relay pair-code` applied. `taken` is true when it applied one. */
  private async load(): Promise<{ file: ConfigFile; taken: boolean } | null> {
    const file = await this.readFile();
    if (!file) return null;
    const code = await this.readPairCode();
    if (!code || code.id === file.pairCodeId) return { file, taken: false };
    rules.setPairCode(file.state, { hash: code.hash, expiresAt: code.expiresAt });
    file.pairCodeId = code.id;
    return { file, taken: true };
  }

  /** The config, or null when config.json does not exist. */
  async read(): Promise<RelayConfigState | null> {
    return (await this.load())?.file.state ?? null;
  }

  private async current(): Promise<RelayConfigState> {
    return (await this.read()) ?? initialRelayConfigState(this.o.deployId, null);
  }

  /**
   * Make the config when it does not exist. True when this call made it.
   * The code is the first pairing code.
   */
  async create(pairCode: { hash: string; expiresAt: number }): Promise<boolean> {
    return this.queued(async () => {
      if ((await this.readFile()) !== null) return false;
      // A pair-code.json from before stays taken: the new code is the live one.
      const old = await this.readPairCode();
      await this.write({ version: 1, state: initialRelayConfigState(this.o.deployId, pairCode), ...(old ? { pairCodeId: old.id } : {}) });
      return true;
    });
  }

  private write(file: ConfigFile): Promise<void> {
    return writeAtomic(this.path, JSON.stringify(file), this.o.writeHooks);
  }

  private queued<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.queue.then(fn);
    this.queue = run.catch(() => undefined);
    return run;
  }

  /**
   * Read, apply the rule, and write when it changed something (with a sweep, as
   * the workflow), or when a new code from `relay pair-code` came. The relay's
   * start makes config.json; a change without it fails, as a Temporal update
   * fails without the relay-config workflow.
   */
  private change<T>(fn: (s: RelayConfigState) => { result: T; changed: boolean }): Promise<T> {
    return this.queued(async () => {
      const loaded = await this.load();
      if (loaded === null) throw new ConfigMissingError();
      const { file, taken } = loaded;
      const out = fn(file.state);
      if (out.changed || taken) {
        rules.sweep(file.state, this.now());
        await this.write(file);
      }
      return out.result;
    });
  }

  async verifyAgent(hash: string): Promise<boolean> {
    return rules.verifyAgent(await this.current(), hash);
  }

  async paired(): Promise<number> {
    return (await this.current()).agents.length;
  }

  pair(codeHash: string, secretHash: string, at: Date): Promise<boolean> {
    return this.change((s) => rules.pair(s, { codeHash, secretHash, at: at.toISOString() }, this.now()));
  }

  async pairCodeValid(codeHash: string): Promise<boolean> {
    return rules.pairCodeValid(await this.current(), codeHash, this.now());
  }

  /**
   * A new pairing code (`relay pair-code`, in its own process): write
   * pair-code.json with a new ID. The relay takes it at its next call.
   */
  async setPairCode(code: { hash: string; expiresAt: number }): Promise<void> {
    if ((await this.readFile()) === null) throw new ConfigMissingError();
    const file: PairCodeFile = { version: 1, id: randomUUID(), hash: code.hash, expiresAt: code.expiresAt };
    await writeAtomic(this.pairCodePath, JSON.stringify(file), this.o.writeHooks);
  }

  unpair(secretHash: string): Promise<boolean> {
    return this.change((s) => rules.unpair(s, secretHash));
  }

  setRepos(repos: string[], at: Date): Promise<void> {
    return this.change((s) => rules.setRepos(s, { repos, at: at.toISOString() }));
  }

  async repos(): Promise<RelayConfigState["repos"]> {
    return (await this.current()).repos;
  }

  setArchivedRepos(repos: string[], at: Date): Promise<void> {
    return this.change((s) => rules.setArchivedRepos(s, { repos, at: at.toISOString() }));
  }

  async archivedRepos(): Promise<{ repos: string[]; at: string } | null> {
    return (await this.current()).archivedRepos ?? null;
  }

  async getClient(id: string): Promise<StoredClient | null> {
    return rules.client(await this.current(), id);
  }

  registerClient(client: StoredClient["client"]): Promise<boolean> {
    return this.change((s) => rules.registerClient(s, client));
  }

  markSignedIn(clientId: string): Promise<void> {
    return this.change((s) => rules.markSignedIn(s, clientId));
  }

  issueTokens(pair: TokenPair): Promise<void> {
    return this.change((s) => rules.issueTokens(s, pair));
  }

  rotateRefresh(oldHash: string, clientId: string, next: TokenPair): Promise<StoredToken | null> {
    return this.change((s) => rules.rotateRefresh(s, { oldHash, clientId, next }, this.now()));
  }

  async token(kind: "access" | "refresh", hash: string): Promise<StoredToken | null> {
    return rules.token(await this.current(), kind, hash, this.now());
  }

  revoke(hash: string, clientId: string): Promise<void> {
    return this.change((s) => rules.revoke(s, { hash, clientId }));
  }
}
