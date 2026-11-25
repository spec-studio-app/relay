// The JSON config store: <data>/config.json, with the same rules as the
// relay-config workflow (workflows/configRules.ts). It keeps no copy in memory:
// each call reads the file, and each change takes the file lock, so a pairing
// code from `relay pair-code` in another process takes effect at once.
import { join } from "node:path";
import * as rules from "../../workflows/configRules.js";
import type { TokenPair } from "../../workflows/configRules.js";
import { initialRelayConfigState, type RelayConfigState, type StoredClient, type StoredToken } from "../../workflows/types.js";
import type { RelayConfigStore } from "../types.js";
import { readText, withFileLock, writeAtomic, type WriteHooks } from "./files.js";

interface ConfigFile {
  version: 1;
  state: RelayConfigState;
}

export class ConfigFileError extends Error {
  constructor() {
    super("The relay cannot read config.json in RELAY_DATA_DIR. Correct the file, or remove it to pair the Mac again.");
  }
}

export class JsonConfigStore implements RelayConfigStore {
  /** In-process queue: the calls of this process wait here, not on the lock file. */
  private queue: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly o: { dir: string; deployId: string; now?: () => number; writeHooks?: WriteHooks },
  ) {}

  get path(): string {
    return join(this.o.dir, "config.json");
  }

  private now(): number {
    return this.o.now?.() ?? Date.now();
  }

  /** The config, or null when the file does not exist. */
  async read(): Promise<RelayConfigState | null> {
    const text = await readText(this.path);
    if (text === null) return null;
    try {
      const file = JSON.parse(text) as ConfigFile;
      if (file?.version !== 1 || !file.state || !Array.isArray(file.state.agents)) throw new Error("not a config file");
      return file.state;
    } catch {
      throw new ConfigFileError();
    }
  }

  private async current(): Promise<RelayConfigState> {
    return (await this.read()) ?? initialRelayConfigState(this.o.deployId, null);
  }

  /**
   * Make the config when it does not exist. True when this call made it.
   * The code is the first pairing code.
   */
  async create(pairCode: { hash: string; expiresAt: number }): Promise<boolean> {
    return this.locked(async () => {
      if ((await this.read()) !== null) return false;
      await this.write(initialRelayConfigState(this.o.deployId, pairCode));
      return true;
    });
  }

  private write(state: RelayConfigState): Promise<void> {
    const file: ConfigFile = { version: 1, state };
    return writeAtomic(this.path, JSON.stringify(file), this.o.writeHooks);
  }

  private locked<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.queue.then(() => withFileLock(this.path, fn));
    this.queue = run.catch(() => undefined);
    return run;
  }

  /** Read, apply the rule, and write when it changed something (with a sweep, as the workflow). */
  private change<T>(fn: (s: RelayConfigState) => { result: T; changed: boolean }): Promise<T> {
    return this.locked(async () => {
      const s = await this.current();
      const out = fn(s);
      if (out.changed) {
        rules.sweep(s, this.now());
        await this.write(s);
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

  setPairCode(code: { hash: string; expiresAt: number }): Promise<void> {
    return this.change((s) => rules.setPairCode(s, code));
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
