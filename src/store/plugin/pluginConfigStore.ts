// The plugin config store: the one document relay / config, with the rules of
// workflows/configRules.ts. Each change is a read, apply, write with the read
// version, so `relay pair-code` (another process) and the relay never undo
// each other's writes.
import * as rules from "../../workflows/configRules.js";
import type { TokenPair } from "../../workflows/configRules.js";
import { initialRelayConfigState, type RelayConfigState, type StoredClient, type StoredToken } from "../../workflows/types.js";
import { ConfigMissingError } from "../json/jsonConfigStore.js";
import type { RelayConfigStore } from "../types.js";
import { OTHER_DEPLOY_TEXT, type Docs } from "./docs.js";
import { StoreRefusedError } from "../errors.js";

export const CONFIG_COLLECTION = "relay";
export const CONFIG_KEY = "config";

interface ConfigDoc {
  version: 1;
  state: RelayConfigState;
}

/** A change before the relay made its config: the relay never started on this storage. */
export class PluginConfigMissingError extends ConfigMissingError {
  constructor() {
    super();
    this.message = "The plugin has no relay config. Start the relay with the same plugin settings first, then try again.";
  }
}

export class PluginConfigStore implements RelayConfigStore {
  constructor(
    private readonly docs: Docs,
    private readonly o: { deployId: string; now?: () => number },
  ) {}

  private now(): number {
    return this.o.now?.() ?? Date.now();
  }

  /** The config, or null when the plugin has none. */
  async read(): Promise<RelayConfigState | null> {
    const doc = await this.docs.get<ConfigDoc>(CONFIG_COLLECTION, CONFIG_KEY);
    if (doc && doc.value.state.deployId !== this.o.deployId) throw new StoreRefusedError(OTHER_DEPLOY_TEXT);
    return doc?.value.state ?? null;
  }

  private async current(): Promise<RelayConfigState> {
    return (await this.read()) ?? initialRelayConfigState(this.o.deployId, null);
  }

  /** Make the config when it does not exist. True when this call made it. */
  create(pairCode: { hash: string; expiresAt: number }): Promise<boolean> {
    const doc: ConfigDoc = { version: 1, state: initialRelayConfigState(this.o.deployId, pairCode) };
    return this.docs.create(CONFIG_COLLECTION, CONFIG_KEY, doc, { deployId: this.o.deployId });
  }

  /** Read, apply the rule, and write when it changed something (with a sweep, as the workflow). */
  private change<T>(fn: (s: RelayConfigState) => { result: T; changed: boolean }): Promise<T> {
    return this.docs.update<ConfigDoc, T>(CONFIG_COLLECTION, CONFIG_KEY, (doc) => {
      if (!doc) throw new PluginConfigMissingError();
      if (doc.value.state.deployId !== this.o.deployId) throw new StoreRefusedError(OTHER_DEPLOY_TEXT);
      const state = structuredClone(doc.value.state);
      const out = fn(state);
      if (!out.changed) return { result: out.result };
      rules.sweep(state, this.now());
      return { write: { value: { version: 1, state }, attributes: { deployId: this.o.deployId } }, result: out.result };
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

  async setPairCode(code: { hash: string; expiresAt: number }): Promise<void> {
    await this.change((s) => rules.setPairCode(s, code));
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
