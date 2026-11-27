// The Temporal config store: the relay-config workflow (payloads use the config key).
import { WorkflowUpdateFailedError, type Client } from "@temporalio/client";
import type { RelayConfigStore } from "./store/types.js";
import {
  clientQuery,
  issueTokensUpdate,
  markSignedInUpdate,
  pairCodeQuery,
  pairUpdate,
  pairedQuery,
  registerClientUpdate,
  reposQuery,
  archivedReposQuery,
  setArchivedReposUpdate,
  revokeUpdate,
  rotateRefreshUpdate,
  setPairCodeUpdate,
  setReposUpdate,
  tokenQuery,
  unpairUpdate,
  verifyAgentQuery,
  type TokenPair,
} from "./workflows/relayConfig.js";
import { boundFor } from "./workflows/bound.js";
import { relayConfigWorkflowId, type RelayConfigState, type StoredClient, type StoredToken } from "./workflows/types.js";

export class ConfigStore implements RelayConfigStore {
  constructor(
    private readonly client: Client,
    private readonly deployId: string,
  ) {}

  /** Bind an update input to the relay-config workflow (see workflows/bound.ts). */
  private bound() {
    return boundFor(relayConfigWorkflowId(this.deployId));
  }

  private get handle() {
    return this.client.workflow.getHandle(relayConfigWorkflowId(this.deployId));
  }

  verifyAgent(hash: string): Promise<boolean> {
    return this.handle.query(verifyAgentQuery, hash);
  }

  paired(): Promise<number> {
    return this.handle.query(pairedQuery);
  }

  pair(codeHash: string, secretHash: string, at: Date): Promise<boolean> {
    return this.handle.executeUpdate(pairUpdate, { args: [{ codeHash, secretHash, at: at.toISOString() }, this.bound()] });
  }

  async setPairCode(code: { hash: string; expiresAt: number }): Promise<void> {
    await this.handle.executeUpdate(setPairCodeUpdate, { args: [code, this.bound()] });
  }

  /** True when the hash is the live pairing code (a query: no history event). */
  pairCodeValid(codeHash: string): Promise<boolean> {
    return this.handle.query(pairCodeQuery, codeHash);
  }

  /** Forget a paired secret. False when it was not paired (the validator refuses it). */
  async unpair(secretHash: string): Promise<boolean> {
    try {
      return await this.handle.executeUpdate(unpairUpdate, { args: [{ secretHash }, this.bound()] });
    } catch (error) {
      if (error instanceof WorkflowUpdateFailedError) return false;
      throw error;
    }
  }

  async setRepos(repos: string[], at: Date): Promise<void> {
    await this.handle.executeUpdate(setReposUpdate, { args: [{ repos, at: at.toISOString() }, this.bound()] });
  }

  repos(): Promise<RelayConfigState["repos"]> {
    return this.handle.query(reposQuery);
  }

  async setArchivedRepos(repos: string[], at: Date): Promise<void> {
    await this.handle.executeUpdate(setArchivedReposUpdate, { args: [{ repos, at: at.toISOString() }, this.bound()] });
  }

  archivedRepos(): Promise<{ repos: string[]; at: string } | null> {
    return this.handle.query(archivedReposQuery);
  }

  getClient(id: string): Promise<StoredClient | null> {
    return this.handle.query(clientQuery, id);
  }

  registerClient(client: StoredClient["client"]): Promise<boolean> {
    return this.handle.executeUpdate(registerClientUpdate, { args: [client, this.bound()] });
  }

  async markSignedIn(clientId: string): Promise<void> {
    await this.handle.executeUpdate(markSignedInUpdate, { args: [clientId, this.bound()] });
  }

  async issueTokens(pair: TokenPair): Promise<void> {
    await this.handle.executeUpdate(issueTokensUpdate, { args: [pair, this.bound()] });
  }

  rotateRefresh(oldHash: string, clientId: string, next: TokenPair): Promise<StoredToken | null> {
    return this.handle.executeUpdate(rotateRefreshUpdate, { args: [{ oldHash, clientId, next }, this.bound()] });
  }

  token(kind: "access" | "refresh", hash: string): Promise<StoredToken | null> {
    return this.handle.query(tokenQuery, kind, hash);
  }

  async revoke(hash: string, clientId: string): Promise<void> {
    await this.handle.executeUpdate(revokeUpdate, { args: [{ hash, clientId }, this.bound()] });
  }
}

