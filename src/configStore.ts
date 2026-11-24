// The relay's side of the relay-config workflow (payloads use the config key).
import { WorkflowUpdateFailedError, type Client } from "@temporalio/client";
import type { OAuthClientInformationFull } from "@modelcontextprotocol/sdk/shared/auth.js";
import type { OAuthStore } from "./oauth.js";
import {
  clientQuery,
  issueTokensUpdate,
  markSignedInUpdate,
  pairCodeQuery,
  pairUpdate,
  pairedQuery,
  registerClientUpdate,
  reposQuery,
  revokeUpdate,
  rotateRefreshUpdate,
  setReposUpdate,
  tokenQuery,
  unpairUpdate,
  verifyAgentQuery,
  type TokenPair,
} from "./workflows/relayConfig.js";
import { relayConfigWorkflowId, type RelayConfigState, type StoredClient, type StoredToken } from "./workflows/types.js";

export class ConfigStore {
  constructor(
    private readonly client: Client,
    private readonly deployId: string,
  ) {}

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
    return this.handle.executeUpdate(pairUpdate, { args: [{ codeHash, secretHash, at: at.toISOString() }] });
  }

  /** True when the hash is the live pairing code (a query: no history event). */
  pairCodeValid(codeHash: string): Promise<boolean> {
    return this.handle.query(pairCodeQuery, codeHash);
  }

  /** Forget a paired secret. False when it was not paired (the validator refuses it). */
  async unpair(secretHash: string): Promise<boolean> {
    try {
      return await this.handle.executeUpdate(unpairUpdate, { args: [{ secretHash }] });
    } catch (error) {
      if (error instanceof WorkflowUpdateFailedError) return false;
      throw error;
    }
  }

  async setRepos(repos: string[], at: Date): Promise<void> {
    await this.handle.executeUpdate(setReposUpdate, { args: [{ repos, at: at.toISOString() }] });
  }

  repos(): Promise<RelayConfigState["repos"]> {
    return this.handle.query(reposQuery);
  }

  getClient(id: string): Promise<StoredClient | null> {
    return this.handle.query(clientQuery, id);
  }

  registerClient(client: StoredClient["client"]): Promise<boolean> {
    return this.handle.executeUpdate(registerClientUpdate, { args: [client] });
  }

  async markSignedIn(clientId: string): Promise<void> {
    await this.handle.executeUpdate(markSignedInUpdate, { args: [clientId] });
  }

  async issueTokens(pair: TokenPair): Promise<void> {
    await this.handle.executeUpdate(issueTokensUpdate, { args: [pair] });
  }

  rotateRefresh(oldHash: string, clientId: string, next: TokenPair): Promise<StoredToken | null> {
    return this.handle.executeUpdate(rotateRefreshUpdate, { args: [{ oldHash, clientId, next }] });
  }

  token(kind: "access" | "refresh", hash: string): Promise<StoredToken | null> {
    return this.handle.query(tokenQuery, kind, hash);
  }

  async revoke(hash: string, clientId: string): Promise<void> {
    await this.handle.executeUpdate(revokeUpdate, { args: [{ hash, clientId }] });
  }
}

/** The OAuth provider's store in the relay-config workflow (R9.S4). */
export function temporalOAuthStore(store: ConfigStore): OAuthStore {
  return {
    async getClient(clientId) {
      const stored = await store.getClient(clientId);
      return stored ? (stored.client as unknown as OAuthClientInformationFull) : undefined;
    },
    registerClient: (client) => store.registerClient(client as unknown as StoredClient["client"]),
    markSignedIn: (clientId) => store.markSignedIn(clientId),
    saveTokens: (pair) => store.issueTokens(pair),
    accessToken: (hash) => store.token("access", hash),
    rotateRefresh: (oldHash, clientId, next) => store.rotateRefresh(oldHash, clientId, next),
    revoke: (hash, clientId) => store.revoke(hash, clientId),
  };
}
