// The relay's side of the relay-config workflow (payloads use the config key).
import type { Client } from "@temporalio/client";
import {
  clientQuery,
  issueTokensUpdate,
  markSignedInUpdate,
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

  unpair(secretHash: string): Promise<boolean> {
    return this.handle.executeUpdate(unpairUpdate, { args: [{ secretHash }] });
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
