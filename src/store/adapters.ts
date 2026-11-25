// The views of a store that the relay's parts use: the agent hub's backend
// and the OAuth provider's store. They work with either store.
import type { OAuthClientInformationFull } from "@modelcontextprotocol/sdk/shared/auth.js";
import type { OAuthStore } from "../oauth.js";
import type { AgentBackend } from "../backend.js";
import type { Attachments } from "../attachments.js";
import type { Message, SpecAddress, StoredClient } from "../workflows/types.js";
import type { RelayConfigStore, SpecStore } from "./types.js";

/** The OAuth provider's store in the relay's config store (R9.S4 of studio-relay). */
export function storeOAuthStore(store: RelayConfigStore): OAuthStore {
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

/** The agent hub's backend on a store. */
export function storeAgentBackend(deps: { specs: SpecStore; config: RelayConfigStore; attachments: Attachments }): AgentBackend {
  return {
    uploadUrl: (spec, filename, contentType, size) => deps.attachments.uploadUrl(spec, filename, contentType, size),
    downloadUrl: (objectKey) => deps.attachments.downloadUrl(objectKey),
    verifyAgent: (hash) => deps.config.verifyAgent(hash),
    hello: (repos, at) => deps.config.setRepos(repos, at),
    pair: (codeHash, secretHash, at) => deps.config.pair(codeHash, secretHash, at),
    unpair: (secretHash) => deps.config.unpair(secretHash),
    pairCodeValid: (codeHash) => deps.config.pairCodeValid(codeHash),
    sync: (spec, input) => deps.specs.sync(spec, input),
    gone: (spec) => deps.specs.gone(spec),
    state: (spec, state, turn) => deps.specs.refresh(spec, { state, turn }),
    ack: async (spec, seq, error) => {
      await deps.specs.ack(spec, seq, error);
    },
    async pendingForMac() {
      const listed = await deps.specs.list({ pendingForMac: true });
      const out: Array<{ spec: SpecAddress; message: Message }> = [];
      for (const item of listed) {
        const state = await deps.specs.state(item);
        if (!state) continue;
        for (const message of [...state.mac.messages].sort((a, b) => a.seq - b.seq)) {
          out.push({ spec: state.spec, message });
        }
      }
      return out;
    },
  };
}
