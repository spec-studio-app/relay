// One workflow per deploy: the agent secret hashes, the pairing code hash, the
// OAuth clients and the token hashes, and the repos from the agent's last hello.
// It keeps only hashes. Its payloads use the config key of the codec.
import {
  allHandlersFinished,
  condition,
  continueAsNew,
  defineQuery,
  defineUpdate,
  setHandler,
  workflowInfo,
} from "@temporalio/workflow";
import { MAX_AGENTS, MAX_CLIENTS, type RelayConfigState, type StoredClient, type StoredToken } from "./types.js";

export interface TokenPair {
  access: { hash: string; token: StoredToken };
  refresh: { hash: string; token: StoredToken };
  /** On a rotation: copy these fields from the old refresh token. */
  inherit?: { scopes: boolean; resource: boolean };
}

function inherit(old: StoredToken, token: StoredToken, how: TokenPair["inherit"]): StoredToken {
  const out = { ...token };
  if (how?.scopes) out.scopes = old.scopes;
  if (how?.resource) {
    if (old.resource) out.resource = old.resource;
    else delete out.resource;
  }
  return out;
}

export const setPairCodeUpdate = defineUpdate<void, [{ hash: string; expiresAt: number }]>("pairCode");
export const pairUpdate = defineUpdate<boolean, [{ codeHash: string; secretHash: string; at: string }]>("pair");
export const unpairUpdate = defineUpdate<boolean, [{ secretHash: string }]>("unpair");
export const registerClientUpdate = defineUpdate<boolean, [StoredClient["client"]]>("registerClient");
export const markSignedInUpdate = defineUpdate<void, [string]>("markSignedIn");
export const issueTokensUpdate = defineUpdate<void, [TokenPair]>("issueTokens");
/** Rotate a refresh token: the old one stops working. False when it is not valid. */
export const rotateRefreshUpdate = defineUpdate<StoredToken | null, [{ oldHash: string; clientId: string; next: TokenPair }]>(
  "rotateRefresh",
);
export const revokeUpdate = defineUpdate<void, [{ hash: string; clientId: string }]>("revoke");
export const setReposUpdate = defineUpdate<void, [{ repos: string[]; at: string }]>("setRepos");

export const verifyAgentQuery = defineQuery<boolean, [string]>("verifyAgent");
export const tokenQuery = defineQuery<StoredToken | null, ["access" | "refresh", string]>("token");
export const clientQuery = defineQuery<StoredClient | null, [string]>("client");
export const reposQuery = defineQuery<RelayConfigState["repos"]>("repos");
export const pairedQuery = defineQuery<number>("paired");
/** True when the hash is the live pairing code. It does not use the code. */
export const pairCodeQuery = defineQuery<boolean, [string]>("pairCodeValid");

export async function relayConfigWorkflow(input: RelayConfigState): Promise<void> {
  const s = input;
  let updates = 0;

  const sweep = () => {
    const now = Date.now();
    for (const map of [s.access, s.refresh]) {
      for (const [hash, token] of Object.entries(map)) if (token.expiresAt <= now) delete map[hash];
    }
    if (s.pairCode && s.pairCode.expiresAt <= now) s.pairCode = null;
  };
  const changed = () => {
    updates++;
    sweep();
  };
  const live = (token: StoredToken | undefined): StoredToken | null =>
    token && token.expiresAt > Date.now() ? token : null;

  setHandler(verifyAgentQuery, (hash) => s.agents.some((a) => a.hash === hash));
  setHandler(tokenQuery, (kind, hash) => live((kind === "access" ? s.access : s.refresh)[hash]));
  setHandler(clientQuery, (id) => s.clients.find((c) => c.client.client_id === id) ?? null);
  setHandler(reposQuery, () => s.repos);
  setHandler(pairedQuery, () => s.agents.length);
  setHandler(pairCodeQuery, (hash) => !!s.pairCode && s.pairCode.expiresAt > Date.now() && s.pairCode.hash === hash);

  setHandler(setPairCodeUpdate, (code) => {
    s.pairCode = code;
    changed();
  });

  setHandler(pairUpdate, ({ codeHash, secretHash, at }) => {
    sweep();
    if (!s.pairCode || s.pairCode.hash !== codeHash) return false;
    s.pairCode = null;
    s.agents.push({ hash: secretHash, pairedAt: at });
    if (s.agents.length > MAX_AGENTS) s.agents.splice(0, s.agents.length - MAX_AGENTS);
    changed();
    return true;
  });

  setHandler(unpairUpdate, ({ secretHash }) => {
    const before = s.agents.length;
    s.agents = s.agents.filter((a) => a.hash !== secretHash);
    changed();
    return s.agents.length < before;
  });

  setHandler(registerClientUpdate, (client) => {
    if (s.clients.length >= MAX_CLIENTS) {
      // Push out the oldest client that never signed in, so that anonymous
      // registrations cannot push out the user's own clients.
      const index = s.clients.findIndex((c) => !c.signedIn);
      if (index < 0) return false;
      s.clients.splice(index, 1);
    }
    s.clients.push({ client, signedIn: false });
    changed();
    return true;
  });

  setHandler(markSignedInUpdate, (clientId) => {
    const stored = s.clients.find((c) => c.client.client_id === clientId);
    if (stored) stored.signedIn = true;
    changed();
  });

  setHandler(issueTokensUpdate, ({ access, refresh }) => {
    s.access[access.hash] = access.token;
    s.refresh[refresh.hash] = refresh.token;
    changed();
  });

  setHandler(rotateRefreshUpdate, ({ oldHash, clientId, next }) => {
    const stored = live(s.refresh[oldHash]);
    if (!stored || stored.clientId !== clientId) return null;
    delete s.refresh[oldHash];
    s.access[next.access.hash] = inherit(stored, next.access.token, next.inherit);
    s.refresh[next.refresh.hash] = inherit(stored, next.refresh.token, next.inherit);
    changed();
    return stored;
  });

  setHandler(revokeUpdate, ({ hash, clientId }) => {
    for (const map of [s.access, s.refresh]) {
      if (map[hash]?.clientId === clientId) delete map[hash];
    }
    changed();
  });

  setHandler(setReposUpdate, (repos) => {
    s.repos = repos;
    changed();
  });

  await condition(() => updates >= s.updateLimit || workflowInfo().continueAsNewSuggested);
  await condition(allHandlersFinished);
  sweep();
  await continueAsNew<typeof relayConfigWorkflow>(s);
}
