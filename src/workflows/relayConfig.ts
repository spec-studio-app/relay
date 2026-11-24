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
import { checkBound, type Bound } from "./bound.js";
import { MAX_AGENTS, MAX_CLIENTS, relayConfigWorkflowId, type RelayConfigState, type StoredClient, type StoredToken } from "./types.js";

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

/** The unpair validator's refusal: the secret is not paired. */
export const NOT_PAIRED = "The secret is not paired.";

export const setPairCodeUpdate = defineUpdate<void, [{ hash: string; expiresAt: number }, Bound]>("pairCode");
export const pairUpdate = defineUpdate<boolean, [{ codeHash: string; secretHash: string; at: string }, Bound]>("pair");
export const unpairUpdate = defineUpdate<boolean, [{ secretHash: string }, Bound]>("unpair");
export const registerClientUpdate = defineUpdate<boolean, [StoredClient["client"], Bound]>("registerClient");
export const markSignedInUpdate = defineUpdate<void, [string, Bound]>("markSignedIn");
export const issueTokensUpdate = defineUpdate<void, [TokenPair, Bound]>("issueTokens");
/** Rotate a refresh token: the old one stops working. False when it is not valid. */
export const rotateRefreshUpdate = defineUpdate<StoredToken | null, [{ oldHash: string; clientId: string; next: TokenPair }, Bound]>(
  "rotateRefresh",
);
export const revokeUpdate = defineUpdate<void, [{ hash: string; clientId: string }, Bound]>("revoke");
export const setReposUpdate = defineUpdate<void, [{ repos: string[]; at: string }, Bound]>("setRepos");

export const verifyAgentQuery = defineQuery<boolean, [string]>("verifyAgent");
export const tokenQuery = defineQuery<StoredToken | null, ["access" | "refresh", string]>("token");
export const clientQuery = defineQuery<StoredClient | null, [string]>("client");
export const reposQuery = defineQuery<RelayConfigState["repos"]>("repos");
export const pairedQuery = defineQuery<number>("paired");
/** True when the hash is the live pairing code. It does not use the code. */
export const pairCodeQuery = defineQuery<boolean, [string]>("pairCodeValid");

/** Every update of the relay-config workflow is bound to it (see bound.ts). */
const bound = { validator: (_: unknown, b: Bound) => checkBound(b) };

export async function relayConfigWorkflow(input: RelayConfigState): Promise<void> {
  const s = input;
  let updates = 0;
  // A start input copied from another deploy's workflow: end at once.
  if (workflowInfo().workflowId !== relayConfigWorkflowId(s.deployId)) return;

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
  }, bound);

  setHandler(pairUpdate, ({ codeHash, secretHash, at }) => {
    sweep();
    if (!s.pairCode || s.pairCode.hash !== codeHash) return false;
    s.pairCode = null;
    s.agents.push({ hash: secretHash, pairedAt: at });
    if (s.agents.length > MAX_AGENTS) s.agents.splice(0, s.agents.length - MAX_AGENTS);
    changed();
    return true;
  }, bound);

  // The validator refuses an unpair that removes nothing, so it adds no event to the history.
  setHandler(
    unpairUpdate,
    ({ secretHash }) => {
      const before = s.agents.length;
      s.agents = s.agents.filter((a) => a.hash !== secretHash);
      if (s.agents.length === before) return false;
      changed();
      return true;
    },
    {
      validator: ({ secretHash }, b) => {
        checkBound(b);
        if (!s.agents.some((a) => a.hash === secretHash)) throw new Error(NOT_PAIRED);
      },
    },
  );

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
  }, bound);

  setHandler(markSignedInUpdate, (clientId) => {
    const stored = s.clients.find((c) => c.client.client_id === clientId);
    if (stored) stored.signedIn = true;
    changed();
  }, bound);

  setHandler(issueTokensUpdate, ({ access, refresh }) => {
    s.access[access.hash] = access.token;
    s.refresh[refresh.hash] = refresh.token;
    changed();
  }, bound);

  setHandler(rotateRefreshUpdate, ({ oldHash, clientId, next }) => {
    const stored = live(s.refresh[oldHash]);
    if (!stored || stored.clientId !== clientId) return null;
    delete s.refresh[oldHash];
    s.access[next.access.hash] = inherit(stored, next.access.token, next.inherit);
    s.refresh[next.refresh.hash] = inherit(stored, next.refresh.token, next.inherit);
    changed();
    return stored;
  }, bound);

  setHandler(revokeUpdate, ({ hash, clientId }) => {
    for (const map of [s.access, s.refresh]) {
      if (map[hash]?.clientId === clientId) delete map[hash];
    }
    changed();
  }, bound);

  setHandler(setReposUpdate, (repos) => {
    s.repos = repos;
    changed();
  }, bound);

  await condition(() => updates >= s.updateLimit || workflowInfo().continueAsNewSuggested);
  await condition(allHandlersFinished);
  sweep();
  await continueAsNew<typeof relayConfigWorkflow>(s);
}
