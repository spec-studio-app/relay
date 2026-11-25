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
import { relayConfigWorkflowId, type RelayConfigState, type StoredClient, type StoredToken } from "./types.js";
import * as rules from "./configRules.js";
import type { TokenPair } from "./configRules.js";

export { NOT_PAIRED, type TokenPair } from "./configRules.js";

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

  const sweep = () => rules.sweep(s, Date.now());
  const changed = () => {
    updates++;
    sweep();
  };
  const counted = <T>(out: { result: T; changed: boolean }): T => {
    if (out.changed) changed();
    return out.result;
  };

  setHandler(verifyAgentQuery, (hash) => rules.verifyAgent(s, hash));
  setHandler(tokenQuery, (kind, hash) => rules.token(s, kind, hash, Date.now()));
  setHandler(clientQuery, (id) => rules.client(s, id));
  setHandler(reposQuery, () => s.repos);
  setHandler(pairedQuery, () => s.agents.length);
  setHandler(pairCodeQuery, (hash) => rules.pairCodeValid(s, hash, Date.now()));

  setHandler(setPairCodeUpdate, (code) => counted(rules.setPairCode(s, code)), bound);
  setHandler(pairUpdate, (input) => counted(rules.pair(s, input, Date.now())), bound);

  // The validator refuses an unpair that removes nothing, so it adds no event to the history.
  setHandler(unpairUpdate, ({ secretHash }) => counted(rules.unpair(s, secretHash)), {
    validator: ({ secretHash }, b) => {
      checkBound(b);
      if (!rules.isPaired(s, secretHash)) throw new Error(rules.NOT_PAIRED);
    },
  });

  setHandler(registerClientUpdate, (client) => counted(rules.registerClient(s, client)), bound);
  setHandler(markSignedInUpdate, (clientId) => counted(rules.markSignedIn(s, clientId)), bound);
  setHandler(issueTokensUpdate, (pair) => counted(rules.issueTokens(s, pair)), bound);
  setHandler(rotateRefreshUpdate, (input) => counted(rules.rotateRefresh(s, input, Date.now())), bound);
  setHandler(revokeUpdate, (input) => counted(rules.revoke(s, input)), bound);
  setHandler(setReposUpdate, (repos) => counted(rules.setRepos(s, repos)), bound);

  await condition(() => updates >= s.updateLimit || workflowInfo().continueAsNewSuggested);
  await condition(allHandlersFinished);
  sweep();
  await continueAsNew<typeof relayConfigWorkflow>(s);
}
