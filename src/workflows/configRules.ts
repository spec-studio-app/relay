// The rules of the relay's config (pairing, repos, OAuth clients and tokens),
// as plain functions on its state. The relay-config workflow (Temporal store)
// and the JSON store both call them. Each change function returns `changed`:
// true when the store counts an update (and sweeps the expired values).
//
// This file runs in the workflow sandbox too: keep it free of Node APIs. The
// time is `now` (ms since the epoch): Date.now() in the workflow.
import { MAX_AGENTS, MAX_CLIENTS, type RelayConfigState, type StoredClient, type StoredToken } from "./types.js";

export interface TokenPair {
  access: { hash: string; token: StoredToken };
  refresh: { hash: string; token: StoredToken };
  /** On a rotation: copy these fields from the old refresh token. */
  inherit?: { scopes: boolean; resource: boolean };
}

/** The unpair refusal: the secret is not paired. */
export const NOT_PAIRED = "The secret is not paired.";

type Change<T> = { result: T; changed: boolean };

function inherit(old: StoredToken, token: StoredToken, how: TokenPair["inherit"]): StoredToken {
  const out = { ...token };
  if (how?.scopes) out.scopes = old.scopes;
  if (how?.resource) {
    if (old.resource) out.resource = old.resource;
    else delete out.resource;
  }
  return out;
}

/** Remove the expired tokens and pairing code. */
export function sweep(s: RelayConfigState, now: number): void {
  for (const map of [s.access, s.refresh]) {
    for (const [hash, token] of Object.entries(map)) if (token.expiresAt <= now) delete map[hash];
  }
  if (s.pairCode && s.pairCode.expiresAt <= now) s.pairCode = null;
}

function live(token: StoredToken | undefined, now: number): StoredToken | null {
  return token && token.expiresAt > now ? token : null;
}

// ---- reads ----

export function verifyAgent(s: RelayConfigState, hash: string): boolean {
  return s.agents.some((a) => a.hash === hash);
}

export function token(s: RelayConfigState, kind: "access" | "refresh", hash: string, now: number): StoredToken | null {
  return live((kind === "access" ? s.access : s.refresh)[hash], now);
}

export function client(s: RelayConfigState, id: string): StoredClient | null {
  return s.clients.find((c) => c.client.client_id === id) ?? null;
}

export function pairCodeValid(s: RelayConfigState, hash: string, now: number): boolean {
  return !!s.pairCode && s.pairCode.expiresAt > now && s.pairCode.hash === hash;
}

export function isPaired(s: RelayConfigState, secretHash: string): boolean {
  return s.agents.some((a) => a.hash === secretHash);
}

// ---- changes ----

export function setPairCode(s: RelayConfigState, code: { hash: string; expiresAt: number }): Change<void> {
  s.pairCode = code;
  return { result: undefined, changed: true };
}

/** Pair a Mac with the live code. The code works once. */
export function pair(
  s: RelayConfigState,
  input: { codeHash: string; secretHash: string; at: string },
  now: number,
): Change<boolean> {
  sweep(s, now);
  if (!s.pairCode || s.pairCode.hash !== input.codeHash) return { result: false, changed: false };
  s.pairCode = null;
  s.agents.push({ hash: input.secretHash, pairedAt: input.at });
  if (s.agents.length > MAX_AGENTS) s.agents.splice(0, s.agents.length - MAX_AGENTS);
  return { result: true, changed: true };
}

export function unpair(s: RelayConfigState, secretHash: string): Change<boolean> {
  const before = s.agents.length;
  s.agents = s.agents.filter((a) => a.hash !== secretHash);
  if (s.agents.length === before) return { result: false, changed: false };
  return { result: true, changed: true };
}

export function registerClient(s: RelayConfigState, c: StoredClient["client"]): Change<boolean> {
  if (s.clients.length >= MAX_CLIENTS) {
    // Push out the oldest client that never signed in, so that anonymous
    // registrations cannot push out the user's own clients.
    const index = s.clients.findIndex((x) => !x.signedIn);
    if (index < 0) return { result: false, changed: false };
    s.clients.splice(index, 1);
  }
  s.clients.push({ client: c, signedIn: false });
  return { result: true, changed: true };
}

export function markSignedIn(s: RelayConfigState, clientId: string): Change<void> {
  const stored = s.clients.find((c) => c.client.client_id === clientId);
  if (stored) stored.signedIn = true;
  return { result: undefined, changed: true };
}

export function issueTokens(s: RelayConfigState, pair: TokenPair): Change<void> {
  s.access[pair.access.hash] = pair.access.token;
  s.refresh[pair.refresh.hash] = pair.refresh.token;
  return { result: undefined, changed: true };
}

/** Rotate a refresh token: the old one stops working. Null when it is not valid. */
export function rotateRefresh(
  s: RelayConfigState,
  input: { oldHash: string; clientId: string; next: TokenPair },
  now: number,
): Change<StoredToken | null> {
  const stored = live(s.refresh[input.oldHash], now);
  if (!stored || stored.clientId !== input.clientId) return { result: null, changed: false };
  delete s.refresh[input.oldHash];
  s.access[input.next.access.hash] = inherit(stored, input.next.access.token, input.next.inherit);
  s.refresh[input.next.refresh.hash] = inherit(stored, input.next.refresh.token, input.next.inherit);
  return { result: stored, changed: true };
}

export function revoke(s: RelayConfigState, input: { hash: string; clientId: string }): Change<void> {
  for (const map of [s.access, s.refresh]) {
    if (map[input.hash]?.clientId === input.clientId) delete map[input.hash];
  }
  return { result: undefined, changed: true };
}

export function setRepos(s: RelayConfigState, repos: { repos: string[]; at: string }): Change<void> {
  s.repos = repos;
  return { result: undefined, changed: true };
}
