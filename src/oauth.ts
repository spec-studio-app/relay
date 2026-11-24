// A single-user OAuth 2.1 provider for the SDK's mcpAuthRouter.
//
// The authorize step shows a login page with one password field. The password
// is compared with a bcrypt hash. There is no sign-up. The clients and the
// token hashes live in an OAuthStore: in the relay it is the relay-config
// workflow, so a restart (or a second relay on the same Temporal) keeps the
// sign-in. Login requests and authorization codes live in memory (minutes).
import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import bcrypt from "bcryptjs";
import express, { type Request, type Response, type Router } from "express";
import type { OAuthRegisteredClientsStore } from "@modelcontextprotocol/sdk/server/auth/clients.js";
import type { AuthorizationParams, OAuthServerProvider } from "@modelcontextprotocol/sdk/server/auth/provider.js";
import type { AuthInfo } from "@modelcontextprotocol/sdk/server/auth/types.js";
import { InvalidClientMetadataError, InvalidGrantError, InvalidTokenError } from "@modelcontextprotocol/sdk/server/auth/errors.js";
import type { OAuthClientInformationFull, OAuthTokens } from "@modelcontextprotocol/sdk/shared/auth.js";
import type { Logger } from "./log.js";

export const LOGIN_PATH = "/login";

const PENDING_TTL_MS = 10 * 60 * 1000;
const CODE_TTL_MS = 5 * 60 * 1000;
const ACCESS_TTL_MS = 60 * 60 * 1000;
const REFRESH_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_FAILED_LOGINS = 10;
const FAILED_LOGIN_WINDOW_MS = 15 * 60 * 1000;
export const MAX_CLIENTS = 100;
/** The number of known devices whose failed logins the relay keeps. */
const MAX_FAILED_LOGIN_SOURCES = 1000;
/**
 * A browser that signed in once gets a signed device cookie. The relay cannot
 * see the caller's real address (Traefik and ServiceLB are in front of it), so
 * the login limit has two kinds of source: each known device has its own limit,
 * and all other callers share one limit. A stranger can block only the shared
 * limit, so the user's known browser can still sign in, and guesses stay limited.
 */
const DEVICE_COOKIE = "relay_device";
const DEVICE_COOKIE_MAX_AGE_MS = 365 * 24 * 60 * 60 * 1000;
const UNKNOWN_SOURCE = "unknown";

/**
 * The redirect URIs that a client may register. Registration is open (the MCP
 * clients register themselves), so the relay sends a code only to the user's
 * own clients: the claude.ai and claude.com connector callback, and a loopback
 * address (Claude Code on the user's computer). EXTRA_REDIRECT_URIS adds exact URIs.
 */
const CLAUDE_CALLBACKS = ["https://claude.ai/api/mcp/auth_callback", "https://claude.com/api/mcp/auth_callback"];
const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

export function redirectAllowed(uri: string, extra: readonly string[] = []): boolean {
  let url: URL;
  try {
    url = new URL(uri);
  } catch {
    return false;
  }
  if (url.username || url.password) return false;
  if (url.protocol === "http:" && LOOPBACK_HOSTS.has(url.hostname)) return true;
  const exact = url.href;
  return CLAUDE_CALLBACKS.includes(exact) || extra.some((allowed) => new URL(allowed).href === exact);
}

interface PendingAuthorization {
  clientId: string;
  params: AuthorizationParams;
  expiresAt: number;
}

interface IssuedCode {
  clientId: string;
  params: AuthorizationParams;
  expiresAt: number;
}

function newSecret(): string {
  return randomBytes(32).toString("base64url");
}

/** Tokens and codes are stored by hash, so a heap dump does not show them. */
function key(secret: string): string {
  return createHash("sha256").update(secret, "utf8").digest("hex");
}

function escapeHtml(text: string): string {
  return text.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
}

export interface StoredOAuthToken {
  clientId: string;
  scopes: string[];
  resource?: string;
  /** Milliseconds since the epoch. */
  expiresAt: number;
}

export interface OAuthTokenPair {
  access: { hash: string; token: StoredOAuthToken };
  refresh: { hash: string; token: StoredOAuthToken };
  /** On a rotation: copy these fields from the old refresh token into the new tokens. */
  inherit?: { scopes: boolean; resource: boolean };
}

/** The new pair of a rotation, with the inherited fields of the old token. */
export function inheritedPair(old: StoredOAuthToken, next: OAuthTokenPair): OAuthTokenPair {
  const fix = (token: StoredOAuthToken): StoredOAuthToken => {
    const out = { ...token };
    if (next.inherit?.scopes) out.scopes = old.scopes;
    if (next.inherit?.resource) {
      if (old.resource) out.resource = old.resource;
      else delete out.resource;
    }
    return out;
  };
  return {
    access: { hash: next.access.hash, token: fix(next.access.token) },
    refresh: { hash: next.refresh.hash, token: fix(next.refresh.token) },
  };
}

/** Where the provider keeps clients and token hashes. */
export interface OAuthStore {
  getClient(clientId: string): Promise<OAuthClientInformationFull | undefined>;
  /** False when the store is full of clients that signed in. */
  registerClient(client: OAuthClientInformationFull): Promise<boolean>;
  markSignedIn(clientId: string): Promise<void>;
  saveTokens(pair: OAuthTokenPair): Promise<void>;
  /** A live access token by its hash, or null. */
  accessToken(hash: string): Promise<StoredOAuthToken | null>;
  /** Swap a live refresh token of this client for a new pair. Null when it is not valid. */
  rotateRefresh(oldHash: string, clientId: string, next: OAuthTokenPair): Promise<StoredOAuthToken | null>;
  revoke(hash: string, clientId: string): Promise<void>;
}

/** An OAuthStore in memory: a restart forgets every sign-in. */
export class MemoryOAuthStore implements OAuthStore {
  private readonly clients = new Map<string, OAuthClientInformationFull>();
  /** Clients that completed a sign-in. Registrations never push these out. */
  private readonly signedIn = new Set<string>();
  private readonly access = new Map<string, StoredOAuthToken>();
  private readonly refresh = new Map<string, StoredOAuthToken>();

  constructor(private readonly now: () => number = () => Date.now()) {}

  private live(map: Map<string, StoredOAuthToken>, hash: string): StoredOAuthToken | null {
    const token = map.get(hash);
    if (!token) return null;
    if (token.expiresAt <= this.now()) {
      map.delete(hash);
      return null;
    }
    return token;
  }

  async getClient(clientId: string) {
    return this.clients.get(clientId);
  }

  async registerClient(client: OAuthClientInformationFull) {
    // Keep the store small: one user has only a few clients. Push out the oldest
    // client that never signed in, so that anonymous registrations cannot push
    // out the user's own clients.
    if (this.clients.size >= MAX_CLIENTS) {
      const oldest = [...this.clients.keys()].find((id) => !this.signedIn.has(id));
      if (oldest === undefined) return false;
      this.clients.delete(oldest);
    }
    this.clients.set(client.client_id, client);
    return true;
  }

  async markSignedIn(clientId: string) {
    this.signedIn.add(clientId);
  }

  async saveTokens({ access, refresh }: OAuthTokenPair) {
    this.access.set(access.hash, access.token);
    this.refresh.set(refresh.hash, refresh.token);
  }

  async accessToken(hash: string) {
    return this.live(this.access, hash);
  }

  async rotateRefresh(oldHash: string, clientId: string, next: OAuthTokenPair) {
    const stored = this.live(this.refresh, oldHash);
    if (!stored || stored.clientId !== clientId) return null;
    this.refresh.delete(oldHash);
    await this.saveTokens(inheritedPair(stored, next));
    return stored;
  }

  async revoke(hash: string, clientId: string) {
    for (const map of [this.access, this.refresh]) {
      if (map.get(hash)?.clientId === clientId) map.delete(hash);
    }
  }
}

/** The SDK's clients store over an OAuthStore, with the redirect URI rule. */
class ClientsStore implements OAuthRegisteredClientsStore {
  constructor(
    private readonly store: OAuthStore,
    private readonly extraRedirectUris: readonly string[],
  ) {}

  markSignedIn(clientId: string): Promise<void> {
    return this.store.markSignedIn(clientId);
  }

  getClient(clientId: string): Promise<OAuthClientInformationFull | undefined> {
    return this.store.getClient(clientId);
  }

  async registerClient(
    client: Omit<OAuthClientInformationFull, "client_id" | "client_id_issued_at">,
  ): Promise<OAuthClientInformationFull> {
    const uris = client.redirect_uris ?? [];
    if (uris.length === 0 || !uris.every((uri) => redirectAllowed(String(uri), this.extraRedirectUris))) {
      throw new InvalidClientMetadataError(
        "The relay accepts only the claude.ai or claude.com connector callback, or a loopback redirect URI.",
      );
    }
    const given = client as Partial<OAuthClientInformationFull>;
    const full: OAuthClientInformationFull = {
      ...client,
      client_id: given.client_id ?? randomBytes(16).toString("hex"),
      client_id_issued_at: given.client_id_issued_at ?? Math.floor(Date.now() / 1000),
    };
    if (!(await this.store.registerClient(JSON.parse(JSON.stringify(full))))) {
      throw new InvalidClientMetadataError("The relay has too many clients.");
    }
    return full;
  }
}

export interface SingleUserProviderOptions {
  loginPasswordBcrypt: string;
  /** The clients and token hashes. A MemoryOAuthStore when not given. */
  store?: OAuthStore;
  extraRedirectUris?: readonly string[];
  now: () => Date;
  log: Logger;
}

export class SingleUserProvider implements OAuthServerProvider {
  readonly clientsStore: ClientsStore;
  private readonly store: OAuthStore;
  private readonly pending = new Map<string, PendingAuthorization>();
  private readonly codes = new Map<string, IssuedCode>();
  /** The times of the failed (and running) logins, by the caller's IP address. */
  private readonly failedLogins = new Map<string, number[]>();

  /** The key that signs device cookies. A new password makes old cookies stop working. */
  private readonly deviceKey: Buffer;

  constructor(private readonly options: SingleUserProviderOptions) {
    this.store = options.store ?? new MemoryOAuthStore(() => this.nowMs());
    this.clientsStore = new ClientsStore(this.store, options.extraRedirectUris ?? []);
    this.deviceKey = createHash("sha256").update("studio-relay device cookie\0").update(options.loginPasswordBcrypt).digest();
  }

  private signDevice(id: string): string {
    return createHmac("sha256", this.deviceKey).update(id).digest("base64url");
  }

  private newDeviceCookie(): string {
    const id = randomBytes(16).toString("base64url");
    return `${id}.${this.signDevice(id)}`;
  }

  /** The login limit's source for this request: a known device, or the shared one. */
  private loginSource(req: Request): string {
    const header = req.headers.cookie ?? "";
    for (const part of header.split(";")) {
      const [name, ...rest] = part.trim().split("=");
      if (name !== DEVICE_COOKIE) continue;
      const [id, signature] = rest.join("=").split(".");
      if (!id || !signature) continue;
      const expected = Buffer.from(this.signDevice(id));
      const actual = Buffer.from(signature);
      if (expected.length === actual.length && timingSafeEqual(expected, actual)) return `device:${id}`;
    }
    return UNKNOWN_SOURCE;
  }

  private nowMs(): number {
    return this.options.now().getTime();
  }

  private sweep(): void {
    const now = this.nowMs();
    for (const map of [this.pending, this.codes] as Map<
      string,
      { expiresAt: number }
    >[]) {
      for (const [k, v] of map) if (v.expiresAt <= now) map.delete(k);
    }
  }

  async authorize(client: OAuthClientInformationFull, params: AuthorizationParams, res: Response): Promise<void> {
    this.sweep();
    if (!redirectAllowed(params.redirectUri, this.options.extraRedirectUris)) {
      res.status(400).type("text/plain").send("The redirect URI is not allowed.");
      return;
    }
    const requestId = newSecret();
    this.pending.set(requestId, {
      clientId: client.client_id,
      params,
      expiresAt: this.nowMs() + PENDING_TTL_MS,
    });
    res.status(200);
    sendLoginPage(res, requestId, client.client_name, params.redirectUri);
  }

  async challengeForAuthorizationCode(client: OAuthClientInformationFull, authorizationCode: string): Promise<string> {
    const code = this.codes.get(key(authorizationCode));
    if (!code || code.clientId !== client.client_id || code.expiresAt <= this.nowMs()) {
      throw new InvalidGrantError("The authorization code is not valid.");
    }
    return code.params.codeChallenge;
  }

  async exchangeAuthorizationCode(
    client: OAuthClientInformationFull,
    authorizationCode: string,
    _codeVerifier?: string,
    redirectUri?: string,
  ): Promise<OAuthTokens> {
    const k = key(authorizationCode);
    const code = this.codes.get(k);
    // A code works only once.
    this.codes.delete(k);
    if (!code || code.clientId !== client.client_id || code.expiresAt <= this.nowMs()) {
      throw new InvalidGrantError("The authorization code is not valid.");
    }
    if (redirectUri !== undefined && redirectUri !== code.params.redirectUri) {
      throw new InvalidGrantError("The redirect_uri does not match the authorization request.");
    }
    await this.store.markSignedIn(client.client_id);
    return this.issueTokens(client.client_id, code.params.scopes ?? [], code.params.resource);
  }

  async exchangeRefreshToken(
    client: OAuthClientInformationFull,
    refreshToken: string,
    scopes?: string[],
    resource?: URL,
  ): Promise<OAuthTokens> {
    const next = this.newTokens(client.client_id, scopes ?? [], resource);
    // Without scopes or a resource in the request, the new tokens keep those of the old token.
    const pair: OAuthTokenPair = { ...next.pair, inherit: { scopes: scopes === undefined, resource: resource === undefined } };
    const stored = await this.store.rotateRefresh(key(refreshToken), client.client_id, pair);
    if (!stored) throw new InvalidGrantError("The refresh token is not valid.");
    const kept = scopes ?? stored.scopes;
    const { scope: _scope, ...tokens } = next.tokens;
    return { ...tokens, ...(kept.length > 0 ? { scope: kept.join(" ") } : {}) };
  }

  async verifyAccessToken(token: string): Promise<AuthInfo> {
    const stored = await this.store.accessToken(key(token));
    if (!stored || stored.expiresAt <= this.nowMs()) {
      throw new InvalidTokenError("The access token is not valid.");
    }
    return {
      token,
      clientId: stored.clientId,
      scopes: stored.scopes,
      expiresAt: Math.floor(stored.expiresAt / 1000),
      resource: stored.resource ? new URL(stored.resource) : undefined,
    };
  }

  async revokeToken(
    client: OAuthClientInformationFull,
    request: { token: string; token_type_hint?: string },
  ): Promise<void> {
    await this.store.revoke(key(request.token), client.client_id);
  }

  private newTokens(clientId: string, scopes: string[], resource?: URL): { pair: OAuthTokenPair; tokens: OAuthTokens } {
    const now = this.nowMs();
    const accessToken = newSecret();
    const refreshToken = newSecret();
    const base = { clientId, scopes, ...(resource ? { resource: resource.href } : {}) };
    return {
      pair: {
        access: { hash: key(accessToken), token: { ...base, expiresAt: now + ACCESS_TTL_MS } },
        refresh: { hash: key(refreshToken), token: { ...base, expiresAt: now + REFRESH_TTL_MS } },
      },
      tokens: {
        access_token: accessToken,
        token_type: "bearer",
        expires_in: Math.floor(ACCESS_TTL_MS / 1000),
        refresh_token: refreshToken,
        ...(scopes.length > 0 ? { scope: scopes.join(" ") } : {}),
      },
    };
  }

  private async issueTokens(clientId: string, scopes: string[], resource?: URL): Promise<OAuthTokens> {
    this.sweep();
    const { pair, tokens } = this.newTokens(clientId, scopes, resource);
    await this.store.saveTokens(pair);
    return tokens;
  }

  /**
   * Counts a login attempt for this source before the password check, so that
   * attempts that run at the same time also count. Returns the attempt's time,
   * or undefined when the source has too many failures.
   */
  private startAttempt(source: string): number | undefined {
    const now = this.nowMs();
    for (const [k, times] of this.failedLogins) {
      const recent = times.filter((t) => now - t < FAILED_LOGIN_WINDOW_MS);
      if (recent.length === 0) this.failedLogins.delete(k);
      else this.failedLogins.set(k, recent);
    }
    const times = this.failedLogins.get(source) ?? [];
    if (times.length >= MAX_FAILED_LOGINS) return undefined;
    if (!this.failedLogins.has(source) && this.failedLogins.size >= MAX_FAILED_LOGIN_SOURCES) {
      const oldest = this.failedLogins.keys().next().value;
      if (oldest !== undefined) this.failedLogins.delete(oldest);
    }
    times.push(now);
    this.failedLogins.set(source, times);
    return now;
  }

  /** A correct password: the attempt does not count as a failure. */
  private endAttempt(source: string, attempt: number): void {
    const times = this.failedLogins.get(source);
    if (!times) return;
    const index = times.indexOf(attempt);
    if (index >= 0) times.splice(index, 1);
    if (times.length === 0) this.failedLogins.delete(source);
  }

  /**
   * Check the owner's password with the same login limit as the sign-in page.
   * Used by the codec sign-in.
   */
  async checkPassword(req: Request, password: string): Promise<"ok" | "wrong" | "locked"> {
    const source = this.loginSource(req);
    const attempt = this.startAttempt(source);
    if (attempt === undefined) return "locked";
    const ok = password.length > 0 && (await bcrypt.compare(password, this.options.loginPasswordBcrypt));
    if (!ok) return "wrong";
    this.endAttempt(source, attempt);
    return "ok";
  }

  /** The POST target of the login page. */
  loginRouter(): Router {
    const router = express.Router();
    router.post(LOGIN_PATH, express.urlencoded({ extended: false, limit: "4kb" }), async (req: Request, res: Response) => {
      res.setHeader("Cache-Control", "no-store");
      const requestId = typeof req.body?.request === "string" ? req.body.request : "";
      const password = typeof req.body?.password === "string" ? req.body.password : "";

      this.sweep();
      const pending = this.pending.get(requestId);
      if (!pending) {
        res.status(400).type("text/plain").send("The sign-in request expired. Start the sign-in again from the client.");
        return;
      }
      const client = await this.clientsStore.getClient(pending.clientId);
      if (!client) {
        this.pending.delete(requestId);
        res.status(400).type("text/plain").send("The client is not registered. Start the sign-in again from the client.");
        return;
      }

      const source = this.loginSource(req);
      const attempt = this.startAttempt(source);
      if (attempt === undefined) {
        this.options.log({ event: "login_locked", status: 429 });
        res.status(429);
        sendLoginPage(
          res,
          requestId,
          client.client_name,
          pending.params.redirectUri,
          "Too many wrong passwords. Wait 15 minutes, then try again.",
        );
        return;
      }

      const ok = password.length > 0 && (await bcrypt.compare(password, this.options.loginPasswordBcrypt));
      if (!ok) {
        this.options.log({ event: "login_failed", status: 401 });
        res.status(401);
        sendLoginPage(res, requestId, client.client_name, pending.params.redirectUri, "The password is not correct.");
        return;
      }
      this.endAttempt(source, attempt);
      // A second post of the same request (sent while the first compare ran) gets no second code.
      if (!this.pending.has(requestId)) {
        res.status(400).type("text/plain").send("The sign-in request expired. Start the sign-in again from the client.");
        return;
      }

      this.pending.delete(requestId);
      const code = newSecret();
      this.codes.set(key(code), {
        clientId: pending.clientId,
        params: pending.params,
        expiresAt: this.nowMs() + CODE_TTL_MS,
      });
      this.options.log({ event: "login_ok" });
      if (source === UNKNOWN_SOURCE) {
        res.cookie(DEVICE_COOKIE, this.newDeviceCookie(), {
          httpOnly: true,
          secure: req.secure,
          sameSite: "lax",
          path: LOGIN_PATH,
          maxAge: DEVICE_COOKIE_MAX_AGE_MS,
        });
      }

      const target = new URL(pending.params.redirectUri);
      target.searchParams.set("code", code);
      if (pending.params.state !== undefined) target.searchParams.set("state", pending.params.state);
      res.redirect(302, target.href);
    });
    return router;
  }
}

function sendLoginPage(res: Response, requestId: string, clientName: string | undefined, redirectUri: string, error?: string): void {
  const who = clientName ? ` for ${escapeHtml(clientName)}` : "";
  // The client name comes from the client. The page also shows where the code goes,
  // which the relay checked against its list.
  const target = new URL(redirectUri);
  const where = escapeHtml(target.host);
  res.setHeader("Cache-Control", "no-store");
  res.setHeader(
    "Content-Security-Policy",
    "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'",
  );
  res.type("html").send(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Spec Studio sign-in</title>
<style>
  :root { color-scheme: light dark; }
  body { font: 16px/1.4 -apple-system, system-ui, sans-serif; display: grid; place-items: center; min-height: 100vh; margin: 0; padding: 0 16px; }
  form { width: 100%; max-width: 320px; display: grid; gap: 12px; }
  h1 { font-size: 20px; margin: 0; }
  input, button { font: inherit; padding: 10px 12px; border-radius: 8px; border: 1px solid #8888; }
  button { cursor: pointer; }
  .error { color: #d33; margin: 0; }
</style>
</head>
<body>
<form method="post" action="${LOGIN_PATH}">
  <h1>Spec Studio relay</h1>
  <p>Sign in${who}. After you sign in, the relay sends you back to <strong>${where}</strong>.</p>
  ${error ? `<p class="error">${escapeHtml(error)}</p>` : ""}
  <input type="hidden" name="request" value="${escapeHtml(requestId)}">
  <input type="password" name="password" autocomplete="current-password" placeholder="Password" required autofocus>
  <button type="submit">Sign in</button>
</form>
</body>
</html>`);
}
