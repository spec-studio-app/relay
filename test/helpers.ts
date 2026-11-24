import { createHash, randomBytes } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import bcrypt from "bcryptjs";
import { WebSocket } from "ws";
import { createRelay, type Relay, type RelayConfig } from "../src/server.js";
import type { LogEntry } from "../src/log.js";
import type { AgentBackend } from "../src/backend.js";
import type { OfflineReader } from "../src/offline.js";

export const PASSWORD = "correct horse battery staple";
export const AGENT_SECRET = "agent-secret-" + randomBytes(16).toString("hex");
const PASSWORD_BCRYPT = bcrypt.hashSync(PASSWORD, 4);
const AGENT_SECRET_SHA256 = createHash("sha256").update(AGENT_SECRET).digest("hex");

export interface TestRelay {
  relay: Relay;
  server: Server;
  base: string;
  logs: LogEntry[];
  close(): Promise<void>;
}

export async function startRelay(
  options: {
    callTimeoutMs?: number;
    pingIntervalMs?: number;
    probeTimeoutMs?: number;
    now?: () => Date;
    agentBackend?: AgentBackend;
    offline?: OfflineReader;
    localTools?: RelayConfig["localTools"];
    oauthStore?: RelayConfig["oauthStore"];
    /** Set to null for no legacy secret. */
    legacySecretSha256?: string | null;
  } = {},
): Promise<TestRelay> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  const base = `http://127.0.0.1:${port}`;
  const logs: LogEntry[] = [];
  const relay = createRelay({
    publicUrl: base,
    loginPasswordBcrypt: PASSWORD_BCRYPT,
    agentSecretSha256: options.legacySecretSha256 === null ? undefined : (options.legacySecretSha256 ?? AGENT_SECRET_SHA256),
    agentBackend: options.agentBackend,
    offline: options.offline,
    localTools: options.localTools,
    oauthStore: options.oauthStore,
    callTimeoutMs: options.callTimeoutMs ?? 2000,
    pingIntervalMs: options.pingIntervalMs ?? 60_000,
    probeTimeoutMs: options.probeTimeoutMs ?? 500,
    now: options.now,
    log: (entry) => logs.push(entry),
  });
  server.on("request", relay.app);
  relay.attach(server);
  return {
    relay,
    server,
    base,
    logs,
    async close() {
      relay.close();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

export interface FakeAgent {
  ws: WebSocket;
  /** The call frames. */
  received: Array<{ type: string; id: string; tool: string; arguments: Record<string, unknown> }>;
  /** Every frame from the relay. */
  frames: Array<Record<string, any>>;
  send(frame: Record<string, unknown>): void;
  /** Wait for the next frame of this type that passes the check. */
  next(type: string, check?: (frame: Record<string, any>) => boolean, ms?: number): Promise<Record<string, any>>;
  /** Reply to each call with this function. Return undefined to never answer. */
  answer: ((call: { tool: string; arguments: Record<string, unknown> }) => { ok: boolean; text: string } | undefined) | null;
  close(): Promise<void>;
}

export async function connectAgent(
  base: string,
  secret = AGENT_SECRET,
  hello: Record<string, unknown> = { type: "hello", version: 1 },
): Promise<FakeAgent> {
  const ws = new WebSocket(base.replace(/^http/, "ws") + "/agent", {
    headers: { Authorization: `Bearer ${secret}` },
  });
  const agent: FakeAgent = {
    ws,
    received: [],
    frames: [],
    send: (frame) => ws.send(JSON.stringify(frame)),
    next: (type, check = () => true, ms = 10_000) =>
      new Promise((resolve, reject) => {
        let index = 0;
        const timer = setTimeout(() => {
          clearInterval(poll);
          reject(new Error(`No ${type} frame in time.`));
        }, ms);
        const poll = setInterval(() => {
          for (; index < agent.frames.length; index++) {
            const f = agent.frames[index];
            if (f.type === type && check(f)) {
              clearInterval(poll);
              clearTimeout(timer);
              agent.frames.splice(index, 1);
              resolve(f);
              return;
            }
          }
          index = 0;
        }, 10);
      }),
    answer: null,
    close: () =>
      new Promise<void>((resolve) => {
        if (ws.readyState === WebSocket.CLOSED) return resolve();
        ws.once("close", () => resolve());
        ws.close();
      }),
  };
  ws.on("message", (data) => {
    const message = JSON.parse(data.toString());
    if (message.type !== "call") {
      agent.frames.push(message);
      return;
    }
    agent.received.push(message);
    const reply = agent.answer?.(message);
    if (reply) ws.send(JSON.stringify({ type: "result", id: message.id, ...reply }));
  });
  await new Promise<void>((resolve, reject) => {
    ws.once("open", () => {
      ws.send(JSON.stringify(hello));
      resolve();
    });
    ws.once("unexpected-response", (_req, res) => reject(Object.assign(new Error("rejected"), { status: res.statusCode })));
    ws.once("error", reject);
  });
  return agent;
}

/** Try to connect and return the HTTP status of the refused upgrade. */
export function rejectedStatus(base: string, secret?: string): Promise<number> {
  const headers: Record<string, string> = secret === undefined ? {} : { Authorization: `Bearer ${secret}` };
  const ws = new WebSocket(base.replace(/^http/, "ws") + "/agent", { headers });
  return new Promise((resolve, reject) => {
    ws.once("unexpected-response", (_req, res) => {
      resolve(res.statusCode ?? 0);
      ws.terminate();
    });
    ws.once("open", () => {
      ws.close();
      reject(new Error("The relay accepted the connection."));
    });
    ws.once("error", () => {});
  });
}

function base64url(buffer: Buffer): string {
  return buffer.toString("base64url");
}

export interface SignIn {
  accessToken: string;
  refreshToken: string;
  code: string;
  clientId: string;
}

/** A real OAuth flow: register, authorize with the password, token with PKCE. */
export async function signIn(base: string, password = PASSWORD): Promise<SignIn> {
  const redirectUri = "http://127.0.0.1:9/callback";
  const registered = await fetch(`${base}/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      client_name: "Test client",
      redirect_uris: [redirectUri],
      grant_types: ["authorization_code", "refresh_token"],
      response_types: ["code"],
      token_endpoint_auth_method: "none",
    }),
  });
  if (registered.status !== 201) throw new Error(`register: ${registered.status} ${await registered.text()}`);
  const client = (await registered.json()) as { client_id: string };

  const verifier = base64url(randomBytes(32));
  const challenge = base64url(createHash("sha256").update(verifier).digest());
  const state = base64url(randomBytes(8));
  const authorizeUrl = new URL(`${base}/authorize`);
  authorizeUrl.search = new URLSearchParams({
    response_type: "code",
    client_id: client.client_id,
    redirect_uri: redirectUri,
    code_challenge: challenge,
    code_challenge_method: "S256",
    state,
    resource: `${base}/mcp`,
  }).toString();
  const page = await fetch(authorizeUrl, { redirect: "manual" });
  const html = await page.text();
  if (page.status !== 200) throw new Error(`authorize: ${page.status} ${html}`);
  const request = /name="request" value="([^"]+)"/.exec(html)?.[1];
  if (!request) throw new Error("No request id in the login page.");

  const login = await fetch(`${base}/login`, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ request, password }).toString(),
    redirect: "manual",
  });
  if (login.status !== 302) throw new Error(`login: ${login.status}`);
  const location = new URL(login.headers.get("location")!);
  if (location.searchParams.get("state") !== state) throw new Error("The state does not match.");
  const code = location.searchParams.get("code")!;

  const token = await fetch(`${base}/token`, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      code,
      code_verifier: verifier,
      client_id: client.client_id,
      redirect_uri: redirectUri,
      resource: `${base}/mcp`,
    }).toString(),
  });
  if (token.status !== 200) throw new Error(`token: ${token.status} ${await token.text()}`);
  const tokens = (await token.json()) as { access_token: string; refresh_token: string };
  return { accessToken: tokens.access_token, refreshToken: tokens.refresh_token, code, clientId: client.client_id };
}

export interface ToolResult {
  isError?: boolean;
  content: Array<{ type: string; text: string }>;
}

/** Send one JSON-RPC request to /mcp. Returns the HTTP status and the parsed message, if any. */
export async function mcpRequest(
  base: string,
  token: string | undefined,
  method: string,
  params: Record<string, unknown> = {},
): Promise<{ status: number; message?: { result?: any; error?: any } }> {
  const headers: Record<string, string> = {
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
  };
  if (token) headers.authorization = `Bearer ${token}`;
  const res = await fetch(`${base}/mcp`, {
    method: "POST",
    headers,
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  });
  const text = await res.text();
  if (res.status !== 200) return { status: res.status };
  const type = res.headers.get("content-type") ?? "";
  if (type.includes("text/event-stream")) {
    const data = text
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trim())
      .find((line) => line.length > 0);
    return { status: res.status, message: data ? JSON.parse(data) : undefined };
  }
  return { status: res.status, message: JSON.parse(text) };
}

export async function callTool(
  base: string,
  token: string,
  name: string,
  args: Record<string, unknown> = {},
): Promise<ToolResult> {
  const { status, message } = await mcpRequest(base, token, "tools/call", { name, arguments: args });
  if (status !== 200 || !message?.result) throw new Error(`tools/call: ${status} ${JSON.stringify(message)}`);
  return message.result as ToolResult;
}

/** Register a client with these redirect URIs. Returns the HTTP status. */
export async function register(base: string, redirectUris: string[]): Promise<number> {
  const res = await fetch(`${base}/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      client_name: "Test client",
      redirect_uris: redirectUris,
      grant_types: ["authorization_code"],
      response_types: ["code"],
      token_endpoint_auth_method: "none",
    }),
  });
  await res.text();
  return res.status;
}

/** Register and open /authorize. Returns the request id and the login page. */
export async function openLogin(base: string): Promise<{ request: string; html: string }> {
  const redirectUri = "http://127.0.0.1:9/callback";
  const registered = await fetch(`${base}/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      client_name: "Test client",
      redirect_uris: [redirectUri],
      grant_types: ["authorization_code"],
      response_types: ["code"],
      token_endpoint_auth_method: "none",
    }),
  });
  const client = (await registered.json()) as { client_id: string };
  const url = new URL(`${base}/authorize`);
  url.search = new URLSearchParams({
    response_type: "code",
    client_id: client.client_id,
    redirect_uri: redirectUri,
    code_challenge: base64url(createHash("sha256").update("verifier-".repeat(6)).digest()),
    code_challenge_method: "S256",
    resource: `${base}/mcp`,
  }).toString();
  const html = await (await fetch(url, { redirect: "manual" })).text();
  const request = /name="request" value="([^"]+)"/.exec(html)?.[1];
  if (!request) throw new Error("No request id in the login page.");
  return { request, html };
}

export function postLogin(base: string, request: string, password: string, cookie?: string): Promise<Response> {
  return fetch(`${base}/login`, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded", ...(cookie ? { cookie } : {}) },
    body: new URLSearchParams({ request, password }).toString(),
    redirect: "manual",
  });
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
