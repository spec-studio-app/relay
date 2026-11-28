// The relay: an MCP server over Streamable HTTP at /mcp, protected by the
// relay's own OAuth 2.1 server, and the agent WebSocket at /agent.
import type { IncomingMessage, Server } from "node:http";
import type { Duplex } from "node:stream";
import express, { type Express, type NextFunction, type Request, type Response, type Router } from "express";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { mcpAuthRouter, getOAuthProtectedResourceMetadataUrl } from "@modelcontextprotocol/sdk/server/auth/router.js";
import { requireBearerAuth } from "@modelcontextprotocol/sdk/server/auth/middleware/bearerAuth.js";
import { AgentHub } from "./agent.js";
import { SingleUserProvider, type OAuthStore } from "./oauth.js";
import { randomBytes } from "node:crypto";
import { pairCodeHash } from "./pairing.js";
import { RELAY_VERSION, WIRE_VERSION } from "./version.js";
import type { PayloadKey } from "./config.js";
import { forwardArguments, LOCAL_TOOLS, TOOLS, UNKNOWN_LIST_COLLECTIONS_PREFIX, UPDATE_FOR_COLLECTIONS_TEXT } from "./tools.js";
import { consoleLogger, type Logger } from "./log.js";
import type { AgentBackend } from "./backend.js";
import { OFFLINE_READ_TOOLS, type OfflineReader, type ToolText } from "./offline.js";

export interface LocalTools {
  answer(tool: string, args: Record<string, unknown>): Promise<ToolText>;
}

import { DEFAULT_CALL_TIMEOUT_MS } from "./config.js";
import { STORE_UNAVAILABLE_TEXT, StoreUnavailableError } from "./store/errors.js";

/** The mark of a request whose token the relay could not check: the store was down. */
const STORE_DOWN = "relayStoreUnavailable";

export const DEFAULT_PING_INTERVAL_MS = 20_000;
/** R6.S1: a call to a sleeping Mac answers "offline" in 5 seconds or less. */
export const DEFAULT_PROBE_TIMEOUT_MS = 3_000;

export interface RelayConfig {
  /** The public origin, e.g. https://studio-mcp.loomis.io. It is the OAuth issuer. */
  publicUrl: string;
  loginPasswordBcrypt: string;
  callTimeoutMs?: number;
  pingIntervalMs?: number;
  probeTimeoutMs?: number;
  /** The longest wait for the secret check of an agent upgrade. */
  agentAuthTimeoutMs?: number;
  /** How often the relay looks for messages that wait for the Mac while the agent is connected. */
  pendingScanMs?: number;
  /** A scan pushes again a message that still waits this long after its push. */
  resendAfterMs?: number;
  /** Try a failed state frame again after this time. */
  stateRetryMs?: number;
  /** After an incompatible hello, keep the socket open this long before the close. */
  incompatibleCloseMs?: number;
  /** Redirect URIs that clients may register, in addition to the defaults (see oauth.ts). */
  extraRedirectUris?: string[];
  /** The clock for the agent's last-seen and start times. */
  now?: () => Date;
  log?: Logger;
  /** The store for the agent hub (the agent wire version, paired secrets). */
  agentBackend?: AgentBackend;
  /** Answers for the read tools while the Mac is offline. */
  offline?: OfflineReader;
  /** The data keys for the codec endpoints. No endpoints when not given. */
  codecDataKeys?: PayloadKey[];
  /** The origin of the Temporal Web UI (CORS for the codec endpoints). */
  uiOrigin?: string;
  /** The largest /mcp request body in bytes (1 MB when not given). */
  mcpBodyLimit?: number;
  /** The OAuth clients and token hashes. In memory when not given. */
  oauthStore?: OAuthStore;
  /** The tools that the relay answers itself (send_message, read_messages, ...). */
  localTools?: (hub: AgentHub) => LocalTools;
}

export interface Relay {
  app: Express;
  hub: AgentHub;
  /** Attach the agent WebSocket upgrade handler to an HTTP server. */
  attach(server: Server): void;
  handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void;
  close(): void;
}

export function createRelay(config: RelayConfig): Relay {
  const log = config.log ?? consoleLogger;
  const now = config.now ?? (() => new Date());
  const publicUrl = new URL(config.publicUrl);
  const mcpUrl = new URL("/mcp", publicUrl);

  const hub = new AgentHub({
    callTimeoutMs: config.callTimeoutMs ?? DEFAULT_CALL_TIMEOUT_MS,
    pingIntervalMs: config.pingIntervalMs ?? DEFAULT_PING_INTERVAL_MS,
    probeTimeoutMs: config.probeTimeoutMs ?? DEFAULT_PROBE_TIMEOUT_MS,
    now,
    log,
    backend: config.agentBackend,
    authTimeoutMs: config.agentAuthTimeoutMs,
    pendingScanMs: config.pendingScanMs,
    resendAfterMs: config.resendAfterMs,
    stateRetryMs: config.stateRetryMs,
    incompatibleCloseMs: config.incompatibleCloseMs,
  });
  // The provider keeps the real clock: the SDK's bearer check compares token
  // expiry with Date.now().
  const provider = new SingleUserProvider({
    loginPasswordBcrypt: config.loginPasswordBcrypt,
    store: config.oauthStore,
    extraRedirectUris: config.extraRedirectUris ?? [],
    now: () => new Date(),
    log,
  });

  const localTools = config.localTools?.(hub);

  const app = express();
  // Traefik is the one proxy in front of the relay.
  app.set("trust proxy", 1);
  app.disable("x-powered-by");

  // Access log: method, path (no query string), status and duration only.
  app.use((req: Request, res: Response, next: NextFunction) => {
    const start = process.hrtime.bigint();
    res.on("finish", () => {
      if (req.path === "/healthz") return;
      const ms = Number(process.hrtime.bigint() - start) / 1e6;
      log({ method: req.method, path: req.path, status: res.statusCode, ms: Math.round(ms) });
    });
    next();
  });

  // Public, so it does not say whether the Mac is online.
  app.get("/healthz", (_req, res) => {
    res.status(200).json({ ok: true });
  });

  // Pairing: a pairing code mints the agent secret. The relay keeps only its hash.
  const pairLimit = new PairLimit();
  app.get("/agent/info", (_req, res) => {
    res.json({ relayVersion: RELAY_VERSION, wireVersion: WIRE_VERSION, mcpURL: mcpUrl.href });
  });
  app.post("/agent/pair", express.json({ limit: "4kb" }), async (req: Request, res: Response) => {
    res.setHeader("Cache-Control", "no-store");
    const backend = config.agentBackend;
    const source = req.ip ?? "unknown";
    const code = typeof req.body?.code === "string" ? req.body.code : "";
    if (!backend || code.length === 0 || code.length > 64) {
      pairLimit.fail(source);
      res.status(403).json({ error: "The relay refused the pairing code." });
      return;
    }
    const codeHash = pairCodeHash(code);
    try {
      // A source with too many wrong codes (or too many wrong codes from all
      // sources) gets a query first, which adds nothing to the workflow history.
      // A correct code always passes, so a stranger cannot block the owner.
      if (pairLimit.limited(source) && !(await backend.pairCodeValid(codeHash))) {
        log({ event: "pair_limited", status: 429 });
        res.status(429).json({ error: "Too many wrong pairing codes. Wait 15 minutes, then try again." });
        return;
      }
    } catch {
      log({ event: "pair_failed" });
      res.status(503).json({ error: "The relay cannot reach its store now. Try again later." });
      return;
    }
    const secret = randomBytes(32).toString("base64url");
    const secretHash = AgentHub.secretHash(`Bearer ${secret}`)!;
    let ok: boolean;
    try {
      ok = await backend.pair(codeHash, secretHash, now());
    } catch {
      log({ event: "pair_failed" });
      res.status(503).json({ error: "The relay cannot reach its store now. Try again later." });
      return;
    }
    if (!ok) {
      pairLimit.fail(source);
      log({ event: "pair_refused", status: 403 });
      res.status(403).json({ error: "The relay refused the pairing code." });
      return;
    }
    log({ event: "paired" });
    res.json({ secret, relayVersion: RELAY_VERSION, mcpURL: mcpUrl.href });
  });
  app.post("/agent/unpair", async (req: Request, res: Response) => {
    const hash = AgentHub.secretHash(req.headers.authorization);
    const backend = config.agentBackend;
    if (!hash || !backend) {
      res.status(401).end();
      return;
    }
    let removed: boolean;
    try {
      // Only a paired secret may unpair: a stranger's bearer costs a query, not an update.
      if (!(await backend.verifyAgent(hash))) {
        log({ event: "unpair_refused", status: 401 });
        res.status(401).end();
        return;
      }
      removed = await backend.unpair(hash);
    } catch {
      res.status(503).json({ error: "The relay cannot reach its store now. Try again later." });
      return;
    }
    if (!removed) {
      res.status(401).end();
      return;
    }
    hub.disconnect(hash);
    log({ event: "unpaired" });
    res.status(204).end();
  });

  app.use(
    mcpAuthRouter({
      provider,
      issuerUrl: publicUrl,
      resourceServerUrl: mcpUrl,
      resourceName: "Spec Studio",
    }),
  );
  app.use(provider.loginRouter());
  if (config.codecDataKeys && config.codecDataKeys.length > 0) {
    // The codec endpoints decode Temporal payloads. They load on the first
    // codec request, so a relay without Temporal never loads the Temporal SDK.
    const dataKeys = config.codecDataKeys;
    let codec: Promise<Router> | null = null;
    app.use((req: Request, res: Response, next: NextFunction) => {
      if (req.path !== "/codec" && !req.path.startsWith("/codec/")) return next();
      codec ??= import("./codecServer.js").then(({ codecRouter }) =>
        codecRouter({ dataKeys, uiOrigin: config.uiOrigin, provider, passwordHash: config.loginPasswordBcrypt, log }),
      );
      codec.then((router) => router(req, res, next), next);
    });
  }

  // While the store is down, the token check cannot read the tokens. The SDK
  // would answer HTTP 500; the verifier marks the request instead, and the
  // routes below answer 503 with the store's text before any tool runs.
  const bearer = requireBearerAuth({
    verifier: {
      async verifyAccessToken(token) {
        try {
          return await provider.verifyAccessToken(token);
        } catch (error) {
          if (!(error instanceof StoreUnavailableError)) throw error;
          // The SDK needs an expiry; the mark stops the request before any tool.
          return { token: "", clientId: "", scopes: [], expiresAt: Math.floor(Date.now() / 1000) + 60, extra: { [STORE_DOWN]: true } };
        }
      },
    },
    resourceMetadataUrl: getOAuthProtectedResourceMetadataUrl(mcpUrl),
  });
  /**
   * The answer while the store is down. No tool runs, because the token is not
   * checked: a tool call gets the store's text as its result (so Claude AI
   * shows it). The requests that need no store (initialize, tools/list) go on.
   */
  const storeDown = (req: Request, res: Response, next: NextFunction) => {
    if (!(req as Request & { auth?: { extra?: Record<string, unknown> } }).auth?.extra?.[STORE_DOWN]) return next();
    log({ event: "mcp_store_unavailable", path: req.path });
    if (req.method !== "POST") {
      res.status(503).set("Retry-After", "60").json({ jsonrpc: "2.0", error: { code: -32000, message: STORE_UNAVAILABLE_TEXT }, id: null });
      return;
    }
    const answer = (message: unknown) => {
      const m = (message && typeof message === "object" ? message : {}) as { id?: unknown; method?: unknown };
      if (m.id === undefined) return null;
      if (m.method === "tools/call") {
        return { jsonrpc: "2.0", id: m.id, result: { content: [{ type: "text", text: STORE_UNAVAILABLE_TEXT }], isError: true } };
      }
      return { jsonrpc: "2.0", id: m.id, error: { code: -32000, message: STORE_UNAVAILABLE_TEXT } };
    };
    const body = req.body as unknown;
    const messages = Array.isArray(body) ? body : [body];
    // initialize, tools/list and notifications read no store and run no tool:
    // they go on, so the client connects and then sees the text at its tool call.
    if (!messages.some((m) => (m as { method?: unknown } | null)?.method === "tools/call")) return next();
    const answers = (Array.isArray(body) ? body.map(answer) : [answer(body)]).filter((a) => a !== null);
    if (answers.length === 0) {
      res.status(202).end();
      return;
    }
    res.status(200).json(Array.isArray(body) ? answers : answers[0]);
  };

  const mcpJson = express.json({ limit: config.mcpBodyLimit ?? 1024 * 1024 });
  app.post("/mcp", bearer, mcpJson, storeDown, async (req: Request, res: Response) => {
    // Stateless: one server and one transport for each request.
    const server = buildMcpServer(hub, log, config.offline, localTools);
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    res.on("close", () => {
      void transport.close();
      void server.close();
    });
    try {
      await server.connect(transport);
      await transport.handleRequest(req, res, req.body);
    } catch {
      log({ event: "mcp_error", path: "/mcp" });
      if (!res.headersSent) {
        res.status(500).json({ jsonrpc: "2.0", error: { code: -32603, message: "Internal error" }, id: null });
      }
    }
  });

  // The relay is stateless, so it has no stream for server messages.
  const methodNotAllowed = (_req: Request, res: Response) => {
    res
      .status(405)
      .set("Allow", "POST")
      .json({ jsonrpc: "2.0", error: { code: -32000, message: "Method not allowed." }, id: null });
  };
  app.get("/mcp", bearer, storeDown, methodNotAllowed);
  app.delete("/mcp", bearer, storeDown, methodNotAllowed);

  const handleUpgrade = (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    if (!hub.handleUpgrade(req, socket, head)) {
      socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
      socket.destroy();
    }
  };

  return {
    app,
    hub,
    handleUpgrade,
    attach(server: Server) {
      server.on("upgrade", handleUpgrade);
    },
    close() {
      hub.close();
    },
  };
}

/**
 * The notice that starts each tool result while the last agent hello failed
 * the handshake, or null. It reads the last sync time only then.
 */
async function mismatchNotice(hub: AgentHub, offline?: OfflineReader): Promise<string | null> {
  const agent = hub.lastAgent;
  if (!agent || agent.compatible) return null;
  // undefined: the relay cannot tell the time of the last sync, so the notice leaves it out.
  let lastSync: string | null | undefined;
  try {
    lastSync = offline?.lastSyncAt ? await offline.lastSyncAt() : undefined;
  } catch {
    lastSync = undefined;
  }
  return hub.mismatchNotice(lastSync);
}

function buildMcpServer(hub: AgentHub, log: Logger, offline?: OfflineReader, localTools?: LocalTools): McpServer {
  const server = new McpServer({ name: "spec-studio", version: RELAY_VERSION });
  for (const tool of TOOLS) {
    server.registerTool(
      tool.name,
      { title: tool.title, description: tool.description, inputSchema: tool.inputSchema },
      async (args: Record<string, unknown>) => {
        const start = Date.now();
        let result = await hub.call(tool.name, forwardArguments(tool.name, args ?? {}));
        const fromMirror = result.offline === true && offline !== undefined && OFFLINE_READ_TOOLS.has(tool.name);
        if (fromMirror) result = await offline.answer(tool.name, args ?? {}, hub.offlineText());
        // An older Spec Studio has no list_collections: say what to do.
        if (tool.name === "list_collections" && result.text.startsWith(UNKNOWN_LIST_COLLECTIONS_PREFIX)) {
          result = { ...result, text: UPDATE_FOR_COLLECTIONS_TEXT };
        }
        log({ event: "tool_call", tool: tool.name, ok: result.ok, offline: fromMirror || undefined, ms: Date.now() - start });
        const notice = await mismatchNotice(hub, offline);
        return {
          content: [{ type: "text" as const, text: notice ? `${notice}\n${result.text}` : result.text }],
          ...(result.ok ? {} : { isError: true }),
        };
      },
    );
  }
  if (localTools) {
    for (const tool of LOCAL_TOOLS) {
      server.registerTool(
        tool.name,
        { title: tool.title, description: tool.description, inputSchema: tool.inputSchema },
        async (args: Record<string, unknown>) => {
          const start = Date.now();
          const result = await localTools.answer(tool.name, args ?? {});
          log({ event: "tool_call", tool: tool.name, ok: result.ok, ms: Date.now() - start });
          const notice = await mismatchNotice(hub, offline);
          return {
            content: [{ type: "text" as const, text: notice ? `${notice}\n${result.text}` : result.text }],
            ...(result.ok ? {} : { isError: true }),
          };
        },
      );
    }
  }
  return server;
}

/**
 * The limit on wrong pairing codes: per source IP, and a ceiling for all
 * sources. A limited request must prove its code with a query before the relay
 * runs the pair update.
 */
export class PairLimit {
  private readonly failures = new Map<string, number[]>();
  private all: number[] = [];

  constructor(
    private readonly o: { perSource: number; total: number; windowMs: number; maxSources: number } = {
      perSource: 10,
      total: 100,
      windowMs: 15 * 60 * 1000,
      maxSources: 10_000,
    },
    private readonly nowMs: () => number = Date.now,
  ) {}

  private prune(): void {
    const start = this.nowMs() - this.o.windowMs;
    this.all = this.all.filter((t) => t > start);
    for (const [source, times] of this.failures) {
      const recent = times.filter((t) => t > start);
      if (recent.length === 0) this.failures.delete(source);
      else this.failures.set(source, recent);
    }
  }

  limited(source: string): boolean {
    this.prune();
    return (this.failures.get(source)?.length ?? 0) >= this.o.perSource || this.all.length >= this.o.total;
  }

  fail(source: string): void {
    this.prune();
    const now = this.nowMs();
    this.all.push(now);
    if (!this.failures.has(source) && this.failures.size >= this.o.maxSources) {
      const oldest = this.failures.keys().next().value;
      if (oldest !== undefined) this.failures.delete(oldest);
    }
    const times = this.failures.get(source) ?? [];
    times.push(now);
    this.failures.set(source, times);
  }
}
