// The relay: an MCP server over Streamable HTTP at /mcp, protected by the
// relay's own OAuth 2.1 server, and the agent WebSocket at /agent.
import type { IncomingMessage, Server } from "node:http";
import type { Duplex } from "node:stream";
import express, { type Express, type NextFunction, type Request, type Response } from "express";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { mcpAuthRouter, getOAuthProtectedResourceMetadataUrl } from "@modelcontextprotocol/sdk/server/auth/router.js";
import { requireBearerAuth } from "@modelcontextprotocol/sdk/server/auth/middleware/bearerAuth.js";
import { AgentHub } from "./agent.js";
import { SingleUserProvider, type OAuthStore } from "./oauth.js";
import { randomBytes } from "node:crypto";
import { pairCodeHash } from "./pairing.js";
import { RELAY_VERSION, WIRE_VERSION } from "./version.js";
import { LOCAL_TOOLS, TOOLS } from "./tools.js";
import { consoleLogger, type Logger } from "./log.js";
import type { AgentBackend } from "./backend.js";
import { OFFLINE_READ_TOOLS, type OfflineReader, type ToolText } from "./offline.js";

export interface LocalTools {
  answer(tool: string, args: Record<string, unknown>): Promise<ToolText>;
}

import { DEFAULT_CALL_TIMEOUT_MS } from "./config.js";

export const DEFAULT_PING_INTERVAL_MS = 20_000;
/** R6.S1: a call to a sleeping Mac answers "offline" in 5 seconds or less. */
export const DEFAULT_PROBE_TIMEOUT_MS = 3_000;

export interface RelayConfig {
  /** The public origin, e.g. https://studio-mcp.loomis.io. It is the OAuth issuer. */
  publicUrl: string;
  loginPasswordBcrypt: string;
  /** The legacy agent secret hash (AGENT_SECRET_SHA256), when set. */
  agentSecretSha256?: string;
  callTimeoutMs?: number;
  pingIntervalMs?: number;
  probeTimeoutMs?: number;
  /** Redirect URIs that clients may register, in addition to the defaults (see oauth.ts). */
  extraRedirectUris?: string[];
  /** The clock for the agent's last-seen and start times. */
  now?: () => Date;
  log?: Logger;
  /** Temporal for the agent hub (wire version 2, paired secrets). */
  agentBackend?: AgentBackend;
  /** Answers for the read tools while the Mac is offline. */
  offline?: OfflineReader;
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
    agentSecretSha256: config.agentSecretSha256?.toLowerCase(),
    callTimeoutMs: config.callTimeoutMs ?? DEFAULT_CALL_TIMEOUT_MS,
    pingIntervalMs: config.pingIntervalMs ?? DEFAULT_PING_INTERVAL_MS,
    probeTimeoutMs: config.probeTimeoutMs ?? DEFAULT_PROBE_TIMEOUT_MS,
    now,
    log,
    backend: config.agentBackend,
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
  const failedPairs: number[] = [];
  const PAIR_LIMIT = 20;
  const PAIR_WINDOW_MS = 15 * 60 * 1000;
  app.get("/agent/info", (_req, res) => {
    res.json({ relayVersion: RELAY_VERSION, wireVersion: WIRE_VERSION, mcpURL: mcpUrl.href });
  });
  app.post("/agent/pair", express.json({ limit: "4kb" }), async (req: Request, res: Response) => {
    res.setHeader("Cache-Control", "no-store");
    const backend = config.agentBackend;
    const nowMs = Date.now();
    while (failedPairs.length > 0 && nowMs - failedPairs[0] > PAIR_WINDOW_MS) failedPairs.shift();
    if (failedPairs.length >= PAIR_LIMIT) {
      res.status(429).json({ error: "Too many wrong pairing codes. Wait 15 minutes, then try again." });
      return;
    }
    const code = typeof req.body?.code === "string" ? req.body.code : "";
    if (!backend || code.length === 0 || code.length > 64) {
      failedPairs.push(nowMs);
      res.status(403).json({ error: "The relay refused the pairing code." });
      return;
    }
    const secret = randomBytes(32).toString("base64url");
    const secretHash = AgentHub.secretHash(`Bearer ${secret}`)!;
    let ok: boolean;
    try {
      ok = await backend.pair(pairCodeHash(code), secretHash, now());
    } catch {
      log({ event: "pair_failed" });
      res.status(503).json({ error: "The relay cannot reach Temporal now. Try again later." });
      return;
    }
    if (!ok) {
      failedPairs.push(nowMs);
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
      removed = await backend.unpair(hash);
    } catch {
      res.status(503).json({ error: "The relay cannot reach Temporal now. Try again later." });
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

  const bearer = requireBearerAuth({
    verifier: provider,
    resourceMetadataUrl: getOAuthProtectedResourceMetadataUrl(mcpUrl),
  });

  app.post("/mcp", bearer, express.json({ limit: config.mcpBodyLimit ?? 1024 * 1024 }), async (req: Request, res: Response) => {
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
  app.get("/mcp", bearer, methodNotAllowed);
  app.delete("/mcp", bearer, methodNotAllowed);

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

function buildMcpServer(hub: AgentHub, log: Logger, offline?: OfflineReader, localTools?: LocalTools): McpServer {
  const server = new McpServer({ name: "spec-studio", version: "0.1.0" });
  for (const tool of TOOLS) {
    server.registerTool(
      tool.name,
      { title: tool.title, description: tool.description, inputSchema: tool.inputSchema },
      async (args: Record<string, unknown>) => {
        const start = Date.now();
        let result = await hub.call(tool.name, args ?? {});
        const fromMirror = result.offline === true && offline !== undefined && OFFLINE_READ_TOOLS.has(tool.name);
        if (fromMirror) result = await offline.answer(tool.name, args ?? {}, hub.offlineText());
        log({ event: "tool_call", tool: tool.name, ok: result.ok, offline: fromMirror || undefined, ms: Date.now() - start });
        return {
          content: [{ type: "text" as const, text: result.text }],
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
          return {
            content: [{ type: "text" as const, text: result.text }],
            ...(result.ok ? {} : { isError: true }),
          };
        },
      );
    }
  }
  return server;
}
