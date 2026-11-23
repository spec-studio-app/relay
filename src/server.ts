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
import { SingleUserProvider } from "./oauth.js";
import { TOOLS } from "./tools.js";
import { consoleLogger, type Logger } from "./log.js";

export const DEFAULT_PORT = 8760;
export const DEFAULT_CALL_TIMEOUT_MS = 90_000;
export const DEFAULT_PING_INTERVAL_MS = 20_000;
/** R6.S1: a call to a sleeping Mac answers "offline" in 5 seconds or less. */
export const DEFAULT_PROBE_TIMEOUT_MS = 3_000;

export interface RelayConfig {
  /** The public origin, e.g. https://studio-mcp.loomis.io. It is the OAuth issuer. */
  publicUrl: string;
  loginPasswordBcrypt: string;
  agentSecretSha256: string;
  callTimeoutMs?: number;
  pingIntervalMs?: number;
  probeTimeoutMs?: number;
  /** Redirect URIs that clients may register, in addition to the defaults (see oauth.ts). */
  extraRedirectUris?: string[];
  /** The clock for the agent's last-seen and start times. */
  now?: () => Date;
  log?: Logger;
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
    agentSecretSha256: config.agentSecretSha256.toLowerCase(),
    callTimeoutMs: config.callTimeoutMs ?? DEFAULT_CALL_TIMEOUT_MS,
    pingIntervalMs: config.pingIntervalMs ?? DEFAULT_PING_INTERVAL_MS,
    probeTimeoutMs: config.probeTimeoutMs ?? DEFAULT_PROBE_TIMEOUT_MS,
    now,
    log,
  });
  // The provider keeps the real clock: the SDK's bearer check compares token
  // expiry with Date.now().
  const provider = new SingleUserProvider({
    loginPasswordBcrypt: config.loginPasswordBcrypt,
    extraRedirectUris: config.extraRedirectUris ?? [],
    now: () => new Date(),
    log,
  });

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

  app.post("/mcp", bearer, express.json({ limit: "1mb" }), async (req: Request, res: Response) => {
    // Stateless: one server and one transport for each request.
    const server = buildMcpServer(hub, log);
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

function buildMcpServer(hub: AgentHub, log: Logger): McpServer {
  const server = new McpServer({ name: "spec-studio", version: "0.1.0" });
  for (const tool of TOOLS) {
    server.registerTool(
      tool.name,
      { title: tool.title, description: tool.description, inputSchema: tool.inputSchema },
      async (args: Record<string, unknown>) => {
        const start = Date.now();
        const result = await hub.call(tool.name, args ?? {});
        log({ event: "tool_call", tool: tool.name, ok: result.ok, ms: Date.now() - start });
        return {
          content: [{ type: "text" as const, text: result.text }],
          ...(result.ok ? {} : { isError: true }),
        };
      },
    );
  }
  return server;
}

export interface EnvConfig {
  port: number;
  relay: RelayConfig;
}

/** Read the configuration from the environment. Throws when a required value is missing or bad. */
export function configFromEnv(env: NodeJS.ProcessEnv): EnvConfig {
  const missing = ["PUBLIC_URL", "LOGIN_PASSWORD_BCRYPT", "AGENT_SECRET_SHA256"].filter((name) => !env[name]?.trim());
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(", ")}`);
  }
  const publicUrl = env.PUBLIC_URL!.trim();
  try {
    new URL(publicUrl);
  } catch {
    throw new Error("PUBLIC_URL is not a valid URL.");
  }
  const hash = env.AGENT_SECRET_SHA256!.trim();
  if (!/^[0-9a-fA-F]{64}$/.test(hash)) {
    throw new Error("AGENT_SECRET_SHA256 must be 64 hex characters (the SHA-256 of the agent secret).");
  }
  const bcryptHash = env.LOGIN_PASSWORD_BCRYPT!.trim();
  if (!/^\$2[aby]\$\d{2}\$.{53}$/.test(bcryptHash)) {
    throw new Error("LOGIN_PASSWORD_BCRYPT is not a bcrypt hash.");
  }
  const port = env.PORT ? Number(env.PORT) : DEFAULT_PORT;
  if (!Number.isInteger(port) || port <= 0) throw new Error("PORT is not a valid port.");
  const callTimeoutMs = env.CALL_TIMEOUT_MS ? Number(env.CALL_TIMEOUT_MS) : DEFAULT_CALL_TIMEOUT_MS;
  if (!Number.isFinite(callTimeoutMs) || callTimeoutMs <= 0) throw new Error("CALL_TIMEOUT_MS is not a valid number.");

  const extraRedirectUris = (env.EXTRA_REDIRECT_URIS ?? "")
    .split(",")
    .map((uri) => uri.trim())
    .filter((uri) => uri.length > 0);
  for (const uri of extraRedirectUris) {
    try {
      new URL(uri);
    } catch {
      throw new Error(`EXTRA_REDIRECT_URIS has a URI that is not valid: ${uri}`);
    }
  }

  return {
    port,
    relay: { publicUrl, loginPasswordBcrypt: bcryptHash, agentSecretSha256: hash, callTimeoutMs, extraRedirectUris },
  };
}
