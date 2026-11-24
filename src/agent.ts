// The hub for the one agent connection from the user's Mac.
//
// Wire protocol (the Swift agent implements the other side):
//   relay -> agent: {"type":"call","id":"<uuid>","tool":"<name>","arguments":{...}}
//   agent -> relay: {"type":"result","id":"<same>","ok":true|false,"text":"<string>"}
//   agent -> relay: {"type":"hello","version":1}   (optional, after connect)
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import { WebSocketServer, WebSocket } from "ws";
import type { Logger } from "./log.js";

export interface CallResult {
  ok: boolean;
  text: string;
}

export interface AgentHubOptions {
  /** Lowercase hex SHA-256 of the legacy agent secret, when set. */
  agentSecretSha256?: string;
  callTimeoutMs: number;
  pingIntervalMs: number;
  /**
   * Before a call, the relay pings the agent when it heard nothing from it in
   * this time. A sleeping Mac does not close its socket, so without the probe a
   * call would wait for the ping timer (up to two ping intervals).
   */
  probeTimeoutMs: number;
  now: () => Date;
  log: Logger;
}

interface PendingCall {
  socket: WebSocket;
  resolve: (result: CallResult) => void;
  timer: NodeJS.Timeout;
}

export const AGENT_PATH = "/agent";

export class AgentHub {
  private readonly wss = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024 * 1024 });
  private readonly expectedHash: Buffer | null;
  private readonly startedAt: Date;
  private socket: WebSocket | null = null;
  private lastSeen: Date | null = null;
  /** Monotonic time (ms) of the last frame from the agent. */
  private lastHeardMs = 0;
  private readonly pending = new Map<string, PendingCall>();

  constructor(private readonly options: AgentHubOptions) {
    this.expectedHash = options.agentSecretSha256 ? Buffer.from(options.agentSecretSha256, "hex") : null;
    this.startedAt = options.now();
  }

  get connected(): boolean {
    return this.socket !== null && this.socket.readyState === WebSocket.OPEN;
  }

  /** The text for a call that cannot reach the Mac. */
  offlineText(): string {
    if (this.lastSeen) {
      return `The Mac is offline. The relay last saw it at ${this.lastSeen.toISOString()}.`;
    }
    return `The Mac is offline. The relay did not see the Mac since it started at ${this.startedAt.toISOString()}.`;
  }

  /** True when the bearer secret in the header hashes to the configured hash. */
  private authorized(req: IncomingMessage): boolean {
    const header = req.headers.authorization;
    if (typeof header !== "string") return false;
    const match = /^Bearer\s+(.+)$/i.exec(header.trim());
    if (!match) return false;
    const actual = createHash("sha256").update(match[1], "utf8").digest();
    const expected = this.expectedHash;
    return expected !== null && actual.length === expected.length && timingSafeEqual(actual, expected);
  }

  /** Handle an HTTP upgrade. Returns false when the path is not the agent path. */
  handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): boolean {
    const path = (req.url ?? "").split("?")[0];
    if (path !== AGENT_PATH) return false;

    if (!this.authorized(req)) {
      this.options.log({ event: "agent_rejected", status: 401 });
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
      socket.destroy();
      return true;
    }

    this.wss.handleUpgrade(req, socket, head, (ws) => this.accept(ws));
    return true;
  }

  private accept(ws: WebSocket): void {
    const previous = this.socket;
    this.socket = ws;
    this.touch();
    this.options.log({ event: "agent_connected", replaced: previous !== null });
    if (previous) {
      previous.close(4000, "replaced by a new agent connection");
    }

    let alive = true;
    const pinger = setInterval(() => {
      if (!alive) {
        this.options.log({ event: "agent_ping_timeout" });
        ws.terminate();
        return;
      }
      alive = false;
      ws.ping();
    }, this.options.pingIntervalMs);
    pinger.unref();

    ws.on("pong", () => {
      alive = true;
      this.touch();
    });

    ws.on("message", (data) => {
      alive = true;
      this.touch();
      this.receive(ws, data.toString());
    });

    ws.on("close", () => {
      clearInterval(pinger);
      if (this.socket === ws) {
        this.socket = null;
        this.options.log({ event: "agent_disconnected" });
      }
      this.failPending(ws);
    });

    ws.on("error", () => {
      this.options.log({ event: "agent_socket_error" });
    });
  }

  private touch(): void {
    this.lastSeen = this.options.now();
    this.lastHeardMs = performance.now();
  }

  /**
   * True when the socket answers. When the agent was quiet for longer than the
   * probe time, send a ping and wait for a pong or a message. No answer in time:
   * the socket is dead (for example, the Mac sleeps), so close it.
   */
  private probe(ws: WebSocket): Promise<boolean> {
    const timeout = this.options.probeTimeoutMs;
    if (performance.now() - this.lastHeardMs < timeout) return Promise.resolve(true);
    return new Promise<boolean>((resolve) => {
      const finish = (alive: boolean) => {
        clearTimeout(timer);
        ws.off("pong", onAnswer);
        ws.off("message", onAnswer);
        ws.off("close", onClose);
        resolve(alive);
      };
      const onAnswer = () => finish(true);
      const onClose = () => finish(false);
      const timer = setTimeout(() => {
        this.options.log({ event: "agent_probe_timeout" });
        ws.terminate();
        finish(false);
      }, timeout);
      ws.on("pong", onAnswer);
      ws.on("message", onAnswer);
      ws.on("close", onClose);
      try {
        ws.ping();
      } catch {
        finish(false);
      }
    });
  }

  private receive(ws: WebSocket, raw: string): void {
    let message: unknown;
    try {
      message = JSON.parse(raw);
    } catch {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    if (typeof message !== "object" || message === null) return;
    const m = message as Record<string, unknown>;
    if (m.type === "hello") return;
    if (m.type !== "result" || typeof m.id !== "string") {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    const call = this.pending.get(m.id);
    if (!call || call.socket !== ws) return;
    this.pending.delete(m.id);
    clearTimeout(call.timer);
    call.resolve({ ok: m.ok === true, text: typeof m.text === "string" ? m.text : "" });
  }

  private failPending(ws: WebSocket): void {
    for (const [id, call] of this.pending) {
      if (call.socket !== ws) continue;
      this.pending.delete(id);
      clearTimeout(call.timer);
      // The Mac connected again (for example, after a wake) while this call was on
      // the old connection. The call may have run, so do not say "offline".
      const text = this.connected
        ? "The connection to the Mac dropped during the call, and the Mac connected again. The call may have run on the Mac. Check the result before you call again."
        : this.offlineText();
      call.resolve({ ok: false, text });
    }
  }

  /**
   * The agent's socket after a probe, or null when the Mac is offline. When the
   * Mac connects again during the probe (it woke up), the call uses the new socket.
   */
  private async liveSocket(): Promise<WebSocket | null> {
    let ws = this.socket;
    for (let tries = 0; tries < 2; tries++) {
      if (!ws || ws.readyState !== WebSocket.OPEN) return null;
      if ((await this.probe(ws)) && ws.readyState === WebSocket.OPEN) return ws;
      if (this.socket === ws) return null;
      ws = this.socket;
    }
    return null;
  }

  /** Send one tool call to the agent and wait for its result. Never rejects. */
  async call(tool: string, args: Record<string, unknown>): Promise<CallResult> {
    const ws = await this.liveSocket();
    if (!ws) return { ok: false, text: this.offlineText() };

    const id = randomUUID();
    return new Promise<CallResult>((resolve) => {
      const seconds = this.options.callTimeoutMs / 1000;
      const timer = setTimeout(() => {
        this.pending.delete(id);
        resolve({ ok: false, text: `The Mac did not answer in time (${seconds} seconds).` });
      }, this.options.callTimeoutMs);
      this.pending.set(id, { socket: ws, resolve, timer });

      ws.send(JSON.stringify({ type: "call", id, tool, arguments: args }), (error) => {
        if (!error) return;
        const call = this.pending.get(id);
        if (!call) return;
        this.pending.delete(id);
        clearTimeout(call.timer);
        resolve({ ok: false, text: this.offlineText() });
      });
    });
  }

  close(): void {
    for (const [id, call] of this.pending) {
      this.pending.delete(id);
      clearTimeout(call.timer);
      call.resolve({ ok: false, text: this.offlineText() });
    }
    this.socket?.terminate();
    this.socket = null;
    this.wss.close();
  }
}
