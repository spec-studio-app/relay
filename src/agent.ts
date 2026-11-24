// The hub for the one agent connection from the user's Mac.
//
// Wire protocol (the Swift agent implements the other side). Version 1:
//   relay -> agent: {"type":"call","id":"<uuid>","tool":"<name>","arguments":{...}}
//   agent -> relay: {"type":"result","id":"<same>","ok":true|false,"text":"<string>"}
//   agent -> relay: {"type":"hello","version":1}   (optional, after connect)
// Version 2 (wire.md in the spec): hello with repos, welcome, sync and synced,
// message and delivered, upload-url and download-url, gone and gone-ok. A
// version 1 agent gets tool calls only.
//   agent -> relay: {"type":"gone","spec":{"repoKey":"<12 hex>","slug":"<slug>"}}
//     A spec that the agent synced no longer exists on the Mac.
//   relay -> agent: {"type":"gone-ok","spec":{"repoKey":"...","slug":"..."}}
//     The relay marked the spec dropped: the agent may drop its cursor. With an
//     "error" field, the relay could not apply it: send it again later.
//   agent -> relay: {"type":"state","spec":{"repoKey":"...","slug":"..."},"state":{...},"turn":{...}|null}
//     The state (`studio show --json`) or the turn changed with no new activity
//     line. The relay replaces the mirror's state and turn of a spec whose
//     workflow runs and is not finished, after the earlier sync frames of that
//     spec. No answer frame.
import { createHash, randomUUID } from "node:crypto";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import { WebSocketServer, WebSocket } from "ws";
import type { Logger } from "./log.js";
import type { AgentBackend } from "./backend.js";
import { RELAY_VERSION, WIRE_VERSION } from "./version.js";
import type { Message, SpecAddress } from "./workflows/types.js";

export interface CallResult {
  ok: boolean;
  text: string;
  /** True when the call did not reach the Mac because it is offline. */
  offline?: boolean;
}

export interface AgentHubOptions {
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
  /** Temporal: the paired secrets and wire version 2. Without it no agent can connect. */
  backend?: AgentBackend;
  relayVersion?: string;
  /** The longest wait for the secret check of an upgrade (10 seconds when not given). */
  authTimeoutMs?: number;
  /**
   * While a version 2 agent is connected, look for messages that wait for the
   * Mac this often (60 seconds when not given). Visibility is eventually
   * consistent, so the scan at the welcome can miss a message.
   */
  pendingScanMs?: number;
  /** A scan pushes again a message that still waits this long after its push (60 seconds when not given). */
  resendAfterMs?: number;
  /** Try a state frame that failed on a Temporal error again after this time (30 seconds when not given). */
  stateRetryMs?: number;
}

export const DEFAULT_STATE_RETRY_MS = 30_000;

interface StateEntry {
  ws: WebSocket;
  spec: SpecAddress;
  state: Record<string, unknown>;
  turn: { turn: string; needsYou: boolean } | null;
  timer?: NodeJS.Timeout;
}

function specKey(spec: { repoKey: string; slug: string }): string {
  return `${spec.repoKey}/${spec.slug}`;
}

export const DEFAULT_PENDING_SCAN_MS = 60_000;

export const DEFAULT_AGENT_AUTH_TIMEOUT_MS = 10_000;

interface PendingCall {
  socket: WebSocket;
  resolve: (result: CallResult) => void;
  timer: NodeJS.Timeout;
}

export const AGENT_PATH = "/agent";

export class AgentHub {
  private readonly wss = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024 * 1024 });
  private readonly startedAt: Date;
  private socket: WebSocket | null = null;
  private lastSeen: Date | null = null;
  /** Monotonic time (ms) of the last frame from the agent. */
  private lastHeardMs = 0;
  private readonly pending = new Map<string, PendingCall>();
  /** The wire version of each socket (1 until a version 2 hello). */
  private readonly versions = new WeakMap<WebSocket, number>();
  /** Sync lines of one spec apply in order: one promise chain per spec. */
  private readonly syncChains = new Map<string, Promise<void>>();
  /** The secret hash of each socket, so an unpair can close it. */
  private readonly hashes = new WeakMap<WebSocket, string>();
  /** The newest state frame of each spec while it applies or waits for a retry. */
  private readonly latestStates = new Map<string, StateEntry>();
  /** The timer of each version 2 socket that looks for waiting messages again. */
  private readonly scanners = new WeakMap<WebSocket, NodeJS.Timeout>();
  /**
   * The messages pushed on each socket (message ID -> monotonic time in ms), so
   * a push goes once, until a scan finds it still waiting after resendAfterMs.
   */
  private readonly pushed = new WeakMap<WebSocket, Map<string, number>>();

  constructor(private readonly options: AgentHubOptions) {
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

  /** The SHA-256 (hex) of the bearer secret in the header, or null. */
  static secretHash(header: string | string[] | undefined): string | null {
    if (typeof header !== "string") return null;
    const match = /^Bearer\s+(.+)$/i.exec(header.trim());
    if (!match) return null;
    return createHash("sha256").update(match[1], "utf8").digest("hex");
  }

  /** True when the secret is a paired secret. Pairing is the only way to get one. */
  async authorizedHash(hash: string | null): Promise<boolean> {
    if (hash === null || !this.options.backend) return false;
    try {
      return await this.options.backend.verifyAgent(hash);
    } catch {
      this.options.log({ event: "agent_verify_failed" });
      return false;
    }
  }

  /** Handle an HTTP upgrade. Returns false when the path is not the agent path. */
  handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): boolean {
    const path = (req.url ?? "").split("?")[0];
    if (path !== AGENT_PATH) return false;

    // Node removes its own error listener before it emits 'upgrade'. During the
    // secret check (a Temporal query) a reset of the socket must not crash the relay.
    const onError = () => {
      this.options.log({ event: "agent_upgrade_socket_error" });
      socket.destroy();
    };
    socket.on("error", onError);
    const refuse = (status: string) => {
      if (!socket.destroyed) {
        socket.end(`HTTP/1.1 ${status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
        socket.destroy();
      }
    };
    const hash = AgentHub.secretHash(req.headers.authorization);
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => resolve("timeout"), this.options.authTimeoutMs ?? DEFAULT_AGENT_AUTH_TIMEOUT_MS);
    });
    void Promise.race([this.authorizedHash(hash), timeout]).then((ok) => {
      clearTimeout(timer);
      if (ok === "timeout") {
        this.options.log({ event: "agent_rejected", status: 503 });
        refuse("503 Service Unavailable");
        return;
      }
      if (!ok) {
        this.options.log({ event: "agent_rejected", status: 401 });
        refuse("401 Unauthorized");
        return;
      }
      if (socket.destroyed) return;
      // ws adds its own error listener in handleUpgrade.
      socket.off("error", onError);
      this.wss.handleUpgrade(req, socket, head, (ws) => {
        if (hash) this.hashes.set(ws, hash);
        this.accept(ws);
      });
    });
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
      for (const [key, entry] of this.latestStates) if (entry.ws === ws) this.forgetState(key);
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
    if (m.type === "hello") {
      this.hello(ws, m);
      return;
    }
    if (m.type === "sync") {
      this.sync(ws, m);
      return;
    }
    if (m.type === "state") {
      this.stateFrame(ws, m);
      return;
    }
    if (m.type === "gone") {
      this.gone(ws, m);
      return;
    }
    if (m.type === "delivered") {
      this.delivered(ws, m);
      return;
    }
    if (m.type === "upload-url" || m.type === "download-url") {
      void this.signedUrl(ws, m);
      return;
    }
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

  /** Close the agent connection that uses this secret hash (after an unpair). */
  disconnect(hash: string): void {
    const ws = this.socket;
    if (ws && this.hashes.get(ws) === hash) ws.close(4001, "unpaired");
  }

  /** The wire version of the current agent socket (0 when no agent). */
  get wireVersion(): number {
    return this.socket ? (this.versions.get(this.socket) ?? 1) : 0;
  }

  private send(ws: WebSocket, frame: Record<string, unknown>): void {
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify(frame));
  }

  private hello(ws: WebSocket, m: Record<string, unknown>): void {
    const backend = this.options.backend;
    if (!backend || typeof m.version !== "number" || m.version < 2) return;
    this.versions.set(ws, WIRE_VERSION);
    this.send(ws, { type: "welcome", version: WIRE_VERSION, relayVersion: this.options.relayVersion ?? RELAY_VERSION });
    const repos = Array.isArray(m.repos) ? m.repos.filter((r): r is string => typeof r === "string") : [];
    this.options.log({ event: "agent_hello", version: WIRE_VERSION, repos: repos.length });
    backend.hello(repos, this.options.now()).catch(() => this.options.log({ event: "agent_hello_failed" }));
    void this.afterWelcome(ws);
    if (!this.scanners.has(ws)) {
      const scanner = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) return;
        void this.afterWelcome(ws);
      }, this.options.pendingScanMs ?? DEFAULT_PENDING_SCAN_MS);
      scanner.unref();
      this.scanners.set(ws, scanner);
      ws.once("close", () => clearInterval(scanner));
    }
  }

  /**
   * After a welcome, and again every pendingScanMs: push every message that
   * waits for the Mac, in seq order per spec. Each message goes once per connection.
   */
  private async afterWelcome(ws: WebSocket): Promise<void> {
    const backend = this.options.backend;
    if (!backend) return;
    try {
      const waiting = await backend.pendingForMac();
      // A message that still waits long after its push (for example, the ack of
      // its delivered failed) goes again. The agent answers delivered again for
      // a message that it already has.
      const resendAfter = this.options.resendAfterMs ?? DEFAULT_PENDING_SCAN_MS;
      for (const { spec, message } of waiting) this.pushTo(ws, spec, message, resendAfter);
    } catch {
      this.options.log({ event: "push_pending_failed" });
    }
  }

  private pushTo(ws: WebSocket, spec: SpecAddress, message: Message, resendAfterMs?: number): void {
    if (this.versions.get(ws) !== WIRE_VERSION) return;
    let pushed = this.pushed.get(ws);
    if (!pushed) {
      pushed = new Map();
      this.pushed.set(ws, pushed);
    }
    // The message ID: a new run of the same slug starts its seqs at 1 again.
    const key = message.id;
    const at = pushed.get(key);
    const now = performance.now();
    if (at !== undefined && (resendAfterMs === undefined || now - at < resendAfterMs)) return;
    pushed.set(key, now);
    this.send(ws, { type: "message", spec: wireSpec(spec, true), seq: message.seq, message });
  }

  /** Push a new message to the Mac when a version 2 agent is connected. */
  pushMessage(spec: SpecAddress, message: Message): boolean {
    const ws = this.socket;
    if (!ws || ws.readyState !== WebSocket.OPEN || this.versions.get(ws) !== WIRE_VERSION) return false;
    this.pushTo(ws, spec, message);
    return true;
  }

  /** The agent asks for a signed URL to upload or download an attachment. */
  private async signedUrl(ws: WebSocket, m: Record<string, unknown>): Promise<void> {
    const backend = this.options.backend;
    if (!backend || this.versions.get(ws) !== WIRE_VERSION || typeof m.id !== "string") {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    const id = m.id;
    try {
      if (m.type === "upload-url") {
        const spec = parseSpec(m.spec);
        if (!spec || typeof m.filename !== "string" || typeof m.size !== "number") {
          this.send(ws, { type: "upload-url", id, ok: false, text: "The upload-url frame is not valid." });
          return;
        }
        const contentType = typeof m.contentType === "string" && m.contentType ? m.contentType : "application/octet-stream";
        const answer = await backend.uploadUrl(spec, m.filename, contentType, m.size);
        this.send(ws, { type: "upload-url", id, ...answer });
        return;
      }
      const objectKey = typeof m.objectKey === "string" ? m.objectKey : "";
      const url = objectKey ? await backend.downloadUrl(objectKey) : null;
      this.send(
        ws,
        url ? { type: "download-url", id, ok: true, url } : { type: "download-url", id, ok: false, text: "The relay has no such attachment." },
      );
    } catch {
      this.options.log({ event: "signed_url_failed" });
      this.send(ws, { type: m.type, id, ok: false, text: "The relay cannot reach the bucket now." });
    }
  }

  private delivered(ws: WebSocket, m: Record<string, unknown>): void {
    const backend = this.options.backend;
    const spec = parseSpec(m.spec);
    if (!backend || this.versions.get(ws) !== WIRE_VERSION || !spec || typeof m.seq !== "number") {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    const error = typeof m.error === "string" && m.error ? m.error.slice(0, 500) : undefined;
    this.options.log({ event: "message_delivered", ok: error === undefined });
    backend.ack(spec, m.seq, error).catch(() => this.options.log({ event: "ack_failed" }));
  }

  private sync(ws: WebSocket, m: Record<string, unknown>): void {
    const backend = this.options.backend;
    if (!backend || this.versions.get(ws) !== WIRE_VERSION) {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    const spec = parseSpec(m.spec);
    const line = m.line;
    if (!spec || typeof line !== "number" || !Number.isInteger(line) || line < 1 || !isObject(m.event)) {
      this.options.log({ event: "agent_bad_sync" });
      if (spec && typeof line === "number") {
        this.send(ws, { type: "synced", spec: wireSpec(spec), line, error: "The sync frame is not valid." });
      }
      return;
    }
    // The line carries the state of its time: a failed state frame before it is old now.
    this.forgetState(specKey(spec));
    this.chain(spec, async () => {
      try {
        await backend.sync(spec, {
          line,
          event: m.event as Record<string, unknown>,
          state: isObject(m.state) ? (m.state as Record<string, unknown>) : null,
          turn: parseTurn(m.turn),
        });
        this.send(ws, { type: "synced", spec: wireSpec(spec), line });
      } catch {
        this.options.log({ event: "sync_failed" });
        this.send(ws, { type: "synced", spec: wireSpec(spec), line, error: "The relay cannot apply the line now." });
      }
    });
  }

  /** The spec's frames (sync and gone) apply in order: one promise chain per spec. */
  private chain(spec: SpecAddress, work: () => Promise<void>): void {
    const key = specKey(spec);
    const previous = this.syncChains.get(key) ?? Promise.resolve();
    const next = previous.then(work);
    this.syncChains.set(key, next);
    void next.finally(() => {
      if (this.syncChains.get(key) === next) this.syncChains.delete(key);
    });
  }

  private stateFrame(ws: WebSocket, m: Record<string, unknown>): void {
    const backend = this.options.backend;
    const spec = parseSpec(m.spec);
    if (!backend || this.versions.get(ws) !== WIRE_VERSION || !spec || !isObject(m.state)) {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    const entry: StateEntry = { ws, spec, state: m.state as Record<string, unknown>, turn: parseTurn(m.turn) };
    // A newer state frame replaces a failed one that waits for its retry.
    this.forgetState(specKey(spec));
    this.latestStates.set(specKey(spec), entry);
    this.applyState(entry);
  }

  /**
   * Apply a state frame after the earlier frames of its spec. When Temporal
   * fails, keep it and try again every stateRetryMs, until it applies, a newer
   * state or sync frame of the spec comes, or the agent disconnects.
   */
  private applyState(entry: StateEntry): void {
    const backend = this.options.backend!;
    const key = specKey(entry.spec);
    this.chain(entry.spec, async () => {
      try {
        await backend.state(entry.spec, entry.state, entry.turn);
        if (this.latestStates.get(key) === entry) this.latestStates.delete(key);
      } catch {
        this.options.log({ event: "state_failed" });
        if (this.latestStates.get(key) !== entry || entry.ws.readyState !== WebSocket.OPEN) return;
        entry.timer = setTimeout(() => {
          entry.timer = undefined;
          if (this.latestStates.get(key) !== entry || entry.ws.readyState !== WebSocket.OPEN) return;
          this.applyState(entry);
        }, this.options.stateRetryMs ?? DEFAULT_STATE_RETRY_MS);
        entry.timer.unref();
      }
    });
  }

  /** Stop the retry of the spec's failed state frame (a newer frame came). */
  private forgetState(key: string): void {
    const entry = this.latestStates.get(key);
    if (!entry) return;
    if (entry.timer) clearTimeout(entry.timer);
    this.latestStates.delete(key);
  }

  private gone(ws: WebSocket, m: Record<string, unknown>): void {
    const backend = this.options.backend;
    const spec = parseSpec(m.spec);
    if (!backend || this.versions.get(ws) !== WIRE_VERSION || !spec) {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    this.chain(spec, async () => {
      try {
        await backend.gone(spec);
        this.options.log({ event: "spec_gone" });
        this.send(ws, { type: "gone-ok", spec: wireSpec(spec) });
      } catch {
        this.options.log({ event: "gone_failed" });
        this.send(ws, { type: "gone-ok", spec: wireSpec(spec), error: "The relay cannot apply it now." });
      }
    });
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
    if (!ws) return { ok: false, text: this.offlineText(), offline: true };

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
        resolve({ ok: false, text: this.offlineText(), offline: true });
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

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** A spec address from a frame: a 12-hex repoKey and a slug. */
export function parseSpec(value: unknown): SpecAddress | null {
  if (!isObject(value)) return null;
  const { repoKey, slug, repo } = value;
  if (typeof repoKey !== "string" || !/^[0-9a-f]{12}$/.test(repoKey)) return null;
  if (typeof slug !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$/.test(slug)) return null;
  return { repoKey, slug, ...(typeof repo === "string" && repo ? { repo } : {}) };
}

function wireSpec(spec: SpecAddress, withRepo = false): SpecAddress {
  return { repoKey: spec.repoKey, slug: spec.slug, ...(withRepo && spec.repo ? { repo: spec.repo } : {}) };
}

function parseTurn(value: unknown): { turn: string; needsYou: boolean } | null {
  if (!isObject(value) || typeof value.turn !== "string") return null;
  return { turn: value.turn, needsYou: value.needsYou === true };
}
