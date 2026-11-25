// The hub for the one agent connection from the user's Mac.
//
// Wire protocol (the Swift agent implements the other side). Version 1:
//   relay -> agent: {"type":"call","id":"<uuid>","tool":"<name>","arguments":{...}}
//   agent -> relay: {"type":"result","id":"<same>","ok":true|false,"text":"<string>"}
//   agent -> relay: {"type":"hello","version":1}   (optional, after connect)
// The handshake (wire version 3): the hello has "version", "agentVersion" and
// "wires" (the wire versions that the agent speaks; a hello with no list
// counts as [version]). The relay picks the highest common wire version and
// sends a welcome with "version", "relayVersion" and its own "wires". With no
// common version it sends
//   relay -> agent: {"type":"incompatible","tooOld":"app"|"relay","relayVersion":"...","wires":[...],"minAppVersion":"...","text":"..."}
// and closes the connection with close code 4002.
// A frame that the relay refuses gets an answer: a sync frame gets "synced"
// with an "error", another frame gets
//   relay -> agent: {"type":"error","frame":"<type of the refused frame>","text":"<reason>"}
// Version 3 (wire.md in the app's specs/studio-relay): hello with repos,
// welcome, sync and synced, message and delivered, upload-url and
// download-url, gone and gone-ok. An agent of an older version gets tool
// calls only.
//   agent -> relay: {"type":"sync","spec":{...},"first":true,"lines":[{"line":<n>,"event":{...}},...],"state":{...},"turn":{...}|null}
//     The new activity lines of a spec from one drain, in ascending order.
//     "first" only when the agent had no cursor for the spec (a short first
//     sync: the last 20 lines and the earlier messages). "state" and "turn"
//     only when they changed since the last frame on this connection; a frame
//     without them keeps the mirror's. The workflow applies the frame as one
//     update.
//   relay -> agent: {"type":"synced","spec":{...},"line":<last line of the frame>}
//     With an "error" field when the relay did not apply the lines.
//   agent -> relay: {"type":"gone","spec":{"repoKey":"<12 hex>","slug":"<slug>"}}
//     A spec that the agent synced no longer exists on the Mac.
//   relay -> agent: {"type":"gone-ok","spec":{"repoKey":"...","slug":"..."}}
//     The relay marked the spec dropped: the agent may drop its cursor. With an
//     "error" field, the relay could not apply it: send it again later.
//   agent -> relay: {"type":"state","spec":{"repoKey":"...","slug":"..."},"state":{...},"turn":{...}|null}
//     The state (`studio show --json`) or the turn changed with no new activity
//     line. The relay replaces the mirror's state and turn of a spec whose
//     workflow runs and is not dropped, after the earlier sync frames of that
//     spec. No answer frame.
// Desks (wire-desks.md in the app's specs/relay-desk-messages; the wire
// version stays 3). The welcome says "desks": true when the relay has a desk
// store. An agent sends no desk frame to a relay without it, and the relay
// sends desk-message frames only to a connection that sent a desks frame.
//   agent -> relay: {"type":"desks","desks":[{"repoKey","repo","open","turn","canMessage","reason"}]}
//     The state of each desk. The relay saves each one and pushes every desk
//     message that waits for the Mac. No answer, or an "error" frame.
//   relay -> agent: {"type":"desk-message","desk":{"repoKey","repo"},"seq":<n>,"message":{...}}
//   agent -> relay: {"type":"desk-delivered","desk":{...},"seq":<n>,"error":"<text>"?}
//     With an error, the message goes back to Claude AI with the reason.
//   agent -> relay: {"type":"desk-sync","desk":{...},"lines":[{"line":<n>,"event":{...}}]}
//   relay -> agent: {"type":"desk-synced","desk":{...},"line":<n>,"error"?}
//   agent -> relay: upload-url with "desk" in place of "spec".
import { createHash, randomUUID } from "node:crypto";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import { WebSocketServer, WebSocket } from "ws";
import type { Logger } from "./log.js";
import type { AgentBackend } from "./backend.js";
import { INCOMPATIBLE_CLOSE_CODE, MIN_APP_VERSION_FOR_WIRE, RELAY_VERSION, SUPPORTED_WIRES, WIRE_VERSION } from "./version.js";
import type { DeskAddress, DeskMessage, DeskReport, Message, SpecAddress } from "./workflows/types.js";
import { DESK_GONE_TEXT } from "./workflows/types.js";

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
  /** The store: the paired secrets and the wire version of the sync. Without it no agent can connect. */
  backend?: AgentBackend;
  relayVersion?: string;
  /** The longest wait for the secret check of an upgrade (10 seconds when not given). */
  authTimeoutMs?: number;
  /**
   * While a current agent is connected, look for messages that wait for the
   * Mac this often (60 seconds when not given). Visibility is eventually
   * consistent, so the scan at the welcome can miss a message.
   */
  pendingScanMs?: number;
  /** A scan pushes again a message that still waits this long after its push (60 seconds when not given). */
  resendAfterMs?: number;
  /** Try a state frame that failed on a Temporal error again after this time (30 seconds when not given). */
  stateRetryMs?: number;
  /**
   * After an incompatible hello, the relay keeps the socket open this long
   * before the close (60 seconds when not given). An app older than v0.1.1
   * connects again 1 second after a close, so the wait limits its tries.
   */
  incompatibleCloseMs?: number;
}

export const DEFAULT_INCOMPATIBLE_CLOSE_MS = 60_000;

/** The most wire versions that the relay reads from a hello. */
export const MAX_HELLO_WIRES = 16;

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

/** The last hello of an agent: its versions, the time, and whether it got a welcome. */
export interface AgentRecord {
  /** The app version from the hello, or null when the hello has none. */
  appVersion: string | null;
  /** The wire versions that the agent speaks. */
  wires: number[];
  at: Date;
  compatible: boolean;
  /** Which side is too old, when the handshake failed. */
  tooOld?: "app" | "relay";
}

/** The wire versions of a hello: its "wires" list, or [version] when it has no list. */
export function agentWires(m: Record<string, unknown>): number[] {
  if (Array.isArray(m.wires)) {
    const wires = m.wires
      .slice(0, MAX_HELLO_WIRES)
      .filter((w): w is number => typeof w === "number" && Number.isInteger(w) && w > 0 && w < 1000);
    if (wires.length > 0) return [...new Set(wires)].sort((a, b) => a - b);
  }
  return typeof m.version === "number" && Number.isInteger(m.version) ? [m.version] : [];
}

/** "v0.1.0" for a version that starts with a digit, else the text as it is. */
function shownVersion(version: string | null): string {
  if (!version) return "of an unknown version";
  return /^\d/.test(version) ? `v${version}` : version;
}

export class AgentHub {
  private readonly wss = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024 * 1024 });
  private readonly startedAt: Date;
  private socket: WebSocket | null = null;
  private lastSeen: Date | null = null;
  /** Monotonic time (ms) of the last frame from the agent. */
  private lastHeardMs = 0;
  private readonly pending = new Map<string, PendingCall>();
  /** The wire version of each socket (1 until a hello of the current version). */
  private readonly versions = new WeakMap<WebSocket, number>();
  /** Sync lines of one spec apply in order: one promise chain per spec. */
  private readonly syncChains = new Map<string, Promise<void>>();
  /** The secret hash of each socket, so an unpair can close it. */
  private readonly hashes = new WeakMap<WebSocket, string>();
  /** The newest state frame of each spec while it applies or waits for a retry. */
  private readonly latestStates = new Map<string, StateEntry>();
  /** The timer of each current socket that looks for waiting messages again. */
  private readonly scanners = new WeakMap<WebSocket, NodeJS.Timeout>();
  /**
   * The messages pushed on each socket (message ID -> monotonic time in ms), so
   * a push goes once, until a scan finds it still waiting after resendAfterMs.
   */
  private readonly pushed = new WeakMap<WebSocket, Map<string, number>>();
  private lastHello: AgentRecord | null = null;
  /** The sockets that sent a desks frame: only they get desk-message frames. */
  private readonly deskCapable = new WeakSet<WebSocket>();

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
      this.refuse(ws, "unknown", "The frame is not JSON.");
      return;
    }
    if (!isObject(message)) {
      this.refuse(ws, "unknown", "The frame is not a JSON object.");
      return;
    }
    const m = message;
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
    if (m.type === "desks") {
      this.desksFrame(ws, m);
      return;
    }
    if (m.type === "desk-delivered") {
      this.deskDelivered(ws, m);
      return;
    }
    if (m.type === "desk-sync") {
      this.deskSync(ws, m);
      return;
    }
    if (m.type === "upload-url" || m.type === "download-url") {
      void this.signedUrl(ws, m);
      return;
    }
    if (m.type !== "result") {
      const type = typeof m.type === "string" ? m.type.slice(0, 40) : "unknown";
      this.refuse(ws, type, `The relay does not know the frame type "${type}".`);
      return;
    }
    if (typeof m.id !== "string") {
      this.refuse(ws, "result", "The result frame has no call ID.");
      return;
    }
    const call = this.pending.get(m.id);
    if (!call || call.socket !== ws) {
      // A result after its call timed out or its connection dropped: the caller already has an answer.
      this.options.log({ event: "agent_late_result" });
      return;
    }
    this.pending.delete(m.id);
    clearTimeout(call.timer);
    call.resolve({ ok: m.ok === true, text: typeof m.text === "string" ? m.text : "" });
  }

  /** Close the agent connection that uses this secret hash (after an unpair). */
  disconnect(hash: string): void {
    const ws = this.socket;
    if (ws && this.hashes.get(ws) === hash) ws.close(4001, "unpaired");
  }

  /** The last hello of an agent since the relay started, or null. */
  get lastAgent(): AgentRecord | null {
    return this.lastHello;
  }

  /**
   * The notice for the MCP tools while the last agent hello failed the
   * handshake, or null. lastSync is the time of the newest sync of any spec:
   * null when no spec synced, undefined when the relay cannot read it now.
   */
  mismatchNotice(lastSync: string | null | undefined): string | null {
    const agent = this.lastHello;
    if (!agent || agent.compatible) return null;
    const relayVersion = this.options.relayVersion ?? RELAY_VERSION;
    const synced =
      lastSync === undefined
        ? ""
        : ` Nothing has synced since ${lastSync ?? `the relay started at ${this.startedAt.toISOString()}`}.`;
    const ask = agent.tooOld === "relay" ? "Ask the user to update the relay." : "Ask the user to update the app.";
    return (
      `The Mac app (${shownVersion(agent.appVersion)}, wire ${agent.wires.join(", ")}) is not compatible with this relay ` +
      `(${relayVersion}, wire ${SUPPORTED_WIRES.join(", ")}).${synced} ${ask} ` +
      `The Mac last tried to connect at ${agent.at.toISOString()}.`
    );
  }

  /** The wire version of the current agent socket (0 when no agent). */
  get wireVersion(): number {
    return this.socket ? (this.versions.get(this.socket) ?? 1) : 0;
  }

  private send(ws: WebSocket, frame: Record<string, unknown>): void {
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify(frame));
  }

  /** Answer a refused frame with an error frame, and log the refusal. */
  private refuse(ws: WebSocket, frame: string, text: string): void {
    this.options.log({ event: "agent_frame_refused", frame, reason: text });
    this.send(ws, { type: "error", frame, text });
  }

  /** The reason for a frame that needs a welcome, from an agent that has none. */
  private notWelcomed(ws: WebSocket): string | null {
    if (!this.options.backend) return "The relay has no backend, so it takes tool results only.";
    if (this.versions.get(ws) === WIRE_VERSION) return null;
    return `The relay did not welcome this agent, so it does not take this frame. Send a hello of wire version ${WIRE_VERSION} first.`;
  }

  private hello(ws: WebSocket, m: Record<string, unknown>): void {
    const backend = this.options.backend;
    // A version 1 hello (no list of wire versions) keeps tool calls only.
    if (!backend || typeof m.version !== "number" || (m.version < 2 && !Array.isArray(m.wires))) return;
    const wires = agentWires(m);
    const appVersion = typeof m.agentVersion === "string" && m.agentVersion ? m.agentVersion.slice(0, 40) : null;
    const common = wires.filter((w) => SUPPORTED_WIRES.includes(w));
    if (common.length === 0) {
      this.incompatible(ws, appVersion, wires);
      return;
    }
    const version = Math.max(...common);
    this.lastHello = { appVersion, wires, at: this.options.now(), compatible: true };
    this.versions.set(ws, version);
    this.send(ws, {
      type: "welcome",
      version,
      relayVersion: this.options.relayVersion ?? RELAY_VERSION,
      wires: [...SUPPORTED_WIRES],
      ...(backend.desks ? { desks: true } : {}),
    });
    const repos = Array.isArray(m.repos) ? m.repos.filter((r): r is string => typeof r === "string") : [];
    this.options.log({ event: "agent_hello", version, appVersion: appVersion ?? undefined, repos: repos.length });
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
   * No common wire version: tell the agent which side is too old, then close
   * the connection with INCOMPATIBLE_CLOSE_CODE. The relay keeps the record,
   * so the MCP tools can tell Claude AI.
   */
  private incompatible(ws: WebSocket, appVersion: string | null, wires: number[]): void {
    const relayVersion = this.options.relayVersion ?? RELAY_VERSION;
    // agentWires sorts the list and keeps at most MAX_HELLO_WIRES entries.
    const highest = wires.length > 0 ? wires[wires.length - 1] : 0;
    const tooOld: "app" | "relay" = highest < SUPPORTED_WIRES[0] ? "app" : "relay";
    const minAppVersion = MIN_APP_VERSION_FOR_WIRE[SUPPORTED_WIRES[0]];
    const head =
      `The relay ${relayVersion} speaks wire version ${SUPPORTED_WIRES.join(", ")}. ` +
      `This app (${shownVersion(appVersion)}) speaks wire version ${wires.join(", ") || "unknown"}.`;
    const text =
      tooOld === "app"
        ? `${head} The relay needs app v${minAppVersion} or later. Update the app.`
        : `${head} Update the relay to a version that speaks wire version ${highest}.`;
    this.lastHello = { appVersion, wires, at: this.options.now(), compatible: false, tooOld };
    this.options.log({ event: "agent_incompatible", appVersion: appVersion ?? undefined, wires: wires.join(","), tooOld });
    this.send(ws, {
      type: "incompatible",
      tooOld,
      relayVersion,
      wires: [...SUPPORTED_WIRES],
      ...(minAppVersion ? { minAppVersion } : {}),
      text,
    });
    // A close reason has at most 123 bytes: the frame carries the full text.
    const reason =
      tooOld === "app"
        ? `incompatible: update the app to v${minAppVersion} or later`
        : `incompatible: update the relay to wire version ${highest}`;
    // An old app ignores the frame and connects again 1 second after a close.
    // Until the close, its tool calls still work, and the tools carry the notice.
    const delay = this.options.incompatibleCloseMs ?? DEFAULT_INCOMPATIBLE_CLOSE_MS;
    const timer = setTimeout(() => {
      if (ws.readyState === WebSocket.OPEN) ws.close(INCOMPATIBLE_CLOSE_CODE, reason);
    }, delay);
    timer.unref();
    ws.once("close", () => clearTimeout(timer));
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
    if (this.deskCapable.has(ws)) await this.pushPendingDesks(ws, this.options.resendAfterMs ?? DEFAULT_PENDING_SCAN_MS);
  }

  /**
   * Push every desk message that waits for the Mac, in seq order per desk.
   * With resendAfterMs 0 (after a desks frame) each one goes again.
   */
  private async pushPendingDesks(ws: WebSocket, resendAfterMs: number): Promise<void> {
    const desks = this.options.backend?.desks;
    if (!desks) return;
    try {
      const waiting = await desks.pendingForMac();
      for (const { desk, message } of waiting) this.pushDeskTo(ws, desk, message, resendAfterMs);
    } catch {
      this.options.log({ event: "push_pending_desks_failed" });
    }
  }

  private pushDeskTo(ws: WebSocket, desk: DeskAddress, message: DeskMessage, resendAfterMs?: number): void {
    if (this.versions.get(ws) !== WIRE_VERSION || !this.deskCapable.has(ws)) return;
    let pushed = this.pushed.get(ws);
    if (!pushed) {
      pushed = new Map();
      this.pushed.set(ws, pushed);
    }
    const at = pushed.get(message.id);
    const now = performance.now();
    if (at !== undefined && (resendAfterMs === undefined || now - at < resendAfterMs)) return;
    pushed.set(message.id, now);
    this.send(ws, { type: "desk-message", desk: wireDesk(desk), seq: message.seq, message });
  }

  /** Push a new desk message to the Mac when a desk-capable agent is connected. */
  pushDeskMessage(desk: DeskAddress, message: DeskMessage): boolean {
    const ws = this.socket;
    if (!ws || ws.readyState !== WebSocket.OPEN || this.versions.get(ws) !== WIRE_VERSION || !this.deskCapable.has(ws)) return false;
    this.pushDeskTo(ws, desk, message);
    return true;
  }

  /** True when the agent is connected and takes desk messages. */
  get desksConnected(): boolean {
    const ws = this.socket;
    return !!ws && ws.readyState === WebSocket.OPEN && this.deskCapable.has(ws);
  }

  /** The agent's desks frame: save each desk, then push the waiting desk messages. */
  private desksFrame(ws: WebSocket, m: Record<string, unknown>): void {
    const desks = this.options.backend?.desks;
    if (!desks || this.versions.get(ws) !== WIRE_VERSION) {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    const reports = parseDeskReports(m.desks);
    if (!reports) {
      this.options.log({ event: "agent_bad_desks" });
      this.send(ws, { type: "error", frame: "desks", text: "The desks frame is not valid." });
      return;
    }
    this.deskCapable.add(ws);
    this.options.log({ event: "agent_desks", desks: reports.length });
    // Each desk's frames apply in order (the desks frame and desk-sync).
    const saved = reports.map(
      (report) =>
        new Promise<boolean>((resolve) =>
          this.chain(deskChainKey(report), async () => {
            try {
              await desks.report([report]);
              resolve(true);
            } catch {
              resolve(false);
            }
          }),
        ),
    );
    // A desk record that the frame does not list belongs to a repo that left
    // Spec Studio or moved: it takes no more messages, and a name does not find it.
    const listed = new Set(reports.map((report) => report.repoKey));
    const gone = (desks.list ? desks.list() : Promise.resolve([]))
      .then((all) => all.filter((d) => !listed.has(d.repoKey) && d.reason !== DESK_GONE_TEXT))
      .catch(() => [])
      .then((stale) =>
        Promise.all(
          stale.map(
            (d) =>
              new Promise<boolean>((resolve) =>
                this.chain(deskChainKey({ repoKey: d.repoKey }), async () => {
                  try {
                    await desks.report([
                      { repoKey: d.repoKey, repo: d.repo ?? "", open: false, turn: null, canMessage: false, reason: DESK_GONE_TEXT },
                    ]);
                    resolve(true);
                  } catch {
                    resolve(false);
                  }
                }),
              ),
          ),
        ),
      );
    void Promise.all([Promise.all(saved), gone]).then(async ([results]) => {
      if (results.some((ok) => !ok)) {
        this.options.log({ event: "desks_failed" });
        this.send(ws, { type: "error", frame: "desks", text: "The relay cannot save the desks now." });
      }
      if (ws.readyState === WebSocket.OPEN) await this.pushPendingDesks(ws, 0);
    });
  }

  private deskDelivered(ws: WebSocket, m: Record<string, unknown>): void {
    const desks = this.options.backend?.desks;
    const desk = parseDesk(m.desk);
    if (!desks || this.versions.get(ws) !== WIRE_VERSION || !desk || typeof m.seq !== "number") {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    const error = typeof m.error === "string" && m.error ? m.error.slice(0, 500) : undefined;
    this.options.log({ event: "desk_message_delivered", ok: error === undefined });
    const seq = m.seq;
    this.chain(deskChainKey(desk), async () => {
      try {
        await desks.ack(desk, seq, error);
      } catch {
        this.options.log({ event: "desk_ack_failed" });
      }
    });
  }

  private deskSync(ws: WebSocket, m: Record<string, unknown>): void {
    const desks = this.options.backend?.desks;
    if (!desks || this.versions.get(ws) !== WIRE_VERSION) {
      this.options.log({ event: "agent_bad_message" });
      return;
    }
    const desk = parseDesk(m.desk);
    const lines = parseLines(m.lines);
    if (!desk || !lines) {
      this.options.log({ event: "agent_bad_desk_sync" });
      const last = lastLineNumber(m.lines);
      if (desk && last !== null) {
        this.send(ws, { type: "desk-synced", desk: wireDesk(desk), line: last, error: "The desk-sync frame is not valid." });
      }
      return;
    }
    const line = lines[lines.length - 1].line;
    this.chain(deskChainKey(desk), async () => {
      try {
        await desks.sync(desk, lines);
        this.send(ws, { type: "desk-synced", desk: wireDesk(desk), line });
      } catch {
        this.options.log({ event: "desk_sync_failed" });
        this.send(ws, { type: "desk-synced", desk: wireDesk(desk), line, error: "The relay cannot apply the lines now." });
      }
    });
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

  /** Push a new message to the Mac when a current agent is connected. */
  pushMessage(spec: SpecAddress, message: Message): boolean {
    const ws = this.socket;
    if (!ws || ws.readyState !== WebSocket.OPEN || this.versions.get(ws) !== WIRE_VERSION) return false;
    this.pushTo(ws, spec, message);
    return true;
  }

  /** The agent asks for a signed URL to upload or download an attachment. */
  private async signedUrl(ws: WebSocket, m: Record<string, unknown>): Promise<void> {
    const backend = this.options.backend;
    const type = m.type as string;
    const refused = this.notWelcomed(ws) ?? (typeof m.id !== "string" ? `The ${type} frame has no ID.` : null);
    if (!backend || refused !== null || typeof m.id !== "string") {
      const text = refused ?? `The ${type} frame is not valid.`;
      if (typeof m.id !== "string") {
        this.refuse(ws, type, text);
        return;
      }
      // The agent waits for the answer with this ID.
      this.options.log({ event: "agent_frame_refused", frame: type, reason: text });
      this.send(ws, { type, id: m.id, ok: false, text });
      return;
    }
    const id = m.id;
    try {
      if (m.type === "upload-url") {
        // A desk's file has "desk" in place of "spec".
        const desk = m.spec === undefined && backend.desks ? parseDesk(m.desk) : null;
        const spec = desk ? null : parseSpec(m.spec);
        if ((!spec && !desk) || typeof m.filename !== "string" || typeof m.size !== "number") {
          this.options.log({ event: "agent_frame_refused", frame: "upload-url", reason: "The upload-url frame is not valid." });
          this.send(ws, { type: "upload-url", id, ok: false, text: "The upload-url frame is not valid." });
          return;
        }
        const contentType = typeof m.contentType === "string" && m.contentType ? m.contentType : "application/octet-stream";
        const answer = desk
          ? await backend.desks!.uploadUrl(desk, m.filename, contentType, m.size)
          : await backend.uploadUrl(spec!, m.filename, contentType, m.size);
        if (!answer.ok) this.options.log({ event: "agent_frame_refused", frame: "upload-url", reason: answer.text });
        this.send(ws, { type: "upload-url", id, ...answer });
        return;
      }
      const objectKey = typeof m.objectKey === "string" ? m.objectKey : "";
      const url = objectKey ? await backend.downloadUrl(objectKey) : null;
      if (!url) {
        const text = objectKey ? "The relay has no such attachment." : "The download-url frame has no objectKey.";
        this.options.log({ event: "agent_frame_refused", frame: "download-url", reason: text });
        this.send(ws, { type: "download-url", id, ok: false, text });
        return;
      }
      this.send(ws, { type: "download-url", id, ok: true, url });
    } catch {
      this.options.log({ event: "signed_url_failed" });
      this.send(ws, { type: m.type, id, ok: false, text: "The relay cannot reach the bucket now." });
    }
  }

  private delivered(ws: WebSocket, m: Record<string, unknown>): void {
    const backend = this.options.backend;
    const spec = parseSpec(m.spec);
    const refused = this.notWelcomed(ws) ?? (!spec || typeof m.seq !== "number" ? "The delivered frame needs a spec and a seq." : null);
    if (!backend || !spec || typeof m.seq !== "number" || refused !== null) {
      this.refuse(ws, "delivered", refused ?? "The delivered frame is not valid.");
      return;
    }
    const error = typeof m.error === "string" && m.error ? m.error.slice(0, 500) : undefined;
    this.options.log({ event: "message_delivered", ok: error === undefined });
    backend.ack(spec, m.seq, error).catch(() => this.options.log({ event: "ack_failed" }));
  }

  private sync(ws: WebSocket, m: Record<string, unknown>): void {
    const backend = this.options.backend;
    const notWelcomed = this.notWelcomed(ws);
    const spec = parseSpec(m.spec);
    const lines = parseLines(m.lines);
    const hasState = "state" in m;
    const refused =
      notWelcomed ??
      (!spec || !lines || (hasState && !isObject(m.state) && m.state !== null) ? "The sync frame is not valid." : null);
    if (!backend || !spec || !lines || refused !== null) {
      const text = refused ?? "The sync frame is not valid.";
      this.options.log({ event: "agent_frame_refused", frame: "sync", reason: text });
      // The agent waits for synced with the frame's last line: answer it when the frame names one.
      const last = lastLineNumber(m.lines) ?? (typeof m.line === "number" && Number.isInteger(m.line) ? m.line : null);
      if (spec && last !== null) this.send(ws, { type: "synced", spec: wireSpec(spec), line: last, error: text });
      else this.send(ws, { type: "error", frame: "sync", text });
      return;
    }
    const line = lines[lines.length - 1].line;
    // The frame with a state carries the state of its time: a failed state frame before it is old now.
    if (hasState) this.forgetState(specKey(spec));
    this.chain(spec, async () => {
      try {
        await backend.sync(spec, {
          lines,
          ...(m.first === true ? { first: true } : {}),
          ...(hasState ? { state: isObject(m.state) ? (m.state as Record<string, unknown>) : null, turn: parseTurn(m.turn) } : {}),
        });
        this.send(ws, { type: "synced", spec: wireSpec(spec), line });
      } catch {
        this.options.log({ event: "sync_failed" });
        this.send(ws, { type: "synced", spec: wireSpec(spec), line, error: "The relay cannot apply the lines now." });
      }
    });
  }

  /** The spec's frames (sync and gone) apply in order: one promise chain per spec (or desk). */
  private chain(spec: SpecAddress | string, work: () => Promise<void>): void {
    const key = typeof spec === "string" ? spec : specKey(spec);
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
    const refused = this.notWelcomed(ws) ?? (!spec || !isObject(m.state) ? "The state frame needs a spec and a state object." : null);
    if (!backend || !spec || !isObject(m.state) || refused !== null) {
      this.refuse(ws, "state", refused ?? "The state frame is not valid.");
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
   * state frame or a sync frame with a state comes for the spec, or the agent
   * disconnects.
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
    const refused = this.notWelcomed(ws) ?? (!spec ? "The gone frame needs a spec." : null);
    if (!backend || !spec || refused !== null) {
      const text = refused ?? "The gone frame is not valid.";
      this.options.log({ event: "agent_frame_refused", frame: "gone", reason: text });
      if (spec) this.send(ws, { type: "gone-ok", spec: wireSpec(spec), error: text });
      else this.send(ws, { type: "error", frame: "gone", text });
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

/** A desk address from a frame: a 12-hex repoKey, and the repo path when it is given. */
export function parseDesk(value: unknown): DeskAddress | null {
  if (!isObject(value)) return null;
  const { repoKey, repo } = value;
  if (typeof repoKey !== "string" || !/^[0-9a-f]{12}$/.test(repoKey)) return null;
  return { repoKey, ...(typeof repo === "string" && repo ? { repo } : {}) };
}

/** The most desks that one desks frame may carry. */
export const MAX_DESKS = 500;

/** The entries of a desks frame. Null when an entry breaks a rule. */
export function parseDeskReports(value: unknown): DeskReport[] | null {
  if (!Array.isArray(value) || value.length > MAX_DESKS) return null;
  const out: DeskReport[] = [];
  for (const item of value) {
    if (!isObject(item)) return null;
    const desk = parseDesk(item);
    if (!desk || !desk.repo || typeof item.open !== "boolean" || typeof item.canMessage !== "boolean") return null;
    if (item.turn !== null && item.turn !== undefined && !isObject(item.turn)) return null;
    const reason = typeof item.reason === "string" && item.reason ? item.reason.slice(0, 500) : null;
    out.push({ repoKey: desk.repoKey, repo: desk.repo, open: item.open, turn: parseTurn(item.turn), canMessage: item.canMessage, reason });
  }
  return out;
}

/** The chain key of a desk: no spec key can be this (a spec key has a slash). */
function deskChainKey(desk: { repoKey: string }): string {
  return `desk:${desk.repoKey}`;
}

function wireDesk(desk: DeskAddress): DeskAddress {
  return { repoKey: desk.repoKey, ...(desk.repo ? { repo: desk.repo } : {}) };
}

function wireSpec(spec: SpecAddress, withRepo = false): SpecAddress {
  return { repoKey: spec.repoKey, slug: spec.slug, ...(withRepo && spec.repo ? { repo: spec.repo } : {}) };
}

function parseTurn(value: unknown): { turn: string; needsYou: boolean } | null {
  if (!isObject(value) || typeof value.turn !== "string") return null;
  return { turn: value.turn, needsYou: value.needsYou === true };
}

/** The most lines that one sync frame may carry. The agent sends at most 200. */
export const MAX_SYNC_LINES = 1000;

/**
 * The lines of a sync frame: at least one, each with an integer line number
 * of 1 or more and an event object, in strictly ascending order. Null when
 * the frame breaks a rule.
 */
export function parseLines(value: unknown): Array<{ line: number; event: Record<string, unknown> }> | null {
  if (!Array.isArray(value) || value.length === 0 || value.length > MAX_SYNC_LINES) return null;
  const lines: Array<{ line: number; event: Record<string, unknown> }> = [];
  for (const item of value) {
    if (!isObject(item)) return null;
    const { line, event } = item;
    if (typeof line !== "number" || !Number.isInteger(line) || line < 1 || !isObject(event)) return null;
    if (lines.length > 0 && line <= lines[lines.length - 1].line) return null;
    lines.push({ line, event });
  }
  return lines;
}

/** The line number of the last entry of a frame's lines, for an error answer. Null when there is none. */
function lastLineNumber(value: unknown): number | null {
  if (!Array.isArray(value) || value.length === 0) return null;
  const last = value[value.length - 1];
  return isObject(last) && typeof last.line === "number" && Number.isInteger(last.line) ? last.line : null;
}
