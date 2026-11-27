// The host client: start a stdio plugin or connect to an HTTP plugin, do the
// handshake, and make typed calls. A stdio plugin that exits is started
// again with backoff, and the client runs the same handshake again.
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { PluginError, RPC, type RpcErrorObject } from "./errors.js";
import {
  PROTOCOL_VERSIONS,
  type Attributes,
  type BlobsBeginResult,
  type BlobsGetResult,
  type Capability,
  type DescribeResult,
  type Document,
  type EventsAppendResult,
  type EventsReadParams,
  type EventsReadResult,
  type Event,
  type HandshakeParams,
  type HandshakeResult,
  type HostKind,
  type Json,
  type NewEvent,
  type PingResult,
  type StateDeleteParams,
  type StateGetParams,
  type StateGetResult,
  type StateListParams,
  type StateListResult,
  type StatePutParams,
  type StatePutResult,
  type WatchWaitParams,
  type WatchWaitResult,
} from "./types.js";

/** A log entry. The client never logs a setting's value. */
export type Logger = (entry: { event: string; [key: string]: unknown }) => void;

/** Sees each message: "sent" for a request, "received" for a response. */
export type MessageObserver = (direction: "sent" | "received", method: string, raw: string) => void;

export interface ConnectOptions {
  host: { kind: HostKind; version: string };
  /** The plugin's settings. Default: none. */
  settings?: Record<string, string>;
  /** Default: the versions of this package. */
  protocolVersions?: string[];
  log?: Logger;
  onMessage?: MessageObserver;
}

export interface StdioOptions extends ConnectOptions {
  /** The plugin command and its arguments. */
  command: string;
  args?: string[];
  env?: NodeJS.ProcessEnv;
  cwd?: string;
  /** The restart backoff. Default: 250 ms, doubled each time, at most 30 s. */
  restart?: { minMs?: number; maxMs?: number } | false;
}

export interface HttpOptions extends ConnectOptions {
  /** For example http://127.0.0.1:7070/rpc */
  url: string;
  token?: string;
}

interface Transport {
  call(method: string, params: unknown, signal?: AbortSignal): Promise<unknown>;
  close(): Promise<void>;
}

const STDERR_LINES = 20;

// ---- stdio ----

class StdioTransport implements Transport {
  private child: ChildProcessWithoutNullStreams | null = null;
  private nextId = 1;
  private pending = new Map<number, { method: string; resolve: (v: unknown) => void; reject: (e: Error) => void }>();
  private stderrTail: string[] = [];
  private stopping = false;
  private backoffMs: number;
  private restartTimer: NodeJS.Timeout | null = null;
  private ready = false;
  /** Runs the handshake after a restart. */
  onRestart: (() => Promise<void>) | null = null;
  exitWaiters: Array<() => void> = [];

  constructor(
    private readonly o: StdioOptions,
    private readonly log: Logger,
  ) {
    this.backoffMs = this.minMs;
  }

  private get minMs(): number {
    return (this.o.restart && this.o.restart.minMs) || 250;
  }

  private get maxMs(): number {
    return (this.o.restart && this.o.restart.maxMs) || 30_000;
  }

  get pid(): number | undefined {
    return this.child?.pid;
  }

  start(): Promise<void> {
    return new Promise((resolve, reject) => {
      const child = spawn(this.o.command, this.o.args ?? [], { env: this.o.env, cwd: this.o.cwd, stdio: ["pipe", "pipe", "pipe"] });
      this.child = child;
      this.stderrTail = [];
      let started = false;
      // The transport is ready for the host's calls only after the handshake (markReady).
      this.ready = false;
      child.once("spawn", () => {
        started = true;
        resolve();
      });
      child.once("error", (err) => {
        if (started) return;
        // A spawn failure emits no exit event: forget the child, so a restart tries again.
        if (this.child === child) this.child = null;
        reject(new PluginError("unavailable", `The plugin ${this.o.command} did not start: ${err.message}`));
      });
      createInterface({ input: child.stdout }).on("line", (line) => this.onLine(line));
      createInterface({ input: child.stderr }).on("line", (line) => {
        this.stderrTail.push(line);
        if (this.stderrTail.length > STDERR_LINES) this.stderrTail.shift();
      });
      child.once("exit", (code, signal) => this.onExit(child, code, signal));
    });
  }

  private onLine(line: string): void {
    if (!line.trim()) return;
    let msg: { id?: unknown; result?: unknown; error?: RpcErrorObject };
    try {
      msg = JSON.parse(line);
    } catch {
      this.log({ event: "plugin_stdout_ignored", reason: "not JSON", length: line.length });
      return;
    }
    if (typeof msg !== "object" || msg === null || typeof msg.id !== "number") {
      this.log({ event: "plugin_stdout_ignored", reason: "no request id" });
      return;
    }
    const p = this.pending.get(msg.id);
    if (!p) return;
    this.pending.delete(msg.id);
    this.o.onMessage?.("received", p.method, line);
    if (msg.error) p.reject(PluginError.fromRpc(msg.error));
    else p.resolve(msg.result);
  }

  private onExit(child: ChildProcessWithoutNullStreams, code: number | null, signal: NodeJS.Signals | null): void {
    if (this.child !== child) return;
    this.ready = false;
    this.child = null;
    const failed = new PluginError("unavailable", "The plugin exited during the call.");
    for (const p of this.pending.values()) p.reject(failed);
    this.pending.clear();
    for (const w of this.exitWaiters.splice(0)) w();
    if (this.stopping) {
      this.log({ event: "plugin_stopped", code, signal });
      return;
    }
    this.log({ event: "plugin_exited", code, signal, stderr: [...this.stderrTail], restartInMs: this.o.restart === false ? null : this.backoffMs });
    if (this.o.restart === false) return;
    const wait = this.backoffMs;
    this.backoffMs = Math.min(this.backoffMs * 2, this.maxMs);
    this.restartTimer = setTimeout(() => void this.restart(), wait);
  }

  private async restart(): Promise<void> {
    this.restartTimer = null;
    if (this.stopping) return;
    try {
      await this.start();
      await this.onRestart?.();
      this.backoffMs = this.minMs;
      this.log({ event: "plugin_restarted", pid: this.child?.pid });
    } catch (err) {
      this.log({ event: "plugin_restart_failed", error: err instanceof Error ? err.message : String(err) });
      // A failed handshake leaves the process: stop it, so onExit starts the next try.
      if (this.child) this.child.kill();
      else if (!this.stopping) {
        const wait = this.backoffMs;
        this.backoffMs = Math.min(this.backoffMs * 2, this.maxMs);
        this.restartTimer = setTimeout(() => void this.restart(), wait);
      }
    }
  }

  /** The handshake ended: the host's calls can go to the plugin. */
  markReady(): void {
    if (this.child) this.ready = true;
  }

  call(method: string, params: unknown, signal?: AbortSignal): Promise<unknown> {
    const child = this.child;
    // Before the handshake ends, only the handshake and describe go to the plugin.
    const early = method === "plugin.handshake" || method === "plugin.describe";
    if (!child || (!this.ready && !early)) {
      return Promise.reject(new PluginError("unavailable", "The plugin does not run now. It restarts; try again later."));
    }
    const id = this.nextId++;
    const line = JSON.stringify({ jsonrpc: "2.0", id, method, params: params ?? {} });
    return new Promise((resolve, reject) => {
      this.pending.set(id, { method, resolve, reject });
      if (signal) {
        const abort = () => {
          if (this.pending.delete(id)) reject(new PluginError("unavailable", `The call to ${method} was cancelled.`));
        };
        if (signal.aborted) return abort();
        signal.addEventListener("abort", abort, { once: true });
      }
      this.o.onMessage?.("sent", method, line);
      child.stdin.write(line + "\n", (err) => {
        if (err && this.pending.delete(id)) reject(new PluginError("unavailable", `The plugin's stdin is closed: ${err.message}`));
      });
    });
  }

  /** Waits for the process to exit, at most ms. True when it exited. */
  waitExit(ms: number): Promise<boolean> {
    if (!this.child) return Promise.resolve(true);
    return new Promise((resolve) => {
      const timer = setTimeout(() => resolve(false), ms);
      this.exitWaiters.push(() => {
        clearTimeout(timer);
        resolve(true);
      });
    });
  }

  markStopping(): void {
    this.stopping = true;
    if (this.restartTimer) clearTimeout(this.restartTimer);
  }

  async close(): Promise<void> {
    this.markStopping();
    const child = this.child;
    if (!child) return;
    child.stdin.end();
    if (!(await this.waitExit(6000))) child.kill("SIGKILL");
  }
}

// ---- HTTP ----

class HttpTransport implements Transport {
  private nextId = 1;
  session: string | null = null;

  constructor(private readonly o: HttpOptions) {}

  async call(method: string, params: unknown, signal?: AbortSignal): Promise<unknown> {
    const body = JSON.stringify({ jsonrpc: "2.0", id: this.nextId++, method, params: params ?? {} });
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this.o.token) headers.Authorization = `Bearer ${this.o.token}`;
    if (this.session) headers["Plugin-Session"] = this.session;
    this.o.onMessage?.("sent", method, body);
    let res: Response;
    try {
      res = await fetch(this.o.url, { method: "POST", headers, body, signal });
    } catch (err) {
      throw new PluginError("unavailable", `The plugin at ${this.o.url} does not answer: ${err instanceof Error ? err.message : String(err)}`);
    }
    const text = await res.text();
    if (res.status === 401) throw new PluginError("refused", `The plugin at ${this.o.url} refused the token (HTTP 401).`);
    if (res.status !== 200) throw new PluginError("unavailable", `The plugin at ${this.o.url} answered HTTP ${res.status}: ${text.slice(0, 200)}`);
    this.o.onMessage?.("received", method, text);
    let msg: { result?: unknown; error?: RpcErrorObject };
    try {
      msg = JSON.parse(text);
    } catch {
      throw new PluginError("unavailable", `The answer of the plugin at ${this.o.url} is not JSON.`);
    }
    if (msg.error) throw PluginError.fromRpc(msg.error);
    return msg.result;
  }

  async close(): Promise<void> {}
}

// ---- the client ----

function toBase64(data: Uint8Array): string {
  return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString("base64");
}

/** The SHA-256 of the data, in lowercase hex. */
export function sha256Hex(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}

/** One connection to a plugin, after the handshake. */
export class PluginClient {
  /** The handshake result. It changes after a restart. */
  info!: HandshakeResult;

  private constructor(
    private readonly t: Transport,
    private readonly hello: HandshakeParams,
    private readonly log: Logger,
  ) {}

  /** Start a plugin as a child process, and do the handshake. */
  static async startStdio(o: StdioOptions): Promise<PluginClient> {
    const log = o.log ?? (() => {});
    const t = new StdioTransport(o, log);
    const client = new PluginClient(t, helloOf(o), log);
    await t.start();
    try {
      await client.handshake();
    } catch (err) {
      await t.close();
      throw err;
    }
    t.onRestart = () => client.handshake();
    return client;
  }

  /** Connect to a plugin over HTTP, and do the handshake. */
  static async connectHttp(o: HttpOptions): Promise<PluginClient> {
    const log = o.log ?? (() => {});
    const t = new HttpTransport(o);
    const client = new PluginClient(t, helloOf(o), log);
    await client.handshake();
    return client;
  }

  /** Call plugin.describe on a new process or connection, before any handshake. */
  static async describeStdio(o: Pick<StdioOptions, "command" | "args" | "env" | "cwd">): Promise<DescribeResult> {
    const t = new StdioTransport({ ...o, host: { kind: "test", version: "" }, restart: false }, () => {});
    await t.start();
    try {
      return (await t.call("plugin.describe", {})) as DescribeResult;
    } finally {
      await t.close();
    }
  }

  /** One new HTTP handshake, shared by every call that lost the session. */
  private rehandshaking: Promise<void> | null = null;

  private rehandshake(): Promise<void> {
    if (!this.rehandshaking) {
      if (this.t instanceof HttpTransport) this.t.session = null;
      this.rehandshaking = this.handshake().finally(() => {
        this.rehandshaking = null;
      });
    }
    return this.rehandshaking;
  }

  private async handshake(): Promise<void> {
    const r = (await this.t.call("plugin.handshake", this.hello)) as HandshakeResult;
    if (this.t instanceof HttpTransport) this.t.session = r.session ?? null;
    if (this.t instanceof StdioTransport) this.t.markReady();
    this.info = r;
    this.log({
      event: "plugin_handshake",
      plugin: r.plugin.name,
      version: r.plugin.version,
      repo: r.plugin.repo,
      protocol: r.protocolVersion,
      capabilities: r.capabilities,
    });
  }

  /** The process ID of a stdio plugin. */
  get pid(): number | undefined {
    return this.t instanceof StdioTransport ? this.t.pid : undefined;
  }

  /** True when the plugin gives the capability. */
  has(c: Capability): boolean {
    return this.info.capabilities.includes(c);
  }

  /**
   * Make any call. Prefer the typed calls. Over HTTP, when the plugin lost
   * the session (it restarted), the client sends the handshake again and
   * retries the call once.
   */
  async call<T>(method: string, params?: unknown, signal?: AbortSignal): Promise<T> {
    try {
      return (await this.t.call(method, params ?? {}, signal)) as T;
    } catch (err) {
      if (!(this.t instanceof HttpTransport) || !(err instanceof PluginError) || err.reason !== "handshake_required") throw err;
      this.log({ event: "plugin_session_lost", method });
      await this.rehandshake();
      return (await this.t.call(method, params ?? {}, signal)) as T;
    }
  }

  describe(): Promise<DescribeResult> {
    return this.call("plugin.describe");
  }

  ping(): Promise<PingResult> {
    return this.call("plugin.ping");
  }

  /** Ask the plugin to stop: it finishes its calls (at most timeoutMs) and exits. */
  async shutdown(timeoutMs = 5000): Promise<void> {
    if (this.t instanceof StdioTransport) this.t.markStopping();
    try {
      await this.call("plugin.shutdown", { timeoutMs });
    } finally {
      if (this.t instanceof StdioTransport && !(await this.t.waitExit(timeoutMs + 1000))) await this.t.close();
    }
  }

  /** Close the connection. A stdio plugin gets the end of its stdin and stops. */
  close(): Promise<void> {
    return this.t.close();
  }

  readonly state = {
    get: (p: StateGetParams): Promise<StateGetResult> => this.call("state.get", p),
    put: (p: StatePutParams): Promise<StatePutResult> => this.call("state.put", p),
    delete: (p: StateDeleteParams): Promise<{ deleted: true }> => this.call("state.delete", p),
    list: (p: StateListParams): Promise<StateListResult> => this.call("state.list", p),
    /** Every document of the list, page by page. */
    listAll: async (p: Omit<StateListParams, "cursor">): Promise<Document[]> => {
      const out: Document[] = [];
      let cursor: string | null = null;
      do {
        const r: StateListResult = await this.state.list({ ...p, cursor });
        out.push(...r.items);
        cursor = r.cursor;
      } while (cursor);
      return out;
    },
  };

  readonly events = {
    append: (stream: string, events: NewEvent[]): Promise<EventsAppendResult> => this.call("events.append", { stream, events }),
    read: (p: EventsReadParams): Promise<EventsReadResult> => this.call("events.read", p),
    /** Every event after p.after, page by page. */
    readAll: async (p: EventsReadParams): Promise<Event[]> => {
      const out: Event[] = [];
      let after = p.after ?? null;
      for (;;) {
        const r = await this.events.read({ ...p, after });
        out.push(...r.events);
        if (!r.more || r.events.length === 0) return out;
        after = r.cursor;
      }
    },
    delete: (stream: string): Promise<{ deleted: boolean }> => this.call("events.delete", { stream }),
  };

  readonly blobs = {
    /** Put a blob in chunks. Returns its SHA-256. */
    put: async (data: Uint8Array): Promise<string> => {
      const sha256 = sha256Hex(data);
      await this.blobs.putAs(sha256, data);
      return sha256;
    },
    /** Put a blob with the hash given. The plugin refuses a wrong hash. */
    putAs: async (sha256: string, data: Uint8Array): Promise<void> => {
      const begin = await this.call<BlobsBeginResult>("blobs.begin", { sha256, size: data.byteLength });
      if (begin.exists) return;
      const chunk = this.info.limits.maxChunkBytes;
      for (let offset = 0; offset < data.byteLength; offset += chunk) {
        await this.call("blobs.write", { uploadId: begin.uploadId, offset, data: toBase64(data.subarray(offset, offset + chunk)) });
      }
      await this.call("blobs.commit", { uploadId: begin.uploadId });
    },
    /** Get a whole blob. A missing blob is a not_found error. */
    get: async (sha256: string): Promise<Buffer> => {
      const parts: Buffer[] = [];
      let offset = 0;
      for (;;) {
        const r = await this.call<BlobsGetResult>("blobs.get", { sha256, offset, length: this.info.limits.maxChunkBytes });
        const part = Buffer.from(r.data, "base64");
        parts.push(part);
        offset += part.length;
        if (offset >= r.size || part.length === 0) return Buffer.concat(parts);
      }
    },
    has: async (sha256s: string[]): Promise<string[]> => (await this.call<{ present: string[] }>("blobs.has", { sha256s })).present,
    delete: async (sha256: string): Promise<boolean> => (await this.call<{ deleted: boolean }>("blobs.delete", { sha256 })).deleted,
  };

  readonly watch = {
    /** Wait for a change to a document or a stream. */
    wait: (p: WatchWaitParams, signal?: AbortSignal): Promise<WatchWaitResult> => this.call("watch.wait", p, signal),
  };

  readonly test = {
    continueStream: (stream: string): Promise<{ run: string }> => this.call("test.continueStream", { stream }),
  };
}

function helloOf(o: ConnectOptions): HandshakeParams {
  return { host: o.host, protocolVersions: o.protocolVersions ?? PROTOCOL_VERSIONS, settings: o.settings ?? {} };
}

export type { Attributes, Json };
export { RPC };
