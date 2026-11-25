// The JSON spec store: one file per spec in <data>/specs, with the same rules
// as the spec workflow (workflows/specRules.ts). The relay process owns the
// files: it keeps every spec in memory, serializes the changes of each spec,
// and writes the file after each change.
import { createHash, randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { readdir } from "node:fs/promises";
import { join } from "node:path";
import type { Logger } from "../../log.js";
import {
  FINISHED_GRACE_MS,
  initialSpecState,
  type Attachment,
  type Message,
  type SpecAddress,
  type SpecMemo,
  type SpecWorkflowState,
} from "../../workflows/types.js";
import {
  MAX_WAIT_MS,
  applyAck,
  applyGone,
  applySend,
  applyState,
  applySync,
  checkSend,
  collectRead,
  hasNewer,
  isDone,
  lastSyncLine,
  matchesFilter,
  readCursor,
  specMemo,
  startsSpec,
  type ListFilter,
  type ReadResult,
  type RuleEnv,
  type StateInput,
  type SyncInput,
  type SyncResult,
} from "../../workflows/specRules.js";
import { SpecClosingError, SpecDroppedError, SpecNotFoundError } from "../errors.js";
import type { ListedSpec, SpecRef, SpecStore } from "../types.js";
import { TEMP_SUFFIX, readText, removeFile, writeAtomic, type WriteHooks } from "./files.js";

/** A closed spec stays in lists with all, and blocks a late line, this long (as the Temporal retention). */
export const CLOSED_KEEP_MS = 30 * 24 * 60 * 60 * 1000;

interface SpecFile {
  version: 1;
  state: SpecWorkflowState;
  /** The time of the last call to the spec (ms since the epoch): the grace period starts here. */
  lastActivityAt: number;
}

/** A spec that the store removed after its grace period. */
interface ClosedSpec {
  repoKey: string;
  slug: string;
  closedAt: number;
  memo: SpecMemo;
}

interface ClosedFile {
  version: 1;
  closed: ClosedSpec[];
}

interface Entry {
  state: SpecWorkflowState;
  lastActivityAt: number;
  /** True while the store removes the spec's data: calls are refused. */
  closing: boolean;
  /** Reads that wait for a new message. The grace period does not end while one waits. */
  reading: number;
  /** The tail of the spec's queue of calls. */
  queue: Promise<unknown>;
  timer: NodeJS.Timeout | null;
}

export interface JsonSpecStoreOptions {
  /** The data folder (RELAY_DATA_DIR). */
  dir: string;
  deployId: string;
  log: Logger;
  /** Remove a spec's attachments from the bucket (the activity of the Temporal store). */
  removeAttachments: (input: { deployId: string; repoKey: string; slug: string }) => Promise<number>;
  /** How long a finished spec stays after its last call (tests set less). */
  graceMs?: number;
  now?: () => Date;
  /** Test hooks for the file writes. */
  writeHooks?: WriteHooks;
}

/** The file name of a spec: a hash of its address, so a slug never makes a path. */
export function specFileName(deployId: string, repoKey: string, slug: string): string {
  return createHash("sha256").update(`${deployId}/${repoKey}/${slug}`, "utf8").digest("hex").slice(0, 32) + ".json";
}

function keyOf(spec: SpecRef): string {
  return `${spec.repoKey}/${spec.slug}`;
}

export class JsonSpecStore implements SpecStore {
  private readonly entries = new Map<string, Entry>();
  private readonly closed = new Map<string, ClosedSpec>();
  private closedQueue: Promise<unknown> = Promise.resolve();
  /** Emits the key of a spec when it gets a new message for Claude AI, or closes. */
  private readonly wake = new EventEmitter();
  private readonly env: RuleEnv;
  private stopped = false;

  constructor(private readonly o: JsonSpecStoreOptions) {
    this.env = { newId: () => randomUUID(), now: () => this.nowDate().toISOString() };
    this.wake.setMaxListeners(0);
  }

  private nowDate(): Date {
    return this.o.now?.() ?? new Date();
  }

  private get specsDir(): string {
    return join(this.o.dir, "specs");
  }

  private get closedPath(): string {
    return join(this.o.dir, "closed.json");
  }

  private pathOf(spec: SpecRef): string {
    return join(this.specsDir, specFileName(this.o.deployId, spec.repoKey, spec.slug));
  }

  private get graceMs(): number {
    return this.o.graceMs ?? FINISHED_GRACE_MS;
  }

  /** Read the spec files. A file that does not parse is logged and left out. */
  async load(): Promise<void> {
    let names: string[] = [];
    try {
      names = await readdir(this.specsDir);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    for (const name of names) {
      const path = join(this.specsDir, name);
      // A crash during a write leaves a temporary file: the old file is whole.
      if (name.endsWith(TEMP_SUFFIX)) {
        await removeFile(path);
        continue;
      }
      if (!name.endsWith(".json")) continue;
      const file = await this.parse(path, name);
      if (!file || file.state.deployId !== this.o.deployId) continue;
      const entry: Entry = {
        state: file.state,
        lastActivityAt: file.lastActivityAt,
        closing: false,
        reading: 0,
        queue: Promise.resolve(),
        timer: null,
      };
      this.entries.set(keyOf(file.state.spec), entry);
      this.schedule(entry);
    }
    const closed = await readText(this.closedPath);
    if (closed) {
      try {
        const parsed = JSON.parse(closed) as ClosedFile;
        const now = this.nowDate().getTime();
        for (const c of parsed.closed ?? []) if (now - c.closedAt < CLOSED_KEEP_MS) this.closed.set(keyOf(c), c);
      } catch {
        this.o.log({ event: "closed_file_unreadable" });
      }
    }
  }

  private async parse(path: string, name: string): Promise<SpecFile | null> {
    try {
      const file = JSON.parse((await readText(path)) ?? "") as SpecFile;
      if (file?.version !== 1 || !file.state?.spec?.repoKey || !file.state.spec.slug) throw new Error("not a spec file");
      return file;
    } catch {
      this.o.log({ event: "spec_file_unreadable", file: name });
      return null;
    }
  }

  /** Stop the timers. The files stay. */
  close(): void {
    this.stopped = true;
    for (const entry of this.entries.values()) {
      if (entry.timer) clearTimeout(entry.timer);
      entry.timer = null;
    }
  }

  /** Run fn in the spec's queue, so the calls of one spec never interleave. */
  private queued<T>(entry: Entry, fn: () => Promise<T>): Promise<T> {
    const run = entry.queue.then(fn, fn);
    entry.queue = run.catch(() => undefined);
    return run;
  }

  private async write(entry: Entry): Promise<void> {
    const file: SpecFile = { version: 1, state: entry.state, lastActivityAt: entry.lastActivityAt };
    await writeAtomic(this.pathOf(entry.state.spec), JSON.stringify(file), this.o.writeHooks);
  }

  /**
   * Apply a change to a spec: refuse it while the spec closes, count the call
   * (the grace period starts again), write the file when the rule changed the
   * state, and wake the reads when a message for Claude AI came.
   */
  private async change<T>(entry: Entry, fn: (s: SpecWorkflowState) => { result: T; changed: boolean }): Promise<T> {
    return this.queued(entry, async () => {
      if (entry.closing) throw new SpecClosingError();
      // A failed write leaves the state as it was: change a copy, then keep it.
      const draft = structuredClone(entry.state);
      const before = draft.coordinator.nextSeq;
      const out = fn(draft);
      entry.lastActivityAt = this.nowDate().getTime();
      if (out.changed) {
        const previous = entry.state;
        entry.state = draft;
        try {
          await this.write(entry);
        } catch (error) {
          entry.state = previous;
          throw error;
        }
      }
      if (entry.state.coordinator.nextSeq !== before) this.wake.emit(keyOf(entry.state.spec));
      this.schedule(entry);
      return out.result;
    });
  }

  /** Set the timer of a finished spec with empty inboxes: it closes after the grace period. */
  private schedule(entry: Entry): void {
    if (entry.timer) clearTimeout(entry.timer);
    entry.timer = null;
    if (this.stopped || entry.closing || !isDone(entry.state)) return;
    const wait = Math.max(0, entry.lastActivityAt + this.graceMs - this.nowDate().getTime());
    entry.timer = setTimeout(() => void this.closeSpec(entry), wait);
    entry.timer.unref?.();
  }

  /** The end of a finished spec: remove its attachments and its file, keep a closed record. */
  private async closeSpec(entry: Entry): Promise<void> {
    entry.timer = null;
    await this.queued(entry, async () => {
      if (this.stopped || entry.closing || !isDone(entry.state)) return;
      const idleMs = this.nowDate().getTime() - entry.lastActivityAt;
      if (entry.reading > 0 || idleMs < this.graceMs) {
        // A call came in the period, or a read still waits: look again later.
        const wait = entry.reading > 0 ? MAX_WAIT_MS : this.graceMs - idleMs;
        if (entry.timer) clearTimeout(entry.timer);
        entry.timer = setTimeout(() => void this.closeSpec(entry), wait);
        entry.timer.unref?.();
        return;
      }
      // From here every call is refused, so nothing that the relay accepted is lost.
      entry.closing = true;
      const { repoKey, slug } = entry.state.spec;
      this.wake.emit(keyOf(entry.state.spec));
      try {
        await this.removeAttachments(repoKey, slug);
        await removeFile(this.pathOf(entry.state.spec));
        await this.remember({ repoKey, slug, closedAt: this.nowDate().getTime(), memo: specMemo(entry.state) });
        this.entries.delete(keyOf(entry.state.spec));
        this.o.log({ event: "spec_closed" });
      } catch {
        // Try again later; the spec takes calls again meanwhile.
        entry.closing = false;
        this.o.log({ event: "spec_close_failed" });
        if (!this.stopped) {
          entry.timer = setTimeout(() => void this.closeSpec(entry), 60_000);
          entry.timer.unref?.();
        }
      }
    });
  }

  private async removeAttachments(repoKey: string, slug: string): Promise<void> {
    let lastError: unknown;
    for (let tries = 0; tries < 3; tries++) {
      try {
        await this.o.removeAttachments({ deployId: this.o.deployId, repoKey, slug });
        return;
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError;
  }

  /** Keep a closed record (for lists with all, and for late lines); drop old ones. */
  private remember(c: ClosedSpec): Promise<void> {
    const run = this.closedQueue.then(async () => {
      const now = this.nowDate().getTime();
      this.closed.set(keyOf(c), c);
      for (const [key, old] of this.closed) if (now - old.closedAt >= CLOSED_KEEP_MS) this.closed.delete(key);
      const file: ClosedFile = { version: 1, closed: [...this.closed.values()] };
      await writeAtomic(this.closedPath, JSON.stringify(file), this.o.writeHooks);
    });
    this.closedQueue = run.catch(() => undefined);
    return run;
  }

  private newEntry(spec: SpecAddress): Entry {
    const entry: Entry = {
      state: initialSpecState(this.o.deployId, { repoKey: spec.repoKey, slug: spec.slug, ...(spec.repo ? { repo: spec.repo } : {}) }, undefined, this.graceMs),
      lastActivityAt: this.nowDate().getTime(),
      closing: false,
      reading: 0,
      queue: Promise.resolve(),
      timer: null,
    };
    this.entries.set(keyOf(spec), entry);
    return entry;
  }

  async sync(spec: SpecAddress, input: Omit<SyncInput, "at">): Promise<SyncResult> {
    const key = keyOf(spec);
    let entry = this.entries.get(key);
    const created = !entry;
    if (!entry) {
      // A spec that closed takes no late line: only a first sync starts it again
      // (as a completed workflow).
      if (this.closed.has(key) && !startsSpec(input)) return { line: lastSyncLine(input), applied: false };
      entry = this.newEntry(spec);
    }
    const sync: SyncInput = { ...input, repo: input.repo ?? spec.repo, at: this.env.now() };
    try {
      return await this.change(entry, (s) => applySync(s, sync, this.env));
    } catch (error) {
      // A new spec whose first write failed is not kept.
      if (created && this.entries.get(key) === entry) this.entries.delete(key);
      if (error instanceof SpecClosingError) {
        // The first sync of a new spec with the same slug: the agent sends the
        // batch again, and then it starts a new spec.
        if (startsSpec(input)) throw error;
        return { line: lastSyncLine(input), applied: false };
      }
      throw error;
    }
  }

  async state(spec: SpecRef): Promise<SpecWorkflowState | null> {
    const entry = this.entries.get(keyOf(spec));
    return entry ? structuredClone(entry.state) : null;
  }

  private entryOf(spec: SpecRef): Entry {
    const entry = this.entries.get(keyOf(spec));
    if (!entry) throw new SpecNotFoundError();
    return entry;
  }

  async refresh(spec: SpecRef, input: Omit<StateInput, "at">): Promise<boolean> {
    const entry = this.entries.get(keyOf(spec));
    if (!entry) return false;
    try {
      const result = await this.change(entry, (s) => applyState(s, { ...input, at: this.env.now() }, () => true));
      return result.applied;
    } catch (error) {
      if (error instanceof SpecClosingError) return false;
      throw error;
    }
  }

  async gone(spec: SpecRef): Promise<void> {
    const entry = this.entries.get(keyOf(spec));
    if (!entry) return;
    try {
      await this.change(entry, (s) => applyGone(s, { at: this.env.now() }, this.env));
    } catch (error) {
      if (error instanceof SpecClosingError) return;
      throw error;
    }
  }

  async send(
    spec: SpecRef,
    input: { text: string; replyTo?: string; attachments?: Attachment[] },
  ): Promise<{ seq: number; message: Message }> {
    const entry = this.entryOf(spec);
    return this.change(entry, (s) => {
      try {
        checkSend(s);
      } catch {
        throw new SpecDroppedError();
      }
      return { result: applySend(s, { ...input, id: randomUUID(), at: this.env.now() }), changed: true };
    });
  }

  async ack(spec: SpecRef, seq: number, error?: string): Promise<{ removed: boolean }> {
    const entry = this.entryOf(spec);
    return this.change(entry, (s) => applyAck(s, { seq, ...(error ? { error } : {}) }, this.env));
  }

  async read(spec: SpecRef, after: number | undefined, waitMs: number, unreadOnly = false): Promise<ReadResult> {
    const entry = this.entryOf(spec);
    const key = keyOf(spec);
    entry.reading++;
    try {
      // The cursor and the first look, in the spec's queue.
      const cursor = await this.queued(entry, async () => {
        if (entry.closing) throw new SpecClosingError();
        entry.lastActivityAt = this.nowDate().getTime();
        return readCursor(entry.state, { after, unreadOnly });
      });
      if (!hasNewer(entry.state, cursor) && waitMs > 0 && !entry.closing) {
        // Wait outside the queue, so syncs and sends go on meanwhile.
        await new Promise<void>((resolve) => {
          const done = () => {
            clearTimeout(timer);
            this.wake.off(key, onWake);
            resolve();
          };
          const onWake = () => {
            if (hasNewer(entry.state, cursor) || entry.closing) done();
          };
          const timer = setTimeout(done, Math.min(waitMs, MAX_WAIT_MS));
          this.wake.on(key, onWake);
        });
      }
      return await this.change(entry, (s) => collectRead(s, cursor));
    } finally {
      entry.reading--;
    }
  }

  async list(filter: ListFilter = {}): Promise<ListedSpec[]> {
    const out: ListedSpec[] = [];
    const seen = new Set<string>();
    for (const [key, entry] of this.entries) {
      const memo = specMemo(entry.state);
      if (!matchesFilter(entry.state.spec.repoKey, memo, filter)) continue;
      seen.add(key);
      out.push({ repoKey: entry.state.spec.repoKey, slug: entry.state.spec.slug, running: true, memo });
    }
    if (filter.all) {
      // The closed specs, as the closed runs of Temporal: a newer open spec wins.
      for (const [key, c] of this.closed) {
        if (seen.has(key) || !matchesFilter(c.repoKey, c.memo, filter)) continue;
        out.push({ repoKey: c.repoKey, slug: c.slug, running: false, memo: c.memo });
      }
    }
    return out;
  }
}
