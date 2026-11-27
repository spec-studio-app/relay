// The plugin spec store: one document per spec in `specs`, a closed record
// in `closed-specs`, and the spec's events in the stream spec:<repoKey>:<slug>
// (PROTOCOL.md section 14). The rules are the ones of workflows/specRules.ts,
// as in the JSON store. The grace period of a finished spec is a timer in
// this process; the time of the last call is in the document, so a restart
// keeps it.
import { randomUUID } from "node:crypto";
import type { Logger } from "../../log.js";
import type { Attributes, NewEvent } from "../../vendor/plugin-sdk/index.js";
import { FINISHED_GRACE_MS, initialSpecState, type Attachment, type Message, type SpecAddress, type SpecMemo, type SpecWorkflowState } from "../../workflows/types.js";
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
  syncLines,
  type ListFilter,
  type ReadResult,
  type RuleEnv,
  type StateInput,
  type SyncInput,
  type SyncResult,
} from "../../workflows/specRules.js";
import { SpecClosingError, SpecDroppedError, SpecNotFoundError } from "../errors.js";
import { CLOSED_KEEP_MS } from "../json/jsonSpecStore.js";
import type { ListedSpec, SpecRef, SpecStore } from "../types.js";
import { KeyQueue, guard, waitForChange, type Doc, type Docs } from "./docs.js";

export const SPECS = "specs";
export const CLOSED_SPECS = "closed-specs";

interface SpecDoc {
  version: 1;
  state: SpecWorkflowState;
  /** The time of the last call to the spec (ms since the epoch): the grace period starts here. */
  lastActivityAt: number;
}

interface ClosedDoc {
  version: 1;
  repoKey: string;
  slug: string;
  closedAt: number;
  memo: SpecMemo;
}

export interface PluginSpecStoreOptions {
  deployId: string;
  log: Logger;
  /** Remove a spec's attachments from the bucket. */
  removeAttachments: (input: { deployId: string; repoKey: string; slug: string }) => Promise<number>;
  /** How long a finished spec stays after its last call (tests set less). */
  graceMs?: number;
  now?: () => Date;
}

/** The key of a spec's documents. */
export function specKey(spec: SpecRef): string {
  return `${spec.repoKey}:${spec.slug}`;
}

/** The stream of a spec's events. */
export function specStream(spec: SpecRef): string {
  return `spec:${spec.repoKey}:${spec.slug}`;
}

/** The attributes of a spec document: `state.list` filters on them. */
function attributesOf(s: SpecWorkflowState): Attributes {
  const memo = specMemo(s);
  return {
    repoKey: s.spec.repoKey,
    deployId: s.deployId,
    open: memo.stage !== "merged",
    pendingForMac: memo.waitingForMac > 0,
    pendingForCoordinator: memo.unreadForCoordinator > 0,
    done: isDone(s),
  };
}

export class PluginSpecStore implements SpecStore {
  private readonly queue = new KeyQueue();
  /** The specs that this process removes now: calls are refused. */
  private readonly closing = new Set<string>();
  /** Reads that wait, by key. The grace period does not end while one waits. */
  private readonly reading = new Map<string, number>();
  private readonly timers = new Map<string, NodeJS.Timeout>();
  private readonly stop = new AbortController();
  private readonly env: RuleEnv;
  /** False for the relay commands: no grace timers. */
  private timersOn = false;

  constructor(
    private readonly docs: Docs,
    private readonly o: PluginSpecStoreOptions,
  ) {
    this.env = { newId: () => randomUUID(), now: () => this.nowDate().toISOString() };
  }

  private nowDate(): Date {
    return this.o.now?.() ?? new Date();
  }

  private get graceMs(): number {
    return this.o.graceMs ?? FINISHED_GRACE_MS;
  }

  /** Start the grace timers of the finished specs, and drop the old closed records. */
  async startTimers(): Promise<void> {
    this.timersOn = true;
    const done = await this.docs.list<SpecDoc>(SPECS, { deployId: this.o.deployId, done: true });
    for (const item of done) this.schedule(item.value.state, item.value.lastActivityAt);
    const now = this.nowDate().getTime();
    for (const item of await this.docs.list<ClosedDoc>(CLOSED_SPECS, { deployId: this.o.deployId })) {
      if (now - item.value.closedAt >= CLOSED_KEEP_MS) await this.docs.remove(CLOSED_SPECS, item.key, item.version);
    }
  }

  /** Stop the timers and end the waits. The data stays. */
  close(): void {
    this.timersOn = false;
    this.stop.abort();
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
  }

  /**
   * Apply a change to a spec that exists: refuse it while the spec closes,
   * write when the rule changed the state (or for a read, the time of the
   * read), and set the grace timer again.
   */
  private change<T>(spec: SpecRef, fn: (s: SpecWorkflowState) => { result: T; changed: boolean }, read = false): Promise<T> {
    const key = specKey(spec);
    return this.queue.run(key, async () => {
      if (this.closing.has(key)) throw new SpecClosingError();
      let after: SpecDoc | null = null;
      const result = await this.docs.update<SpecDoc, T>(SPECS, key, (doc) => {
        if (!doc || doc.value.state.deployId !== this.o.deployId) throw new SpecNotFoundError();
        const state = structuredClone(doc.value.state);
        const out = fn(state);
        if (!out.changed && !read) {
          after = doc.value;
          return { result: out.result };
        }
        after = { version: 1, state, lastActivityAt: this.nowDate().getTime() };
        return { write: { value: after, attributes: attributesOf(state) }, result: out.result };
      });
      const kept = after as SpecDoc | null;
      if (kept) this.schedule(kept.state, kept.lastActivityAt);
      return result;
    });
  }

  private async append(spec: SpecRef, events: NewEvent[]): Promise<void> {
    if (events.length === 0 || !this.docs.client.has("events")) return;
    // The stream is the history; the document is the state. A failed append is logged, not thrown.
    await guard(() => this.docs.client.events.append(specStream(spec), events)).catch(() => this.o.log({ event: "spec_events_append_failed" }));
  }

  /** Set the timer of a finished spec with empty inboxes: it closes after the grace period. */
  private schedule(s: SpecWorkflowState, lastActivityAt: number): void {
    const key = specKey(s.spec);
    const old = this.timers.get(key);
    if (old) clearTimeout(old);
    this.timers.delete(key);
    if (!this.timersOn || this.closing.has(key) || !isDone(s)) return;
    const wait = Math.max(0, lastActivityAt + this.graceMs - this.nowDate().getTime());
    this.later(key, s.spec, wait);
  }

  private later(key: string, spec: SpecRef, ms: number): void {
    if (!this.timersOn) return;
    const timer = setTimeout(() => {
      this.timers.delete(key);
      void this.closeSpec({ repoKey: spec.repoKey, slug: spec.slug });
    }, ms);
    timer.unref?.();
    this.timers.set(key, timer);
  }

  /** The end of a finished spec: remove its attachments and its document, keep a closed record. */
  private async closeSpec(spec: SpecRef): Promise<void> {
    const key = specKey(spec);
    await this.queue.run(key, async () => {
      if (!this.timersOn || this.closing.has(key)) return;
      let doc: Doc<SpecDoc> | null;
      try {
        doc = await this.readDoc(spec);
      } catch {
        this.later(key, spec, 60_000);
        return;
      }
      if (!doc || !isDone(doc.value.state)) return;
      const idleMs = this.nowDate().getTime() - doc.value.lastActivityAt;
      const reading = this.reading.get(key) ?? 0;
      if (reading > 0 || idleMs < this.graceMs) {
        // A call came in the period, or a read still waits: look again later.
        this.later(key, spec, reading > 0 ? MAX_WAIT_MS : this.graceMs - idleMs);
        return;
      }
      // From here every call of this process is refused, so nothing that the relay accepted is lost.
      this.closing.add(key);
      try {
        await this.removeAttachments(spec);
        const closed: ClosedDoc = { version: 1, repoKey: spec.repoKey, slug: spec.slug, closedAt: this.nowDate().getTime(), memo: specMemo(doc.value.state) };
        await this.docs.update<ClosedDoc, void>(CLOSED_SPECS, key, () => ({
          write: { value: closed, attributes: { repoKey: spec.repoKey, deployId: this.o.deployId, closedAt: closed.closedAt } },
          result: undefined,
        }));
        await this.docs.remove(SPECS, key, doc.version);
        if (this.docs.client.has("events")) await guard(() => this.docs.client.events.delete(specStream(spec)));
        this.o.log({ event: "spec_closed" });
      } catch {
        // Try again later; the spec takes calls again meanwhile.
        this.o.log({ event: "spec_close_failed" });
        this.later(key, spec, 60_000);
      } finally {
        this.closing.delete(key);
      }
    });
  }

  private async removeAttachments(spec: SpecRef): Promise<void> {
    let lastError: unknown;
    for (let tries = 0; tries < 3; tries++) {
      try {
        await this.o.removeAttachments({ deployId: this.o.deployId, repoKey: spec.repoKey, slug: spec.slug });
        return;
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError;
  }

  async sync(spec: SpecAddress, input: Omit<SyncInput, "at">): Promise<SyncResult> {
    const key = specKey(spec);
    return this.queue.run(key, async () => {
      if (this.closing.has(key)) {
        // The first sync of a new spec with the same slug: the agent sends the batch again later.
        if (startsSpec(input)) throw new SpecClosingError();
        return { line: lastSyncLine(input), applied: false };
      }
      const sync: SyncInput = { ...input, repo: input.repo ?? spec.repo, at: this.env.now() };
      let closed: Doc<ClosedDoc> | null = null;
      if (!(await this.readDoc(spec))) {
        // A spec that closed takes no late line: only a first sync starts it again.
        closed = await this.docs.get<ClosedDoc>(CLOSED_SPECS, key);
        if (closed && !startsSpec(input)) return { line: lastSyncLine(input), applied: false };
      }
      let after: SpecDoc | null = null;
      const result = await this.docs.update<SpecDoc, SyncResult>(SPECS, key, (doc) => {
        const mine = doc && doc.value.state.deployId === this.o.deployId ? doc : null;
        const state = mine
          ? structuredClone(mine.value.state)
          : initialSpecState(this.o.deployId, { repoKey: spec.repoKey, slug: spec.slug, ...(spec.repo ? { repo: spec.repo } : {}) }, undefined, this.graceMs);
        const out = applySync(state, sync, this.env);
        if (!out.changed && mine) {
          after = mine.value;
          return { result: out.result };
        }
        after = { version: 1, state, lastActivityAt: this.nowDate().getTime() };
        return { write: { value: after, attributes: attributesOf(state) }, result: out.result };
      });
      if (closed) await this.docs.remove(CLOSED_SPECS, key).catch(() => this.o.log({ event: "closed_record_remove_failed" }));
      const kept = after as SpecDoc | null;
      if (kept) this.schedule(kept.state, kept.lastActivityAt);
      if (result.applied) await this.append(spec, [{ type: "sync", data: { lines: syncLines(sync) } as never }]);
      return result;
    });
  }

  async state(spec: SpecRef): Promise<SpecWorkflowState | null> {
    return (await this.readDoc(spec))?.value.state ?? null;
  }

  async refresh(spec: SpecRef, input: Omit<StateInput, "at">): Promise<boolean> {
    try {
      const result = await this.change(spec, (s) => applyState(s, { ...input, at: this.env.now() }, () => true));
      return result.applied;
    } catch (error) {
      if (error instanceof SpecClosingError || error instanceof SpecNotFoundError) return false;
      throw error;
    }
  }

  async gone(spec: SpecRef): Promise<void> {
    try {
      await this.change(spec, (s) => applyGone(s, { at: this.env.now() }, this.env));
    } catch (error) {
      if (error instanceof SpecClosingError || error instanceof SpecNotFoundError) return;
      throw error;
    }
  }

  async send(spec: SpecRef, input: { text: string; replyTo?: string; attachments?: Attachment[] }): Promise<{ seq: number; message: Message }> {
    const id = randomUUID();
    const sent = await this.change(spec, (s) => {
      try {
        checkSend(s);
      } catch {
        throw new SpecDroppedError();
      }
      return { result: applySend(s, { ...input, id, at: this.env.now() }), changed: true };
    });
    await this.append(spec, [{ type: "message", data: sent.message as never }]);
    return sent;
  }

  ack(spec: SpecRef, seq: number, error?: string): Promise<{ removed: boolean }> {
    return this.change(spec, (s) => applyAck(s, { seq, ...(error ? { error } : {}) }, this.env));
  }

  async read(spec: SpecRef, after: number | undefined, waitMs: number, unreadOnly = false): Promise<ReadResult> {
    const key = specKey(spec);
    if (this.closing.has(key)) throw new SpecClosingError();
    const first = await this.readDoc(spec);
    if (!first) throw new SpecNotFoundError();
    this.reading.set(key, (this.reading.get(key) ?? 0) + 1);
    try {
      const cursor = readCursor(first.value.state, { after, unreadOnly });
      const deadline = Date.now() + Math.min(waitMs, MAX_WAIT_MS);
      let doc: Doc<SpecDoc> = first;
      // Wait for a write to the spec; after each write with no new message, wait again with the new version.
      while (!hasNewer(doc.value.state, cursor) && !this.closing.has(key)) {
        const left = deadline - Date.now();
        if (left <= 0 || this.stop.signal.aborted) break;
        await waitForChange(this.docs, { collection: SPECS, key, version: doc.version }, left, this.stop.signal);
        const next = await this.readDoc(spec);
        // A deleted document: the spec closed.
        if (!next) throw new SpecClosingError();
        doc = next;
      }
      return await this.change(spec, (s) => collectRead(s, cursor), true);
    } finally {
      const n = (this.reading.get(key) ?? 1) - 1;
      if (n > 0) this.reading.set(key, n);
      else this.reading.delete(key);
    }
  }

  /** This deploy's spec, or null. A spec of another deploy on the same storage counts as none. */
  private async readDoc(spec: SpecRef): Promise<Doc<SpecDoc> | null> {
    const doc = await this.docs.get<SpecDoc>(SPECS, specKey(spec));
    return doc && doc.value.state.deployId === this.o.deployId ? doc : null;
  }

  async list(filter: ListFilter = {}): Promise<ListedSpec[]> {
    const where: Attributes = { deployId: this.o.deployId };
    if (filter.repoKey) where.repoKey = filter.repoKey;
    if (filter.openOnly) where.open = true;
    if (filter.pendingForMac) where.pendingForMac = true;
    if (filter.pendingForCoordinator) where.pendingForCoordinator = true;
    const out: ListedSpec[] = [];
    const seen = new Set<string>();
    for (const item of await this.docs.list<SpecDoc>(SPECS, where)) {
      const s = item.value.state;
      const memo = specMemo(s);
      if (s.deployId !== this.o.deployId || !matchesFilter(s.spec.repoKey, memo, filter)) continue;
      seen.add(item.key);
      out.push({ repoKey: s.spec.repoKey, slug: s.spec.slug, running: true, memo });
    }
    if (filter.all) {
      // The closed specs, as the closed runs of Temporal: a newer open spec wins.
      const now = this.nowDate().getTime();
      const closedWhere: Attributes = { deployId: this.o.deployId, ...(filter.repoKey ? { repoKey: filter.repoKey } : {}) };
      for (const item of await this.docs.list<ClosedDoc>(CLOSED_SPECS, closedWhere)) {
        const c = item.value;
        if (seen.has(item.key) || now - c.closedAt >= CLOSED_KEEP_MS || !matchesFilter(c.repoKey, c.memo, filter)) continue;
        out.push({ repoKey: c.repoKey, slug: c.slug, running: false, memo: c.memo });
      }
    }
    return out;
  }
}
