// The plugin spec store: one document per spec in `specs`, a closed record
// in `closed-specs`, and the spec's events in the stream spec:<repoKey>:<slug>
// (PROTOCOL.md section 14). The rules are the ones of workflows/specRules.ts,
// as in the JSON store. The grace period of a finished spec is a timer in
// this process; the time of the last call is in the document, so a restart
// keeps it.
//
// When the Mac's store is the same store (the same storeId), the Mac sends
// sync frames with "linesInStore" (syncFromStore): the relay reads the Mac's
// state from `app-specs` and the Mac's `activity` events from the spec's
// stream after its cursor (`app` in the spec document). It does not append
// them to the stream again.
import { isDeepStrictEqual } from "node:util";
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
  type SyncLine,
  type SyncResult,
} from "../../workflows/specRules.js";
import { SpecClosingError, SpecDroppedError, SpecNotFoundError } from "../errors.js";
import { CLOSED_KEEP_MS } from "../json/jsonSpecStore.js";
import type { ListedSpec, SpecRef, SpecStore, StoreSyncInput } from "../types.js";
import { KeyQueue, MAX_TRIES, OTHER_DEPLOY_TEXT, guard, waitForChange, type Doc, type Docs } from "./docs.js";
import { StoreRefusedError, StoreUnavailableError } from "../errors.js";

export const SPECS = "specs";
export const CLOSED_SPECS = "closed-specs";
/** The Mac's spec documents, when the Mac uses the same store (contract 4). */
export const APP_SPECS = "app-specs";
/** The event type of the Mac's activity lines in a spec's stream (contract 3). */
export const ACTIVITY_EVENT = "activity";

/** The types of the events that the relay appends to a spec's stream. Any other type is another writer's (the Mac's activity). */
export const RELAY_EVENT_TYPES: ReadonlySet<string> = new Set(["sync", "message"]);

/** The text when a linesInStore sync comes to a store that cannot read the Mac's lines. */
export const NO_LINES_IN_STORE_TEXT =
  "The relay's plugin has no events, so the relay cannot read the Mac's lines from the store. Send the sync frame with its lines.";

/** The spec document changed after the read of the cursor: read the stream again. */
class CursorMoved extends Error {}

interface SpecDoc {
  version: 1;
  state: SpecWorkflowState;
  /** The time of the last call to the spec (ms since the epoch): the grace period starts here. */
  lastActivityAt: number;
  /**
   * The Mac's lines in the stream (linesInStore): the cursor of the last
   * event that the relay read, and the number of activity events up to it.
   * The stream then holds the Mac's history, so the close keeps it.
   */
  app?: AppCursor;
}

interface AppCursor {
  cursor: string | null;
  line: number;
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
    await this.pruneClosed();
  }

  /** Remove the closed records older than CLOSED_KEEP_MS: at start and after each close. */
  private async pruneClosed(): Promise<void> {
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
        if (!doc) throw new SpecNotFoundError();
        if (doc.value.state.deployId !== this.o.deployId) throw new StoreRefusedError(OTHER_DEPLOY_TEXT);
        const state = structuredClone(doc.value.state);
        const out = fn(state);
        if (!out.changed && !read) {
          after = doc.value;
          return { result: out.result };
        }
        after = { version: 1, state, lastActivityAt: this.nowDate().getTime(), ...(doc.value.app ? { app: doc.value.app } : {}) };
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
        // On a store that the Mac shares, the stream is the Mac's history too
        // (its activity events): it stays. A stream with only the relay's own
        // events (a separate store) goes with the spec.
        if (this.docs.client.has("events") && !doc.value.app && (await this.onlyRelayEvents(spec))) {
          await guard(() => this.docs.client.events.delete(specStream(spec)));
        }
        this.o.log({ event: "spec_closed" });
        await this.pruneClosed().catch(() => this.o.log({ event: "closed_prune_failed" }));
      } catch {
        // Try again later; the spec takes calls again meanwhile.
        this.o.log({ event: "spec_close_failed" });
        this.later(key, spec, 60_000);
      } finally {
        this.closing.delete(key);
      }
    });
  }

  /**
   * True when the spec's stream holds only the relay's own events. An event
   * of another type (the Mac's `activity`) or a failed read gives false: the
   * close then keeps the stream.
   */
  private async onlyRelayEvents(spec: SpecRef): Promise<boolean> {
    const limit = this.docs.client.info.limits.maxPageSize;
    let after: string | null = null;
    try {
      for (;;) {
        const page = await this.docs.client.events.read({ stream: specStream(spec), after, limit });
        if (page.events.some((e) => !RELAY_EVENT_TYPES.has(e.type))) return false;
        if (!page.more || page.events.length === 0) return true;
        after = page.cursor;
      }
    } catch {
      this.o.log({ event: "spec_stream_check_failed" });
      return false;
    }
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
        // A closed record of another deploy, or one past its time, does not count.
        if (closed && (closed.attributes.deployId !== this.o.deployId || this.nowDate().getTime() - closed.value.closedAt >= CLOSED_KEEP_MS)) closed = null;
        if (closed && !startsSpec(input)) return { line: lastSyncLine(input), applied: false };
      }
      let after: SpecDoc | null = null;
      const events = [{ type: "sync", data: { lines: syncLines(sync) } as never }];
      this.docs.checkEvents(events);
      const result = await this.docs.update<SpecDoc, SyncResult>(SPECS, key, (doc) => {
        if (doc && doc.value.state.deployId !== this.o.deployId) throw new StoreRefusedError(OTHER_DEPLOY_TEXT);
        const mine = doc;
        const state = mine
          ? structuredClone(mine.value.state)
          : initialSpecState(this.o.deployId, { repoKey: spec.repoKey, slug: spec.slug, ...(spec.repo ? { repo: spec.repo } : {}) }, undefined, this.graceMs);
        const out = applySync(state, sync, this.env);
        if (!out.changed && mine) {
          after = mine.value;
          return { result: out.result };
        }
        after = { version: 1, state, lastActivityAt: this.nowDate().getTime(), ...(mine?.value.app ? { app: mine.value.app } : {}) };
        return { write: { value: after, attributes: attributesOf(state) }, result: out.result };
      });
      if (closed) await this.docs.remove(CLOSED_SPECS, key).catch(() => this.o.log({ event: "closed_record_remove_failed" }));
      const kept = after as SpecDoc | null;
      if (kept) this.schedule(kept.state, kept.lastActivityAt);
      if (result.applied) await this.append(spec, events);
      return result;
    });
  }

  /**
   * A sync frame with "linesInStore" (contract 4): read the Mac's state from
   * `app-specs` and the Mac's activity events after the spec's cursor, apply
   * them with applySync, and keep the new cursor in the same write. The
   * events are in the stream already, so nothing is appended. A first sync
   * reads the stream from its start; applySync skips the lines that the spec
   * has.
   */
  async syncFromStore(spec: SpecAddress, input: StoreSyncInput): Promise<SyncResult> {
    const key = specKey(spec);
    return this.queue.run(key, async () => {
      if (!this.docs.client.has("events")) throw new StoreRefusedError(NO_LINES_IN_STORE_TEXT);
      const first = input.first === true;
      if (this.closing.has(key)) {
        if (first) throw new SpecClosingError();
        return { line: 0, applied: false };
      }
      let doc = await this.readDoc(spec);
      let closed: Doc<ClosedDoc> | null = null;
      if (!doc) {
        // A spec that closed takes no late line: only a first sync starts it again.
        closed = await this.docs.get<ClosedDoc>(CLOSED_SPECS, key);
        if (closed && (closed.attributes.deployId !== this.o.deployId || this.nowDate().getTime() - closed.value.closedAt >= CLOSED_KEEP_MS)) closed = null;
        if (closed && !first) return { line: 0, applied: false };
      }
      for (let tries = 0; tries < MAX_TRIES; tries++) {
        if (tries > 0) doc = await this.readDoc(spec);
        const from: AppCursor = first || !doc?.value.app ? { cursor: null, line: 0 } : doc.value.app;
        const read = await this.readAppLines(spec, from);
        const appDoc = await this.docs.get<Record<string, unknown>>(APP_SPECS, key);
        const appState = appDoc && typeof appDoc.value === "object" && appDoc.value !== null && !Array.isArray(appDoc.value) ? appDoc.value : null;
        let after: SpecDoc | null = null;
        try {
          const result = await this.docs.update<SpecDoc, SyncResult>(SPECS, key, (cur) => {
            if (cur && cur.value.state.deployId !== this.o.deployId) throw new StoreRefusedError(OTHER_DEPLOY_TEXT);
            // Another write moved the cursor after this read: read the stream again.
            if (!first && !isDeepStrictEqual(cur?.value.app ?? null, doc?.value.app ?? null)) throw new CursorMoved();
            const state = cur
              ? structuredClone(cur.value.state)
              : initialSpecState(this.o.deployId, { repoKey: spec.repoKey, slug: spec.slug, ...(spec.repo ? { repo: spec.repo } : {}) }, undefined, this.graceMs);
            // The state and the turn go in only when they changed, so a sync with nothing new changes nothing.
            const stateChanged = appState !== null && (first || !isDeepStrictEqual(appState, state.state));
            const turnChanged = input.turn !== undefined && !isDeepStrictEqual(input.turn, state.turn);
            const sync: SyncInput = {
              lines: read.lines,
              ...(first ? { first: true } : {}),
              ...(stateChanged || turnChanged ? { state: appState ?? state.state } : {}),
              ...(input.turn !== undefined ? { turn: input.turn } : {}),
              ...(spec.repo ? { repo: spec.repo } : {}),
              at: this.env.now(),
            };
            const out = applySync(state, sync, this.env);
            const moved = !isDeepStrictEqual(read.to, cur?.value.app ?? null);
            if (!out.changed && (!cur || !moved)) {
              after = cur?.value ?? null;
              return { result: out.result };
            }
            const lastActivityAt = out.changed || !cur ? this.nowDate().getTime() : cur.value.lastActivityAt;
            after = { version: 1, state, lastActivityAt, app: read.to };
            return { write: { value: after, attributes: attributesOf(state) }, result: out.result };
          });
          if (closed) await this.docs.remove(CLOSED_SPECS, key).catch(() => this.o.log({ event: "closed_record_remove_failed" }));
          const kept = after as SpecDoc | null;
          if (kept) this.schedule(kept.state, kept.lastActivityAt);
          return result;
        } catch (error) {
          if (error instanceof CursorMoved) continue;
          throw error;
        }
      }
      throw new StoreUnavailableError();
    });
  }

  /** The Mac's activity events after the cursor, as sync lines: the n-th activity event of the stream is line n. */
  private async readAppLines(spec: SpecRef, from: AppCursor): Promise<{ lines: SyncLine[]; to: AppCursor }> {
    const limit = this.docs.client.info.limits.maxPageSize;
    const events = await guard(() => this.docs.client.events.readAll({ stream: specStream(spec), after: from.cursor, limit }));
    const lines: SyncLine[] = [];
    let line = from.line;
    for (const event of events) {
      // The relay's own events in the stream (sync, message) are not lines of the Mac.
      if (event.type !== ACTIVITY_EVENT) continue;
      line += 1;
      const data = event.data;
      // A line that is not a JSON object (the import keeps it as text) counts for the
      // line number, but has nothing to mirror (contract 3), as on the normal sync.
      if (typeof data !== "object" || data === null || Array.isArray(data)) continue;
      lines.push({ line, event: data as Record<string, unknown> });
    }
    const last = events.length > 0 ? events[events.length - 1].cursor : from.cursor;
    return { lines, to: { cursor: last, line } };
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
    this.docs.checkEvents([{ type: "message", data: { ...input, id } as never }]);
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
