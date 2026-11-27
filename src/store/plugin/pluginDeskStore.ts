// The plugin desk store: one document per desk in `desks`, and the desk's
// events in the stream desk:<repoKey> (PROTOCOL.md section 14). The rules are
// the ones of workflows/deskRules.ts, as in the JSON store. A desk lives as
// long as the relay.
import { randomUUID } from "node:crypto";
import type { Logger } from "../../log.js";
import type { Attributes, NewEvent } from "../../vendor/plugin-sdk/index.js";
import { initialDeskState, type Attachment, type DeskAddress, type DeskMessage, type DeskReport, type DeskState } from "../../workflows/types.js";
import { MAX_WAIT_MS, hasNewer, readCursor, type RuleEnv } from "../../workflows/inboxRules.js";
import {
  applyDeskAck,
  applyDeskSend,
  applyDeskSync,
  applyReport,
  collectDeskRead,
  deskMemo,
  deskRefusal,
  matchesDeskFilter,
  type DeskListFilter,
  type DeskReadResult,
  type DeskSyncLine,
  type DeskSyncResult,
} from "../../workflows/deskRules.js";
import { DeskNotFoundError, DeskRefusedError } from "../errors.js";
import type { DeskStore, ListedDesk } from "../types.js";
import { KeyQueue, OTHER_DEPLOY_TEXT, guard, waitForChange, type Doc, type Docs } from "./docs.js";
import { StoreRefusedError } from "../errors.js";

export const DESKS = "desks";

interface DeskDoc {
  version: 1;
  state: DeskState;
}

/** The stream of a desk's events. */
export function deskStream(repoKey: string): string {
  return `desk:${repoKey}`;
}

function attributesOf(s: DeskState): Attributes {
  const memo = deskMemo(s);
  return {
    repoKey: s.desk.repoKey,
    deployId: s.deployId,
    pendingForMac: memo.waitingForMac > 0,
    pendingForCoordinator: memo.unreadForCoordinator > 0,
  };
}

export class PluginDeskStore implements DeskStore {
  private readonly queue = new KeyQueue();
  private readonly stop = new AbortController();
  private readonly env: RuleEnv;

  constructor(
    private readonly docs: Docs,
    private readonly o: { deployId: string; log: Logger; now?: () => Date },
  ) {
    this.env = { newId: () => randomUUID(), now: () => (this.o.now?.() ?? new Date()).toISOString() };
  }

  /** End the waits. The data stays. */
  close(): void {
    this.stop.abort();
  }

  private async readDoc(repoKey: string): Promise<Doc<DeskDoc> | null> {
    const doc = await this.docs.get<DeskDoc>(DESKS, repoKey);
    return doc && doc.value.state.deployId === this.o.deployId ? doc : null;
  }

  /**
   * Apply a change to a desk. With `make`, a missing desk starts as a new
   * one; without it, a missing desk is DeskNotFoundError.
   */
  private change<T>(desk: { repoKey: string }, fn: (s: DeskState) => { result: T; changed: boolean }, make?: DeskAddress): Promise<T> {
    return this.queue.run(desk.repoKey, () =>
      this.docs.update<DeskDoc, T>(DESKS, desk.repoKey, (doc) => {
        if (doc && doc.value.state.deployId !== this.o.deployId) throw new StoreRefusedError(OTHER_DEPLOY_TEXT);
        const mine = doc;
        if (!mine && !make) throw new DeskNotFoundError();
        const state = mine ? structuredClone(mine.value.state) : initialDeskState(this.o.deployId, make!);
        const out = fn(state);
        if (!out.changed) return { result: out.result };
        return { write: { value: { version: 1, state }, attributes: attributesOf(state) }, result: out.result };
      }),
    );
  }

  private async append(repoKey: string, events: NewEvent[]): Promise<void> {
    if (events.length === 0 || !this.docs.client.has("events")) return;
    await guard(() => this.docs.client.events.append(deskStream(repoKey), events)).catch(() => this.o.log({ event: "desk_events_append_failed" }));
  }

  async report(report: DeskReport): Promise<void> {
    const { repoKey, ...rest } = report;
    await this.change({ repoKey }, (s) => applyReport(s, { ...rest, at: this.env.now() }), { repoKey, repo: report.repo });
  }

  async sync(desk: DeskAddress, lines: DeskSyncLine[]): Promise<DeskSyncResult> {
    const events = [{ type: "sync", data: { lines } as never }];
    this.docs.checkEvents(events);
    const result = await this.change(
      desk,
      (s) => applyDeskSync(s, { lines, at: this.env.now(), ...(desk.repo ? { repo: desk.repo } : {}) }, this.env),
      desk,
    );
    if (result.applied) await this.append(desk.repoKey, events);
    return result;
  }

  async get(desk: { repoKey: string }): Promise<DeskState | null> {
    return (await this.readDoc(desk.repoKey))?.value.state ?? null;
  }

  async send(desk: { repoKey: string }, input: { text: string; replyTo?: string; attachments?: Attachment[] }): Promise<{ seq: number; message: DeskMessage }> {
    const id = randomUUID();
    this.docs.checkEvents([{ type: "message", data: { ...input, id } as never }]);
    const sent = await this.change(desk, (s) => {
      const refusal = deskRefusal(s);
      if (refusal) throw new DeskRefusedError(refusal);
      return { result: applyDeskSend(s, { ...input, id, at: this.env.now() }), changed: true };
    });
    await this.append(desk.repoKey, [{ type: "message", data: sent.message as never }]);
    return sent;
  }

  ack(desk: { repoKey: string }, seq: number, error?: string): Promise<{ removed: boolean }> {
    return this.change(desk, (s) => applyDeskAck(s, { seq, ...(error ? { error } : {}) }, this.env));
  }

  async read(desk: { repoKey: string }, after: number | undefined, waitMs: number, unreadOnly = false): Promise<DeskReadResult> {
    const first = await this.readDoc(desk.repoKey);
    if (!first) throw new DeskNotFoundError();
    const cursor = readCursor(first.value.state, { after, unreadOnly });
    const deadline = Date.now() + Math.min(waitMs, MAX_WAIT_MS);
    let doc: Doc<DeskDoc> = first;
    while (!hasNewer(doc.value.state, cursor)) {
      const left = deadline - Date.now();
      if (left <= 0 || this.stop.signal.aborted) break;
      await waitForChange(this.docs, { collection: DESKS, key: desk.repoKey, version: doc.version }, left, this.stop.signal);
      const next = await this.readDoc(desk.repoKey);
      if (!next) throw new DeskNotFoundError();
      doc = next;
    }
    return this.change(desk, (s) => collectDeskRead(s, cursor));
  }

  async list(filter: DeskListFilter = {}): Promise<ListedDesk[]> {
    const where: Attributes = { deployId: this.o.deployId };
    if (filter.repoKey) where.repoKey = filter.repoKey;
    if (filter.pendingForMac) where.pendingForMac = true;
    if (filter.pendingForCoordinator) where.pendingForCoordinator = true;
    const out: ListedDesk[] = [];
    for (const item of await this.docs.list<DeskDoc>(DESKS, where)) {
      const s = item.value.state;
      const memo = deskMemo(s);
      if (s.deployId === this.o.deployId && matchesDeskFilter(s.desk.repoKey, memo, filter)) out.push({ repoKey: s.desk.repoKey, memo });
    }
    return out;
  }
}
