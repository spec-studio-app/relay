// One long-running workflow per spec: the mirror of its state and two inboxes.
import {
  allHandlersFinished,
  condition,
  continueAsNew,
  defineQuery,
  defineUpdate,
  proxyActivities,
  setHandler,
  upsertMemo,
  upsertSearchAttributes,
  uuid4,
  workflowInfo,
} from "@temporalio/workflow";
import type { Activities } from "../activities.js";
import {
  CLOSING_TEXT,
  COORDINATOR_KEEP,
  EVENT_LIMIT,
  FINISHED_GRACE_MS,
  SA,
  effectiveTurn,
  mirrorFields,
  unreadForCoordinator,
  type Attachment,
  type Message,
  type SpecMemo,
  type SpecWorkflowState,
  type Turn,
} from "./types.js";

export interface SyncInput {
  line: number;
  event: Record<string, unknown>;
  state: Record<string, unknown> | null;
  turn: Turn | null;
  /** The relay's time. */
  at: string;
  repo?: string;
}

export interface SyncResult {
  line: number;
  applied: boolean;
}

export interface SendInput {
  id: string;
  text: string;
  replyTo?: string;
  attachments?: Attachment[];
  at: string;
}

export interface AckInput {
  seq: number;
  error?: string;
}

export interface ReadInput {
  /** Return the messages with a seq above this cursor. */
  after?: number;
  /** Return only the unread messages (above the highest seq that any read returned). */
  unreadOnly?: boolean;
  /** Wait this long for a new message when none is there. */
  waitMs?: number;
}

export interface ReadResult {
  messages: Message[];
  stage: string;
  syncedAt: string | null;
}

export const syncUpdate = defineUpdate<SyncResult, [SyncInput]>("sync");
export const sendUpdate = defineUpdate<{ seq: number; message: Message }, [SendInput]>("send");
export const ackUpdate = defineUpdate<{ removed: boolean }, [AckInput]>("ack");
export const readUpdate = defineUpdate<ReadResult, [ReadInput]>("read");
export const stateQuery = defineQuery<SpecWorkflowState>("state");

const { removeAttachments } = proxyActivities<Activities>({
  startToCloseTimeout: "1 minute",
  retry: { maximumAttempts: 10 },
});

const MAX_WAIT_MS = 20_000;

function isFinished(s: SpecWorkflowState, event: Record<string, unknown>): boolean {
  const fields = mirrorFields(s.state);
  return fields.stage === "merged" || fields.status === "dropped" || event.kind === "dropped" || event.kind === "merged";
}

function attachmentsOf(value: unknown): Attachment[] | undefined {
  if (!Array.isArray(value) || value.length === 0) return undefined;
  return value
    .filter((a): a is Record<string, unknown> => typeof a === "object" && a !== null)
    .map((a) => ({
      // An attachment that the agent could not upload has no objectKey, and an error.
      ...(typeof a.objectKey === "string" && a.objectKey ? { objectKey: a.objectKey } : {}),
      filename: String(a.filename ?? ""),
      contentType: String(a.contentType ?? "application/octet-stream"),
      size: Number(a.size ?? 0),
      sha256: String(a.sha256 ?? ""),
      ...(typeof a.error === "string" && a.error ? { error: a.error } : {}),
    }));
}

export async function specWorkflow(input: SpecWorkflowState): Promise<void> {
  const s = input;
  let updates = 0;

  const publish = () => {
    const fields = mirrorFields(s.state);
    const unread = unreadForCoordinator(s);
    const turn = effectiveTurn(s.turn, s.state);
    upsertSearchAttributes([
      { key: SA.RelayDeployId, value: s.deployId },
      { key: SA.RepoKey, value: s.spec.repoKey },
      { key: SA.SpecSlug, value: s.spec.slug },
      { key: SA.SpecTitle, value: fields.title },
      { key: SA.Stage, value: fields.stage },
      { key: SA.Status, value: fields.status },
      { key: SA.Mode, value: fields.mode },
      { key: SA.NeedsYou, value: turn?.needsYou ?? false },
      { key: SA.PendingForMac, value: s.mac.messages.length > 0 },
      { key: SA.PendingForCoordinator, value: unread > 0 },
    ]);
    const memo: SpecMemo = {
      repo: s.spec.repo ?? null,
      title: fields.title,
      stage: fields.stage,
      status: fields.status,
      note: fields.note,
      mode: fields.mode,
      turn,
      line: s.line,
      syncedAt: s.syncedAt,
      waitingForMac: s.mac.messages.length,
      unreadForCoordinator: unread,
    };
    upsertMemo({ mirror: memo });
  };

  const addForCoordinator = (message: Omit<Message, "seq">) => {
    const seq = s.coordinator.nextSeq++;
    s.coordinator.messages.push({ ...message, seq });
    if (s.coordinator.messages.length > COORDINATOR_KEEP) {
      s.coordinator.messages.splice(0, s.coordinator.messages.length - COORDINATOR_KEEP);
    }
  };

  /** Counts every update, so the grace period of a finished spec starts again. */
  let activity = 0;
  /** True when the workflow removes the attachments and ends: the validators refuse updates. */
  let closing = false;
  const refuseWhenClosing = () => {
    if (closing) throw new Error(CLOSING_TEXT);
  };

  const changed = () => {
    updates++;
    activity++;
    publish();
  };

  setHandler(stateQuery, () => s);

  setHandler(syncUpdate, (sync) => {
    if (sync.line <= s.line) return { line: s.line, applied: false };
    s.line = sync.line;
    s.syncedAt = sync.at;
    if (sync.repo) s.spec.repo = sync.repo;
    if (sync.state) s.state = sync.state;
    s.turn = sync.turn;
    s.events.push({ line: sync.line, event: sync.event });
    if (s.events.length > EVENT_LIMIT) s.events.splice(0, s.events.length - EVENT_LIMIT);
    const event = sync.event;
    if (event.kind === "message") {
      addForCoordinator({
        id: typeof event.id === "string" && event.id ? event.id : uuid4(),
        from: "session",
        spec: { repoKey: s.spec.repoKey, slug: s.spec.slug },
        stage: typeof event.stage === "string" ? event.stage : mirrorFields(s.state).stage,
        text: typeof event.text === "string" ? event.text : "",
        ...(typeof event.replyTo === "string" ? { replyTo: event.replyTo } : {}),
        ...(attachmentsOf(event.attachments) ? { attachments: attachmentsOf(event.attachments) } : {}),
        createdAt: typeof event.t === "string" ? event.t : sync.at,
      });
    }
    if (isFinished(s, event)) s.finished = true;
    changed();
    return { line: s.line, applied: true };
  }, { validator: (_: SyncInput) => refuseWhenClosing() });

  setHandler(sendUpdate, (send) => {
    const seq = s.mac.nextSeq++;
    const message: Message = {
      id: send.id,
      seq,
      from: "coordinator",
      spec: { repoKey: s.spec.repoKey, slug: s.spec.slug },
      stage: "",
      text: send.text,
      ...(send.replyTo ? { replyTo: send.replyTo } : {}),
      ...(send.attachments && send.attachments.length > 0 ? { attachments: send.attachments } : {}),
      createdAt: send.at,
    };
    s.mac.messages.push(message);
    changed();
    return { seq, message };
  }, { validator: (_: SendInput) => refuseWhenClosing() });

  setHandler(ackUpdate, (ack) => {
    const index = s.mac.messages.findIndex((m) => m.seq === ack.seq);
    if (index < 0) return { removed: false };
    const [dropped] = s.mac.messages.splice(index, 1);
    if (ack.error) {
      addForCoordinator({
        id: uuid4(),
        from: "session",
        spec: { repoKey: s.spec.repoKey, slug: s.spec.slug },
        stage: mirrorFields(s.state).stage,
        text: `The Mac did not take the message: ${ack.error}`,
        replyTo: dropped.id,
        createdAt: new Date().toISOString(),
      });
    }
    changed();
    return { removed: true };
  }, { validator: (_: AckInput) => refuseWhenClosing() });

  setHandler(readUpdate, async (read) => {
    activity++;
    const after = read.unreadOnly ? s.coordinator.readUpTo : (read.after ?? 0);
    const newest = () => s.coordinator.nextSeq - 1;
    if (newest() <= after && (read.waitMs ?? 0) > 0) {
      await condition(() => newest() > after, Math.min(read.waitMs ?? 0, MAX_WAIT_MS));
    }
    const messages = s.coordinator.messages.filter((m) => m.seq > after);
    if (messages.length > 0) {
      const last = messages[messages.length - 1].seq;
      if (last > s.coordinator.readUpTo) {
        s.coordinator.readUpTo = last;
        changed();
      }
    }
    return { messages, stage: mirrorFields(s.state).stage, syncedAt: s.syncedAt };
  }, { validator: (_: ReadInput) => refuseWhenClosing() });

  publish();

  const done = () => s.finished && s.mac.messages.length === 0 && unreadForCoordinator(s) === 0;
  const tooLong = () => updates >= s.updateLimit || workflowInfo().continueAsNewSuggested;

  for (;;) {
    await condition(() => done() || tooLong());
    if (!tooLong()) {
      // A finished spec with empty inboxes: wait the grace period. Any update
      // in it (a read, a late line, a message) starts it again.
      const seen = activity;
      const interrupted = await condition(() => activity !== seen || !done() || tooLong(), s.graceMs ?? FINISHED_GRACE_MS);
      if (interrupted) continue;
      await condition(allHandlersFinished);
      if (activity !== seen || !done()) continue;
      // From here the validators refuse every update, so nothing that the
      // relay accepted can be lost when the workflow ends.
      closing = true;
      await removeAttachments({ deployId: s.deployId, repoKey: s.spec.repoKey, slug: s.spec.slug });
      return;
    }
    await condition(allHandlersFinished);
    await continueAsNew<typeof specWorkflow>(s);
  }
}
