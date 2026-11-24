// One long-running workflow per spec: the mirror of its state and two inboxes.
import {
  allHandlersFinished,
  condition,
  continueAsNew,
  defineQuery,
  patched,
  defineUpdate,
  proxyActivities,
  setHandler,
  upsertMemo,
  upsertSearchAttributes,
  uuid4,
  workflowInfo,
} from "@temporalio/workflow";
import type { Activities } from "../activities.js";
import { checkBound, type Bound } from "./bound.js";
import {
  CLOSING_TEXT,
  DROPPED_TEXT,
  RECREATED_TEXT,
  COORDINATOR_KEEP,
  EVENT_LIMIT,
  FINISHED_GRACE_MS,
  SA,
  effectiveTurn,
  specWorkflowId,
  mirrorFields,
  unreadForCoordinator,
  type Attachment,
  type Message,
  type SpecMemo,
  type SpecWorkflowState,
  type Turn,
} from "./types.js";

/** One activity line of a sync batch. */
export interface SyncLine {
  line: number;
  event: Record<string, unknown>;
}

/**
 * One sync frame of wire version 3: the new lines of a spec, in line order,
 * with the state and the turn when they changed. A missing state or turn
 * keeps the mirror's; `turn: null` means that the app does not run.
 *
 * The workflows that ran before version 3 have single-line updates (`line` and
 * `event`) in their history: the handler reads them as a batch of one line, so
 * their replay stays the same.
 */
export interface SyncInput {
  lines?: SyncLine[];
  /** The agent had no cursor for the spec: a finished run starts again as a new spec. */
  first?: boolean;
  /** Wire version 2: one line. */
  line?: number;
  event?: Record<string, unknown>;
  state?: Record<string, unknown> | null;
  turn?: Turn | null;
  /** The relay's time. */
  at: string;
  repo?: string;
}

/** The lines of a sync input, in the order they came. */
export function syncLines(input: Pick<SyncInput, "lines" | "line" | "event">): SyncLine[] {
  if (Array.isArray(input.lines)) return input.lines;
  if (typeof input.line === "number" && input.event) return [{ line: input.line, event: input.event }];
  return [];
}

/** The last line of a sync input, or 0 when it has none. */
export function lastSyncLine(input: Pick<SyncInput, "lines" | "line" | "event">): number {
  const lines = syncLines(input);
  return lines.length > 0 ? lines[lines.length - 1].line : 0;
}

/**
 * True when the input may start a spec again: the agent's first sync of the
 * spec, or a batch that starts at line 1. Before the short first sync, line 1
 * was the only sign of a new spec with the same slug.
 */
export function startsSpec(input: Pick<SyncInput, "lines" | "line" | "event" | "first">): boolean {
  return input.first === true || syncLines(input)[0]?.line === 1;
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

export const syncUpdate = defineUpdate<SyncResult, [SyncInput, Bound]>("sync");
export const sendUpdate = defineUpdate<{ seq: number; message: Message }, [SendInput, Bound]>("send");
export const ackUpdate = defineUpdate<{ removed: boolean }, [AckInput, Bound]>("ack");
export const readUpdate = defineUpdate<ReadResult, [ReadInput, Bound]>("read");
export interface StateInput {
  /** `studio show --json` of the spec now. */
  state: Record<string, unknown>;
  turn: Turn | null;
  /** The relay's time: the mirror's state is as of this time (syncedAt). */
  at: string;
}

/**
 * The agent's state frame: the state and the turn changed with no new activity
 * line (for example, the turn moved). It replaces them; the line cursor and the
 * events stay. A dropped run ignores it; a merged run in its grace period takes it.
 */
export const stateUpdate = defineUpdate<{ applied: boolean }, [StateInput, Bound]>("state");

/** The agent's gone frame: the spec no longer exists on the Mac. */
export const goneUpdate = defineUpdate<{ dropped: boolean }, [{ at: string }, Bound]>("gone");
export const stateQuery = defineQuery<SpecWorkflowState>("state");

const { removeAttachments } = proxyActivities<Activities>({
  startToCloseTimeout: "1 minute",
  retry: { maximumAttempts: 10 },
});

const MAX_WAIT_MS = 20_000;

/** A merged spec is finished. A dropped spec is finished by the agent's gone frame. */
function isFinished(s: SpecWorkflowState, event: Record<string, unknown>): boolean {
  return mirrorFields(s.state).stage === "merged" || event.kind === "merged";
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

/**
 * True when a first sync on a finished run belongs to another spec with the
 * same slug: the run was dropped (the folder came back), or the frame's state
 * has another `createdAt`. A first sync of the same merged spec sent again
 * (a lost answer, or a first sync in more frames), or one with no state,
 * does not reset a merged run.
 */
function isOtherSpec(s: SpecWorkflowState, sync: SyncInput): boolean {
  if (s.dropped) return true;
  const was = s.state?.createdAt;
  const now = sync.state?.createdAt;
  // With no createdAt in the frame (studio show failed, so the state is null)
  // there is no proof of a new spec: keep the run.
  if (typeof now !== "string") return false;
  return typeof was !== "string" || was !== now;
}

export async function specWorkflow(input: SpecWorkflowState): Promise<void> {
  const s = input;
  // A start input copied from another spec's workflow: end at once.
  if (workflowInfo().workflowId !== specWorkflowId(s.deployId, s.spec.repoKey, s.spec.slug)) return;
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
      ...(s.dropped ? { dropped: true } : {}),
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
  /** Every validator: the input is bound to this workflow (bound.ts), and the workflow is not closing. */
  const open = (b: Bound) => {
    checkBound(b);
    if (closing) throw new Error(CLOSING_TEXT);
  };

  const changed = () => {
    updates++;
    activity++;
    publish();
  };

  setHandler(stateQuery, () => s);

  /** Tell Claude AI that the Mac will not take the messages that wait for it. */
  const bounceMacMessages = (reason: string, at: string) => {
    for (const message of s.mac.messages) {
      addForCoordinator({
        id: uuid4(),
        from: "session",
        spec: { repoKey: s.spec.repoKey, slug: s.spec.slug },
        stage: mirrorFields(s.state).stage,
        text: `The Mac did not take the message: ${reason}`,
        replyTo: message.id,
        createdAt: at,
      });
    }
    s.mac.messages = [];
  };

  setHandler(syncUpdate, (sync) => {
    const lines = syncLines(sync);
    // A single-line input of wire version 2, from a history written before
    // version 3: it keeps the old rules, so its replay stays the same.
    const legacy = !Array.isArray(sync.lines);
    if (lines.length > 0 && startsSpec(sync) && s.finished && s.line >= 1 && (legacy || isOtherSpec(s, sync))) {
      // The first sync on a finished run (merged or dropped, in its grace
      // period): the spec was made again on the Mac with the same slug (a new
      // spec, or the folder came back). Start it as a new spec. The main loop
      // sees that the run is no longer finished, so the grace period ends.
      bounceMacMessages(RECREATED_TEXT, sync.at);
      s.finished = false;
      s.dropped = false;
      // A frame with no state keeps the run's last one: the agent sends no
      // state that it sent before on this connection.
      s.state = legacy ? null : (sync.state ?? s.state);
      s.turn = legacy ? null : sync.turn !== undefined ? sync.turn : s.turn;
      s.events = [];
      s.line = 0;
      s.syncedAt = null;
    }
    const fresh = lines.filter((l) => l.line > s.line);
    if (fresh.length === 0) {
      // No new line, but a state: the agent counts it as delivered once the
      // relay answers synced, so apply it (a dropped spec keeps its last state).
      if (!legacy && sync.state && !s.dropped) {
        s.state = sync.state;
        if (sync.turn !== undefined) s.turn = sync.turn;
        s.syncedAt = sync.at;
        changed();
      }
      return { line: s.line, applied: false };
    }
    s.syncedAt = sync.at;
    if (sync.repo) s.spec.repo = sync.repo;
    if (sync.state) s.state = sync.state;
    if (sync.turn !== undefined) s.turn = sync.turn;
    for (const { line, event } of fresh) {
      if (line <= s.line) continue;
      s.line = line;
      s.events.push({ line, event });
      // A message that the inbox already has (a restored spec sends its old lines
      // again after the reset of a first sync) is not added a second time.
      const knownMessage =
        typeof event.id === "string" && event.id !== "" && s.coordinator.messages.some((m) => m.id === event.id);
      if (event.kind === "message" && !knownMessage) {
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
    }
    if (s.events.length > EVENT_LIMIT) s.events.splice(0, s.events.length - EVENT_LIMIT);
    // One batch is one change: one memo and one search attribute upsert.
    changed();
    return { line: s.line, applied: true };
  }, { validator: (_: SyncInput, b: Bound) => open(b) });

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
  }, {
    validator: (_: SendInput, b: Bound) => {
      open(b);
      if (s.dropped) throw new Error(DROPPED_TEXT);
    },
  });

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
  }, { validator: (_: AckInput, b: Bound) => open(b) });

  setHandler(stateUpdate, (input) => {
    // A merged spec in its grace period takes a new state too: the agent counts
    // a state frame as delivered, so its later sync frames leave the state out.
    // Histories from before this rule keep it (the patch marker).
    if (s.dropped || (s.finished && !patched("state-on-finished-run"))) return { applied: false };
    s.state = input.state;
    s.turn = input.turn;
    // The age that offline reads show is the age of this state.
    s.syncedAt = input.at;
    changed();
    return { applied: true };
  }, { validator: (_: StateInput, b: Bound) => open(b) });

  setHandler(goneUpdate, (gone) => {
    if (s.dropped) return { dropped: false };
    s.dropped = true;
    s.finished = true;
    // The Mac can no longer take the messages that wait for it: tell Claude AI.
    bounceMacMessages(DROPPED_TEXT, gone.at);
    changed();
    return { dropped: true };
  }, { validator: (_: { at: string }, b: Bound) => open(b) });

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
  }, { validator: (_: ReadInput, b: Bound) => open(b) });

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
