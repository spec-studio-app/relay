// The rules of a spec's mirror and inboxes, as plain functions on its state.
// The spec workflow (Temporal store) and the JSON store both call them, so the
// two stores cannot drift. The inbox rules are in inboxRules.ts. Each change function returns `changed`: true when
// the store must count an update (and publish the memo).
//
// This file runs in the workflow sandbox too: keep it free of Node APIs. The
// workflow gives uuid4 and its own time in `env`. Keep the order of the
// `newId()` calls: a change in that order changes the replay of a history.
import {
  CLOSING_TEXT,
  DROPPED_TEXT,
  EVENT_LIMIT,
  RECREATED_TEXT,
  effectiveTurn,
  mirrorFields,
  unreadForCoordinator,
  type Message,
  type SpecMemo,
  type SpecWorkflowState,
} from "./types.js";
import {
  ackMac,
  addForCoordinator as addToCoordinatorInbox,
  addForMac,
  addSessionMessage,
  bounceMacMessages as bounceInbox,
  collectMessages,
  hasCoordinatorMessage,
  type AckInput,
  type Origin,
  type RuleEnv,
  type SendInput,
} from "./inboxRules.js";

export { CLOSING_TEXT, DROPPED_TEXT };
export { MAX_WAIT_MS, hasNewer, readCursor, type AckInput, type ReadInput, type RuleEnv, type SendInput } from "./inboxRules.js";

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
  turn?: SpecWorkflowState["turn"];
  /** The relay's time. */
  at: string;
  repo?: string;
}

export interface SyncResult {
  line: number;
  applied: boolean;
}

export interface ReadResult {
  messages: Message[];
  stage: string;
  syncedAt: string | null;
}

export interface StateInput {
  /** `studio show --json` of the spec now. */
  state: Record<string, unknown>;
  turn: SpecWorkflowState["turn"];
  /** The relay's time: the mirror's state is as of this time (syncedAt). */
  at: string;
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

/** A merged spec is finished. A dropped spec is finished by the agent's gone frame. */
function isFinished(s: SpecWorkflowState, event: Record<string, unknown>): boolean {
  return mirrorFields(s.state).stage === "merged" || event.kind === "merged";
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

function specRef(s: SpecWorkflowState) {
  return { repoKey: s.spec.repoKey, slug: s.spec.slug };
}

/** The origin of a spec's message: the spec and its stage now. */
function specOrigin(s: SpecWorkflowState): Origin<Message> {
  return { spec: specRef(s), stage: mirrorFields(s.state).stage };
}

/** Add a message for Claude AI. The inbox keeps its newest COORDINATOR_KEEP messages. */
export function addForCoordinator(s: SpecWorkflowState, message: Omit<Message, "seq">): void {
  addToCoordinatorInbox<Message>(s, message);
}

/** Tell Claude AI that the Mac will not take the messages that wait for it. */
export function bounceMacMessages(s: SpecWorkflowState, reason: string, at: string, env: RuleEnv): void {
  bounceInbox<Message>(s, reason, at, env, () => specOrigin(s));
}

/** Apply a sync batch: skip the lines the spec has, keep the first-sync rules. */
export function applySync(s: SpecWorkflowState, sync: SyncInput, env: RuleEnv): { result: SyncResult; changed: boolean } {
  const lines = syncLines(sync);
  // A single-line input of wire version 2, from a history written before
  // version 3: it keeps the old rules, so its replay stays the same.
  const legacy = !Array.isArray(sync.lines);
  if (lines.length > 0 && startsSpec(sync) && s.finished && s.line >= 1 && (legacy || isOtherSpec(s, sync))) {
    // The first sync on a finished run (merged or dropped, in its grace
    // period): the spec was made again on the Mac with the same slug (a new
    // spec, or the folder came back). Start it as a new spec. The store sees
    // that the run is no longer finished, so the grace period ends.
    bounceMacMessages(s, RECREATED_TEXT, sync.at, env);
    s.finished = false;
    s.dropped = false;
    // A frame with no state keeps the run's last one (the folder of a
    // dropped spec came back), unless that state is merged: a merged state
    // would finish the new spec at its first line.
    const kept = mirrorFields(s.state).stage === "merged" ? null : s.state;
    s.state = legacy ? null : (sync.state ?? kept);
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
      return { result: { line: s.line, applied: false }, changed: true };
    }
    return { result: { line: s.line, applied: false }, changed: false };
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
    if (event.kind === "message" && !hasCoordinatorMessage(s, event.id)) {
      addSessionMessage<Message>(s, event, sync.at, env, {
        spec: specRef(s),
        stage: typeof event.stage === "string" ? event.stage : mirrorFields(s.state).stage,
      });
    }
    if (isFinished(s, event)) s.finished = true;
  }
  if (s.events.length > EVENT_LIMIT) s.events.splice(0, s.events.length - EVENT_LIMIT);
  // One batch is one change: one memo.
  return { result: { line: s.line, applied: true }, changed: true };
}

/** Refuse a message to a spec that no longer exists on the Mac. */
export function checkSend(s: SpecWorkflowState): void {
  if (s.dropped) throw new Error(DROPPED_TEXT);
}

/** Add a message from Claude AI to the Mac inbox. Always a change. */
export function applySend(s: SpecWorkflowState, send: SendInput): { seq: number; message: Message } {
  return addForMac<Message>(s, send, { spec: specRef(s), stage: "" });
}

/** The Mac took a message (or refused it with an error). */
export function applyAck(s: SpecWorkflowState, ack: AckInput, env: RuleEnv): { result: { removed: boolean }; changed: boolean } {
  return ackMac<Message>(s, ack, env, () => specOrigin(s));
}

/**
 * The agent's state frame: the state and the turn changed with no new activity
 * line. It replaces them; the line cursor and the events stay. A dropped run
 * ignores it; a merged run in its grace period takes it. `stateOnFinished` is
 * the workflow's patch marker (true in the JSON store); it is called only for
 * a finished run that is not dropped, as before.
 */
export function applyState(
  s: SpecWorkflowState,
  input: StateInput,
  stateOnFinished: () => boolean,
): { result: { applied: boolean }; changed: boolean } {
  if (s.dropped || (s.finished && !stateOnFinished())) return { result: { applied: false }, changed: false };
  s.state = input.state;
  s.turn = input.turn;
  // The age that offline reads show is the age of this state.
  s.syncedAt = input.at;
  return { result: { applied: true }, changed: true };
}

/** The agent's gone frame: the spec no longer exists on the Mac. */
export function applyGone(s: SpecWorkflowState, gone: { at: string }, env: RuleEnv): { result: { dropped: boolean }; changed: boolean } {
  if (s.dropped) return { result: { dropped: false }, changed: false };
  s.dropped = true;
  s.finished = true;
  // The Mac can no longer take the messages that wait for it: tell Claude AI.
  bounceMacMessages(s, DROPPED_TEXT, gone.at, env);
  return { result: { dropped: true }, changed: true };
}

/** The answer of a read after its wait. It moves readUpTo when it returns a newer message. */
export function collectRead(s: SpecWorkflowState, after: number): { result: ReadResult; changed: boolean } {
  const { messages, changed } = collectMessages<Message>(s, after);
  return { result: { messages, stage: mirrorFields(s.state).stage, syncedAt: s.syncedAt }, changed };
}

/** A finished spec with empty inboxes: it ends after its grace period. */
export function isDone(s: SpecWorkflowState): boolean {
  return s.finished && s.mac.messages.length === 0 && unreadForCoordinator(s) === 0;
}

/** The summary of a spec that lists show (the workflow's memo). */
export function specMemo(s: SpecWorkflowState): SpecMemo {
  const fields = mirrorFields(s.state);
  return {
    repo: s.spec.repo ?? null,
    title: fields.title,
    stage: fields.stage,
    status: fields.status,
    note: fields.note,
    mode: fields.mode,
    turn: effectiveTurn(s.turn, s.state),
    line: s.line,
    syncedAt: s.syncedAt,
    waitingForMac: s.mac.messages.length,
    unreadForCoordinator: unreadForCoordinator(s),
    ...(s.dropped ? { dropped: true } : {}),
  };
}

/** The filter of a list of specs. */
export interface ListFilter {
  repoKey?: string;
  /** Closed specs too (Temporal: closed runs in the retention). */
  all?: boolean;
  /** Only the open specs: not merged (list_specs without all). */
  openOnly?: boolean;
  pendingForMac?: boolean;
  pendingForCoordinator?: boolean;
}

/** The filter of a list on one running spec, the same as the workflow's search attributes. */
export function matchesFilter(repoKey: string, memo: SpecMemo, filter: ListFilter): boolean {
  if (filter.openOnly && memo.stage === "merged") return false;
  if (filter.repoKey && repoKey !== filter.repoKey) return false;
  if (filter.pendingForMac && !(memo.waitingForMac > 0)) return false;
  if (filter.pendingForCoordinator && !(memo.unreadForCoordinator > 0)) return false;
  return true;
}
