// The rules of a desk, as plain functions on its state. The desk workflow
// (Temporal store) and the JSON desk store both call them, so the two stores
// cannot drift. The inbox rules are in inboxRules.ts.
//
// This file runs in the workflow sandbox too: keep it free of Node APIs.
import {
  DESK_OFF_TEXT,
  unreadForCoordinator,
  type DeskAddress,
  type DeskMemo,
  type DeskMessage,
  type DeskReport,
  type DeskState,
} from "./types.js";
import {
  ackMac,
  addForMac,
  addSessionMessage,
  collectMessages,
  hasCoordinatorMessage,
  type AckInput,
  type RuleEnv,
  type SendInput,
} from "./inboxRules.js";

/** The type of the desk workflow's refusal of a send (an ApplicationFailure in the Temporal store). */
export const DESK_REFUSED_TYPE = "DeskRefused";

export interface DeskReportInput extends Omit<DeskReport, "repoKey"> {
  /** The relay's time. */
  at: string;
}

/** One reply line of a desk's outbox. */
export interface DeskSyncLine {
  line: number;
  event: Record<string, unknown>;
}

export interface DeskSyncInput {
  lines: DeskSyncLine[];
  /** The relay's time. */
  at: string;
  repo?: string;
}

export interface DeskSyncResult {
  line: number;
  applied: boolean;
}

export interface DeskReadResult {
  messages: DeskMessage[];
  /** The time of the desk's last state from the agent. */
  stateAt: string | null;
}

/** The filter of a list of desks. */
export interface DeskListFilter {
  repoKey?: string;
  pendingForMac?: boolean;
  pendingForCoordinator?: boolean;
}

function deskRef(s: DeskState): DeskAddress {
  return { repoKey: s.desk.repoKey, ...(s.desk.repo ? { repo: s.desk.repo } : {}) };
}

/** The agent's state of the desk (one entry of a desks frame). Always a change: the time moves. */
export function applyReport(s: DeskState, input: DeskReportInput): { result: { applied: boolean }; changed: boolean } {
  if (input.repo) s.desk.repo = input.repo;
  s.open = input.open;
  s.turn = input.turn;
  s.canMessage = input.canMessage;
  s.reason = input.canMessage ? null : (input.reason ?? DESK_OFF_TEXT);
  s.stateAt = input.at;
  return { result: { applied: true }, changed: true };
}

/** The refusal of a message to the desk, or null when Claude AI can message it. */
export function deskRefusal(s: DeskState): string | null {
  if (s.canMessage) return null;
  return s.reason ?? DESK_OFF_TEXT;
}

/** Add a message from Claude AI to the desk's Mac inbox. Always a change. */
export function applyDeskSend(s: DeskState, send: SendInput): { seq: number; message: DeskMessage } {
  return addForMac<DeskMessage>(s, send, { desk: deskRef(s) });
}

/** The Mac took a desk message (or refused it with an error: then Claude AI gets a note). */
export function applyDeskAck(s: DeskState, ack: AckInput, env: RuleEnv): { result: { removed: boolean }; changed: boolean } {
  return ackMac<DeskMessage>(s, ack, env, () => ({ desk: deskRef(s) }));
}

/**
 * The desk's reply lines (the agent's desk-sync frame). Each `message` event
 * becomes a message for Claude AI. A line with an ID that the inbox has is
 * skipped, so a line sent again adds nothing. A line with no ID is skipped
 * when its number is not above the last line.
 */
export function applyDeskSync(s: DeskState, sync: DeskSyncInput, env: RuleEnv): { result: DeskSyncResult; changed: boolean } {
  let changed = false;
  if (sync.repo && s.desk.repo !== sync.repo) {
    s.desk.repo = sync.repo;
    changed = true;
  }
  let applied = false;
  for (const { line, event } of sync.lines) {
    const hasId = typeof event.id === "string" && event.id !== "";
    const fresh = line > s.line;
    if (fresh) {
      s.line = line;
      changed = true;
    }
    if (event.kind !== "message") continue;
    if (hasId ? hasCoordinatorMessage(s, event.id) : !fresh) continue;
    addSessionMessage<DeskMessage>(s, event, sync.at, env, { desk: deskRef(s) });
    applied = true;
    changed = true;
  }
  return { result: { line: sync.lines.length > 0 ? sync.lines[sync.lines.length - 1].line : s.line, applied }, changed };
}

/** The answer of a read of the desk after its wait. It moves readUpTo when it returns a newer message. */
export function collectDeskRead(s: DeskState, after: number): { result: DeskReadResult; changed: boolean } {
  const { messages, changed } = collectMessages<DeskMessage>(s, after);
  return { result: { messages, stateAt: s.stateAt }, changed };
}

/** The summary of a desk that lists show (the workflow's memo). */
export function deskMemo(s: DeskState): DeskMemo {
  return {
    repo: s.desk.repo ?? null,
    open: s.open,
    turn: s.turn,
    canMessage: s.canMessage,
    reason: s.reason,
    stateAt: s.stateAt,
    waitingForMac: s.mac.messages.length,
    unreadForCoordinator: unreadForCoordinator(s),
  };
}

/** The filter of a list on one desk, the same as the workflow's search attributes. */
export function matchesDeskFilter(repoKey: string, memo: DeskMemo, filter: DeskListFilter): boolean {
  if (filter.repoKey && repoKey !== filter.repoKey) return false;
  if (filter.pendingForMac && !(memo.waitingForMac > 0)) return false;
  if (filter.pendingForCoordinator && !(memo.unreadForCoordinator > 0)) return false;
  return true;
}
