// Types shared by the workflows and the relay.
// This file runs in the workflow sandbox too: keep it free of Node APIs. It has
// no Temporal import, so the JSON store can use it (the search attributes are
// in searchAttributes.ts).
export interface SpecAddress {
  repoKey: string;
  slug: string;
  repo?: string;
}

export interface Attachment {
  /** Missing when the upload failed: then `error` says why. */
  objectKey?: string;
  error?: string;
  filename: string;
  contentType: string;
  size: number;
  sha256: string;
}

/** The fields that every inbox message has, for a spec or a desk. */
export interface InboxMessage {
  id: string;
  seq: number;
  /** `user`: the user's answer in the spec's terminal, copied by Spec Studio (a local answer). */
  from: "coordinator" | "session" | "user";
  text: string;
  replyTo?: string;
  attachments?: Attachment[];
  createdAt: string;
}

/** A message of a spec. */
export interface Message extends InboxMessage {
  spec: { repoKey: string; slug: string };
  stage: string;
}

/**
 * The two inboxes of a spec or a desk. `mac`: the messages from Claude AI that
 * wait for the Mac; the agent's ack removes them. `coordinator`: the messages
 * for Claude AI. Reads do not remove them, so each chat keeps its own cursor.
 * `readUpTo` is the highest seq that any read returned: a message above it is
 * unread.
 */
export interface Inboxes<M extends InboxMessage = InboxMessage> {
  mac: { nextSeq: number; messages: M[] };
  coordinator: { nextSeq: number; messages: M[]; readUpTo: number };
}

export interface Turn {
  turn: string;
  needsYou: boolean;
}

export interface ActivityLine {
  line: number;
  event: Record<string, unknown>;
}

/** The fields of the mirror that the relay reads from `studio show --json`. */
export interface MirrorFields {
  title: string;
  stage: string;
  status: string;
  note: string | null;
  mode: string;
  prs: unknown[];
  loop: unknown | null;
}

export interface SpecWorkflowState {
  deployId: string;
  spec: SpecAddress;
  /** The last `studio show --json` of the spec. */
  state: Record<string, unknown> | null;
  turn: Turn | null;
  /** The last activity line that the workflow applied (0: none). */
  line: number;
  /** The relay's time when it applied that line. */
  syncedAt: string | null;
  /** The last 20 activity events. */
  events: ActivityLine[];
  /** Messages from Claude AI that wait for the Mac. The agent's `delivered` removes them. */
  mac: { nextSeq: number; messages: Message[] };
  /**
   * Messages for Claude AI. Reads do not remove them, so each chat keeps its own
   * cursor. `readUpTo` is the highest seq that any read returned: a message
   * above it is unread, and PendingForCoordinator is true while one exists.
   */
  coordinator: { nextSeq: number; messages: Message[]; readUpTo: number };
  /** True after a merged line or a gone frame. The workflow ends when no message waits, after the grace period. */
  finished: boolean;
  /** True after the agent's gone frame: the spec no longer exists on the Mac. */
  dropped?: boolean;
  /**
   * The time (`t`) of the newest synced status change (STATUS_KINDS). Missing
   * in runs from before it: then the newest status change in `events` counts.
   */
  lastStatusAt?: string | null;
  /** The activity line of that status change: it orders a status change and a message in the same second. */
  lastStatusLine?: number | null;
  /** The activity line of the newest synced message event. */
  lastMessageLine?: number | null;
  /** The time of the coordinator's newest message (they leave the Mac inbox after the ack). */
  lastSentAt?: string | null;
  /** Continue as new after this many updates in one run. */
  updateLimit: number;
  /**
   * A finished spec with empty inboxes stays open this long after its last
   * update (the default when missing: FINISHED_GRACE_MS), so the links in a
   * read stay valid and late updates still apply.
   */
  graceMs?: number;
}

/** A finished spec's workflow ends this long after its last update. */
export const FINISHED_GRACE_MS = 60 * 60 * 1000;

/** The send validator's refusal for a spec that no longer exists on the Mac. */
export const DROPPED_TEXT = "The spec no longer exists on the Mac.";

/** Why a message for the Mac went back to Claude AI when the first sync of a new spec with the same slug came. */
export const RECREATED_TEXT = "The spec was made again on the Mac, as a new spec with the same slug. Send the message again.";

/** The validators' refusal while a finished spec's workflow removes its data and ends. */
export const CLOSING_TEXT = "The spec is finished, and the relay is removing its data.";

export const EVENT_LIMIT = 20;
/** The coordinator inbox keeps this many of its newest messages. */
export const COORDINATOR_KEEP = 200;
export const DEFAULT_UPDATE_LIMIT = 2000;

export function initialSpecState(
  deployId: string,
  spec: SpecAddress,
  updateLimit = DEFAULT_UPDATE_LIMIT,
  graceMs = FINISHED_GRACE_MS,
): SpecWorkflowState {
  return {
    deployId,
    spec,
    state: null,
    turn: null,
    line: 0,
    syncedAt: null,
    events: [],
    mac: { nextSeq: 1, messages: [] },
    coordinator: { nextSeq: 1, messages: [], readUpTo: 0 },
    finished: false,
    updateLimit,
    graceMs,
  };
}

/**
 * The ID of a spec workflow. The deploy ID is in it, so two relays on one
 * namespace never share a spec workflow.
 */
export function specWorkflowId(deployId: string, repoKey: string, slug: string): string {
  return `spec-${deployId}-${repoKey}-${slug}`;
}

export function relayConfigWorkflowId(deployId: string): string {
  return `relay-config-${deployId}`;
}

function text(value: unknown): string {
  return typeof value === "string" ? value : "";
}

/** Read the mirror fields from `studio show --json`. Missing fields give empty values. */
export function mirrorFields(state: Record<string, unknown> | null): MirrorFields {
  const s = state ?? {};
  const repos = Array.isArray(s.repos) ? (s.repos as Array<Record<string, unknown>>) : [];
  const prs = repos.map((r) => (r && typeof r === "object" ? r.pr : null)).filter((pr) => pr !== null && pr !== undefined);
  const loops = Array.isArray(s.loops) ? (s.loops as Array<Record<string, unknown>>) : [];
  const last = loops.length > 0 ? loops[loops.length - 1] : null;
  const loop = s.loop ?? s.activeLoop ?? (last && !last.endedAt ? last : null);
  const note = typeof s.statusNote === "string" ? s.statusNote : typeof s.note === "string" ? s.note : null;
  return {
    title: text(s.title),
    stage: text(s.stage),
    status: text(s.status),
    note,
    mode: text(s.mode) || (state ? "guided" : ""),
    prs,
    loop: loop ?? null,
  };
}

/** True when the state has a `paused` object (the user paused the spec in Spec Studio). */
export function isPaused(state: Record<string, unknown> | null): boolean {
  const paused = state?.paused;
  return !!paused && typeof paused === "object";
}

/**
 * The turn of a spec. The sync's turn comes from the app's turns.json; when the
 * app does not run it is null, and the turn comes from the state the way
 * `studio specs` does it: blocked and awaiting-approval need the user.
 * A merged spec is merged, also when it is paused. A paused spec never needs
 * the user, whatever its status.
 */
export function effectiveTurn(turn: Turn | null, state: Record<string, unknown> | null): Turn | null {
  if (turn) return turn;
  if (!state) return null;
  const { stage, status } = mirrorFields(state);
  if (stage === "merged") return { turn: "merged", needsYou: false };
  if (isPaused(state)) return { turn: "paused", needsYou: false };
  switch (status) {
    case "blocked":
      return { turn: "blocked", needsYou: true };
    case "awaiting-approval":
      return { turn: "yours", needsYou: true };
    case "working":
      return { turn: "working", needsYou: false };
    case "idle":
      return { turn: "idle", needsYou: false };
    case "draft":
      return { turn: "asleep", needsYou: false };
    default:
      return null;
  }
}

/** The activity events that change the stage or the status of a spec. */
export const STATUS_KINDS: ReadonlySet<string> = new Set(["status", "approved", "went-back", "merged", "loop-ended"]);

/** The latest status change: the mirror's stage, status and note, and the time of the newest status event. */
export interface StatusChange {
  stage: string;
  status: string;
  note: string | null;
  at: string;
}

/** The newest status event (its time and line), from the state or else from the last events. */
export function lastStatus(s: SpecWorkflowState): { at: string; line: number | null } | null {
  if (typeof s.lastStatusAt === "string") return { at: s.lastStatusAt, line: s.lastStatusLine ?? null };
  for (let i = s.events.length - 1; i >= 0; i--) {
    const event = s.events[i].event;
    if (STATUS_KINDS.has(String(event.kind)) && typeof event.t === "string") return { at: event.t, line: s.events[i].line };
  }
  return null;
}

/** The spec's latest status change, or null when no status event was synced. */
export function statusChange(s: SpecWorkflowState): StatusChange | null {
  const status = lastStatus(s);
  if (!status) return null;
  const { stage, status: value, note } = mirrorFields(s.state);
  return { stage, status: value, note, at: status.at };
}

/** The time of the thread's newest message: in the coordinator inbox, or the coordinator's last send. */
export function lastMessageAt(s: SpecWorkflowState): string | null {
  const times = s.coordinator.messages.map((m) => m.createdAt);
  if (typeof s.lastSentAt === "string") times.push(s.lastSentAt);
  let newest: string | null = null;
  for (const time of times) {
    if (newest === null || Date.parse(time) > Date.parse(newest)) newest = time;
  }
  return newest;
}

/**
 * True when the spec changed after the thread's newest message. The Mac writes
 * whole seconds, so a status change and a message line in the same second are
 * ordered by their activity lines: `studio message` then `studio status` is stale.
 */
export function isStale(s: SpecWorkflowState): boolean {
  const status = lastStatus(s);
  const message = lastMessageAt(s);
  if (!status || !message) return false;
  const at = Date.parse(status.at);
  const last = Date.parse(message);
  if (Number.isNaN(at) || Number.isNaN(last)) return false;
  if (at !== last) return at > last;
  // The same time: a coordinator's send (the relay's time) is not a line; a synced message is.
  const sent = typeof s.lastSentAt === "string" && Date.parse(s.lastSentAt) === last;
  if (sent) return false;
  return status.line !== null && typeof s.lastMessageLine === "number" && status.line > s.lastMessageLine;
}

export function unreadForCoordinator(s: Pick<Inboxes, "coordinator">): number {
  return s.coordinator.messages.filter((m) => m.seq > s.coordinator.readUpTo).length;
}

/** Memo of a spec workflow: what `list_specs` shows without a query. */
export interface SpecMemo {
  repo: string | null;
  title: string;
  stage: string;
  status: string;
  note: string | null;
  mode: string;
  turn: Turn | null;
  line: number;
  syncedAt: string | null;
  waitingForMac: number;
  unreadForCoordinator: number;
  /** True when the spec no longer exists on the Mac (missing in older memos). */
  dropped?: boolean;
}

// ---- relay-config workflow ----

export interface StoredToken {
  clientId: string;
  scopes: string[];
  resource?: string;
  /** Milliseconds since the epoch. */
  expiresAt: number;
}

export interface StoredClient {
  /** The OAuth client information (RFC 7591), as JSON. */
  client: Record<string, unknown> & { client_id: string };
  signedIn: boolean;
}

export interface RelayConfigState {
  deployId: string;
  /** SHA-256 (hex) of each paired agent secret. */
  agents: Array<{ hash: string; pairedAt: string }>;
  pairCode: { hash: string; expiresAt: number } | null;
  /** Clients in registration order. */
  clients: StoredClient[];
  access: Record<string, StoredToken>;
  refresh: Record<string, StoredToken>;
  /** The repos from the agent's last hello, and its time. */
  repos: { repos: string[]; at: string } | null;
  /**
   * The archived repos (paths) from the agent's last desks frame or hello
   * that had `archivedRepos`, and its time. Null (or missing, in a state from
   * an older relay) when no agent sent the list: then no repo counts as archived.
   */
  archivedRepos?: { repos: string[]; at: string } | null;
  updateLimit: number;
}

export const MAX_AGENTS = 10;
export const MAX_CLIENTS = 100;
export const DEFAULT_CONFIG_UPDATE_LIMIT = 500;

export function initialRelayConfigState(deployId: string, pairCode: { hash: string; expiresAt: number } | null): RelayConfigState {
  return {
    deployId,
    agents: [],
    pairCode,
    clients: [],
    access: {},
    refresh: {},
    repos: null,
    archivedRepos: null,
    updateLimit: DEFAULT_CONFIG_UPDATE_LIMIT,
  };
}

// ---- desk workflow ----

/** A desk: the repo's own session, with no spec. Its address is the repo key. */
export interface DeskAddress {
  repoKey: string;
  repo?: string;
}

/** A message of a desk: `desk` in place of `spec`, and no stage. */
export interface DeskMessage extends InboxMessage {
  desk: DeskAddress;
}

/** One entry of the agent's desks frame. */
export interface DeskReport {
  repoKey: string;
  repo: string;
  open: boolean;
  turn: Turn | null;
  canMessage: boolean;
  /** Why Claude AI cannot message the desk (set when canMessage is false). */
  reason: string | null;
}

/**
 * The relay's record of a desk: the last state that the agent sent, and the
 * two inboxes. No spec state and no activity events. A desk record lives as
 * long as the relay.
 */
export interface DeskState extends Inboxes<DeskMessage> {
  deployId: string;
  desk: DeskAddress;
  open: boolean;
  turn: Turn | null;
  canMessage: boolean;
  reason: string | null;
  /** The relay's time of the last desks frame for this desk (null: none yet). */
  stateAt: string | null;
  /** The last reply line of the desk's outbox that the relay applied (0: none). */
  line: number;
  /** Continue as new after this many updates in one run. */
  updateLimit: number;
}

/** Memo of a desk workflow: what lists show without a query. */
export interface DeskMemo {
  repo: string | null;
  open: boolean;
  turn: Turn | null;
  canMessage: boolean;
  reason: string | null;
  stateAt: string | null;
  waitingForMac: number;
  unreadForCoordinator: number;
}

/**
 * The reason of a desk whose repo is not in the Mac's last desks frame (the
 * user removed or moved the repo). The same text as the agent's.
 */
export const DESK_GONE_TEXT = "The repo is not in Spec Studio on this Mac.";

/** The refusal when Claude AI cannot message a desk and the agent gave no reason. */
export const DESK_OFF_TEXT =
  "Claude AI cannot message this desk. The user can turn it on in Spec Studio, Settings, Remote Relay.";

export function initialDeskState(deployId: string, desk: DeskAddress, updateLimit = DEFAULT_UPDATE_LIMIT): DeskState {
  return {
    deployId,
    desk: { repoKey: desk.repoKey, ...(desk.repo ? { repo: desk.repo } : {}) },
    open: false,
    turn: null,
    canMessage: false,
    reason: null,
    stateAt: null,
    line: 0,
    mac: { nextSeq: 1, messages: [] },
    coordinator: { nextSeq: 1, messages: [], readUpTo: 0 },
    updateLimit,
  };
}

/**
 * The ID of a desk workflow. The deploy ID is in it, so two relays on one
 * namespace never share a desk workflow.
 */
export function deskWorkflowId(deployId: string, repoKey: string): string {
  return `desk-${deployId}-${repoKey}`;
}
