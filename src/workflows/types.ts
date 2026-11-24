// Types and search attributes shared by the workflows and the relay.
// This file runs in the workflow sandbox too: keep it free of Node APIs.
import { defineSearchAttributeKey, SearchAttributeType } from "@temporalio/common";

export const SPEC_WORKFLOW_TYPE = "specWorkflow";
export const RELAY_CONFIG_WORKFLOW_TYPE = "relayConfigWorkflow";

export const SA = {
  RepoKey: defineSearchAttributeKey("RepoKey", SearchAttributeType.KEYWORD),
  SpecSlug: defineSearchAttributeKey("SpecSlug", SearchAttributeType.KEYWORD),
  SpecTitle: defineSearchAttributeKey("SpecTitle", SearchAttributeType.TEXT),
  Stage: defineSearchAttributeKey("Stage", SearchAttributeType.KEYWORD),
  Status: defineSearchAttributeKey("Status", SearchAttributeType.KEYWORD),
  Mode: defineSearchAttributeKey("Mode", SearchAttributeType.KEYWORD),
  NeedsYou: defineSearchAttributeKey("NeedsYou", SearchAttributeType.BOOL),
  PendingForMac: defineSearchAttributeKey("PendingForMac", SearchAttributeType.BOOL),
  PendingForCoordinator: defineSearchAttributeKey("PendingForCoordinator", SearchAttributeType.BOOL),
  RelayDeployId: defineSearchAttributeKey("RelayDeployId", SearchAttributeType.KEYWORD),
} as const;

export const SEARCH_ATTRIBUTE_KEYS = Object.values(SA);

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

export interface Message {
  id: string;
  seq: number;
  from: "coordinator" | "session";
  spec: { repoKey: string; slug: string };
  stage: string;
  text: string;
  replyTo?: string;
  attachments?: Attachment[];
  createdAt: string;
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
  /** True after a merged or dropped line. The workflow ends when no message waits. */
  finished: boolean;
  /** Continue as new after this many updates in one run. */
  updateLimit: number;
}

export const EVENT_LIMIT = 20;
/** The coordinator inbox keeps this many of its newest messages. */
export const COORDINATOR_KEEP = 200;
export const DEFAULT_UPDATE_LIMIT = 2000;

export function initialSpecState(deployId: string, spec: SpecAddress, updateLimit = DEFAULT_UPDATE_LIMIT): SpecWorkflowState {
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
  };
}

export function specWorkflowId(repoKey: string, slug: string): string {
  return `spec-${repoKey}-${slug}`;
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

export function unreadForCoordinator(s: SpecWorkflowState): number {
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
    updateLimit: DEFAULT_CONFIG_UPDATE_LIMIT,
  };
}
