// The relay's store: every read and write of relay state goes through these
// interfaces. Three stores implement them: the JSON store (files on a volume,
// the default), the Temporal store (one workflow per spec), and the plugin
// store (a backend plugin, store/plugin/).
import type { OAuthClientInformationFull } from "@modelcontextprotocol/sdk/shared/auth.js";
import type { Logger } from "../log.js";
import type { PayloadKey } from "../config.js";
import type { ListFilter, ReadResult, StateInput, SyncInput, SyncResult } from "../workflows/specRules.js";
import type { TokenPair } from "../workflows/configRules.js";
import type { DeskListFilter, DeskReadResult, DeskSyncLine, DeskSyncResult } from "../workflows/deskRules.js";
import type {
  Attachment,
  DeskAddress,
  DeskMemo,
  DeskMessage,
  DeskReport,
  DeskState,
  Message,
  RelayConfigState,
  SpecAddress,
  SpecMemo,
  SpecWorkflowState,
  StoredClient,
  StoredToken,
} from "../workflows/types.js";

export type { DeskListFilter, ListFilter, TokenPair };

export type StoreKind = "json" | "temporal" | "plugin";

/** A spec by its repo key and slug. */
export interface SpecRef {
  repoKey: string;
  slug: string;
}

/** A sync frame with "linesInStore": no lines and no state. */
export interface StoreSyncInput {
  /** The agent had no cursor for the spec. */
  first?: boolean;
  /** The turn of the frame. Missing: keep the spec's turn. */
  turn?: SpecWorkflowState["turn"];
}

/** The store that the relay uses, for the welcome: the plugin's name and its storeId. */
export interface StoreIdentity {
  plugin: string;
  storeId: string;
}

export interface ListedSpec {
  repoKey: string;
  slug: string;
  running: boolean;
  memo: SpecMemo | null;
}

/**
 * The mirror of each spec and its two inboxes. The errors are the ones in
 * store/errors.ts: SpecNotFoundError, SpecClosingError, SpecDroppedError.
 */
export interface SpecStore {
  /** Apply a batch of activity lines. The first sync of a spec makes it. */
  sync(spec: SpecAddress, input: Omit<SyncInput, "at">): Promise<SyncResult>;
  /**
   * A sync frame with "linesInStore": the Mac's store is this store. Read the
   * Mac's state and its new activity lines from the store, and apply them as
   * a sync. Only the plugin store has it.
   */
  syncFromStore?(spec: SpecAddress, input: StoreSyncInput): Promise<SyncResult>;
  /** The spec's state, or null when the store has no such spec. */
  state(spec: SpecRef): Promise<SpecWorkflowState | null>;
  /** The agent's state frame. True when the store applied it. */
  refresh(spec: SpecRef, input: Omit<StateInput, "at">): Promise<boolean>;
  /** The agent's gone frame: the spec no longer exists on the Mac. */
  gone(spec: SpecRef): Promise<void>;
  /** Add a message from Claude AI to the spec's Mac inbox. */
  send(spec: SpecRef, input: { text: string; replyTo?: string; attachments?: Attachment[] }): Promise<{ seq: number; message: Message }>;
  /** The Mac wrote the message to disk (or refused it with an error). */
  ack(spec: SpecRef, seq: number, error?: string): Promise<{ removed: boolean }>;
  /** Messages for Claude AI after the cursor. Waits up to waitMs for a new one. */
  read(spec: SpecRef, after: number | undefined, waitMs: number, unreadOnly?: boolean): Promise<ReadResult>;
  /** The specs of this relay, with their summaries. */
  list(filter?: ListFilter): Promise<ListedSpec[]>;
}

export interface ListedDesk {
  repoKey: string;
  memo: DeskMemo;
}

/**
 * The desk of each repo: its last state from the agent and its two inboxes.
 * The errors are the ones in store/errors.ts: DeskNotFoundError and
 * DeskRefusedError. A desk is never a spec: SpecStore.list never shows one.
 */
export interface DeskStore {
  /** One entry of the agent's desks frame. It makes the desk when the store has none. */
  report(report: DeskReport): Promise<void>;
  /** The desk's state, or null when the store has no such desk. */
  get(desk: { repoKey: string }): Promise<DeskState | null>;
  /** Add a message from Claude AI to the desk's Mac inbox. Refused when Claude AI cannot message the desk. */
  send(desk: { repoKey: string }, input: { text: string; replyTo?: string; attachments?: Attachment[] }): Promise<{ seq: number; message: DeskMessage }>;
  /** The Mac wrote the message to disk (or refused it with an error). */
  ack(desk: { repoKey: string }, seq: number, error?: string): Promise<{ removed: boolean }>;
  /** The desk's reply lines (desk-sync). It makes the desk when the store has none. */
  sync(desk: DeskAddress, lines: DeskSyncLine[]): Promise<DeskSyncResult>;
  /** Messages for Claude AI after the cursor. Waits up to waitMs for a new one. */
  read(desk: { repoKey: string }, after: number | undefined, waitMs: number, unreadOnly?: boolean): Promise<DeskReadResult>;
  /** The desks of this relay, with their summaries. */
  list(filter?: DeskListFilter): Promise<ListedDesk[]>;
}

/** The pairing, the repos of the agent's hello, and the OAuth clients and tokens. */
export interface RelayConfigStore {
  /** True when the SHA-256 (hex) of an agent secret belongs to a paired Mac. */
  verifyAgent(hash: string): Promise<boolean>;
  /** The number of paired Macs. */
  paired(): Promise<number>;
  /** Pair a Mac: true when the code was valid. The code works once. */
  pair(codeHash: string, secretHash: string, at: Date): Promise<boolean>;
  /** True when the hash is the live pairing code. It does not use the code. */
  pairCodeValid(codeHash: string): Promise<boolean>;
  /** Set a new pairing code. The old code stops working. */
  setPairCode(code: { hash: string; expiresAt: number }): Promise<void>;
  /** Forget a paired secret. False when it was not paired. */
  unpair(secretHash: string): Promise<boolean>;
  setRepos(repos: string[], at: Date): Promise<void>;
  repos(): Promise<RelayConfigState["repos"]>;
  /** Keep the archived repos (paths) that the agent sent, and the time. */
  setArchivedRepos(repos: string[], at: Date): Promise<void>;
  /** The archived repos that the agent sent last, or null when no agent sent a list. */
  archivedRepos(): Promise<{ repos: string[]; at: string } | null>;
  getClient(id: string): Promise<StoredClient | null>;
  registerClient(client: StoredClient["client"]): Promise<boolean>;
  markSignedIn(clientId: string): Promise<void>;
  issueTokens(pair: TokenPair): Promise<void>;
  rotateRefresh(oldHash: string, clientId: string, next: TokenPair): Promise<StoredToken | null>;
  token(kind: "access" | "refresh", hash: string): Promise<StoredToken | null>;
  revoke(hash: string, clientId: string): Promise<void>;
}

export interface StoreStartOptions {
  log: Logger;
  /** The console for the owner: the pairing code goes here, never to the log. */
  print: (text: string) => void;
}

export interface StoreStartResult {
  /** The pairing code, when this start made the relay's config. */
  pairCode: string | null;
}

export interface RelayStore {
  kind: StoreKind;
  specs: SpecStore;
  desks: DeskStore;
  config: RelayConfigStore;
  /**
   * The plugin's name and storeId, from its last handshake. Null when the
   * plugin gave no storeId. Only the plugin store has it.
   */
  identity?(): StoreIdentity | null;
  /** The data keys for the codec endpoints (Temporal only). */
  codecDataKeys?: PayloadKey[];
  /** The checks at start, the first pairing code, and the background work (workers, timers). */
  start(options: StoreStartOptions): Promise<StoreStartResult>;
  close(): Promise<void>;
}

/** The OAuth client information as the SDK types it. */
export type OAuthClient = OAuthClientInformationFull;
