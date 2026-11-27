// The messages of the Spec Studio backend plugin protocol, version 1.0.
// PROTOCOL.md at the root of the repo defines them.

export const PROTOCOL_VERSION = "1.0";
export const PROTOCOL_VERSIONS = ["1.0"];

export type HostKind = "app" | "relay" | "test";
export type Capability = "state" | "events" | "blobs" | "watch";
export type TestHook = "continueStream";

/** The JSON value of a document or an event. */
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
/** A flat map of JSON scalars. */
export type Attributes = Record<string, string | number | boolean | null>;

export interface PluginInfo {
  name: string;
  version: string;
  repo: string;
}

export interface Setting {
  name: string;
  description: string;
  required: boolean;
  secret: boolean;
}

export interface Limits {
  maxMessageBytes: number;
  maxChunkBytes: number;
  maxPageSize: number;
  maxWaitMs: number;
}

export interface DescribeResult {
  plugin: PluginInfo;
  protocolVersions: string[];
  capabilities: Capability[];
  settings: Setting[];
  testHooks: TestHook[];
  durable: boolean;
}

export interface HandshakeParams {
  host: { kind: HostKind; version: string };
  protocolVersions: string[];
  settings: Record<string, string>;
}

export interface HandshakeResult {
  plugin: PluginInfo;
  protocolVersion: string;
  capabilities: Capability[];
  testHooks: TestHook[];
  durable: boolean;
  limits: Limits;
  /** Over HTTP only. */
  session?: string;
}

export interface PingResult {
  status: "ok";
  time: string;
}

export interface StateGetParams {
  collection: string;
  key: string;
}

export type StateGetResult =
  | { exists: false }
  | { exists: true; value: Json; attributes: Attributes; version: string; updatedAt: string };

export interface StatePutParams {
  collection: string;
  key: string;
  value: Json;
  attributes?: Attributes;
  /** null: the document must not exist. */
  expectedVersion: string | null;
}

export interface StatePutResult {
  version: string;
  updatedAt: string;
}

export interface StateDeleteParams {
  collection: string;
  key: string;
  expectedVersion: string;
}

export interface StateListParams {
  collection: string;
  filter?: Attributes;
  pageSize?: number;
  cursor?: string | null;
  withValues?: boolean;
}

export interface Document {
  key: string;
  attributes: Attributes;
  version: string;
  updatedAt: string;
  /** With withValues only. */
  value?: Json;
}

export interface StateListResult {
  items: Document[];
  cursor: string | null;
}

export interface NewEvent {
  type: string;
  data: Json;
  /** The time of the event (ISO 8601). Missing: the time the plugin stores it. */
  at?: string;
}

export interface EventsAppendParams {
  stream: string;
  events: NewEvent[];
}

export interface EventsAppendResult {
  cursors: string[];
}

export interface EventsReadParams {
  stream: string;
  after?: string | null;
  from?: string;
  to?: string;
  limit?: number;
}

export interface Event {
  cursor: string;
  type: string;
  at: string;
  data: Json;
}

export interface EventsReadResult {
  events: Event[];
  cursor: string | null;
  more: boolean;
}

export interface BlobsBeginResult {
  exists: boolean;
  uploadId?: string;
}

export interface BlobsGetResult {
  /** base64 */
  data: string;
  offset: number;
  size: number;
}

export type WatchWaitParams =
  | { collection: string; key: string; version: string | null; timeoutMs: number }
  | { stream: string; after: string | null; timeoutMs: number };

export interface WatchWaitResult {
  changed: boolean;
  version?: string | null;
  cursor?: string | null;
}
