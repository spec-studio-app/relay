// What the agent hub needs from the store. The hub's tests without a store give
// no backend: the hub then speaks wire version 1 only.
import type { Attachments, UploadUrlAnswer } from "./attachments.js";
import type { SyncInput, SyncResult } from "./workflows/spec.js";
import type { Message, SpecAddress, Turn } from "./workflows/types.js";

export interface AgentBackend {
  /** True when the SHA-256 (hex) of an agent secret belongs to a paired Mac. */
  verifyAgent(hash: string): Promise<boolean>;
  /** A hello of the current wire version: keep the repos and the time. */
  hello(repos: string[], at: Date): Promise<void>;
  sync(spec: SpecAddress, input: Omit<SyncInput, "at">): Promise<SyncResult>;
  /** The agent's state frame: a new state and turn with no new line. True when applied. */
  state(spec: SpecAddress, state: Record<string, unknown>, turn: Turn | null): Promise<boolean>;
  /** The agent's gone frame: the spec no longer exists on the Mac. */
  gone(spec: SpecAddress): Promise<void>;
  /** The agent wrote a message to disk, or refused it with an error. */
  ack(spec: SpecAddress, seq: number, error?: string): Promise<void>;
  /** Pair a Mac: true when the code was valid. The code works once. */
  pair(codeHash: string, secretHash: string, at: Date): Promise<boolean>;
  /** True when the code hash is the live pairing code. It does not use the code. */
  pairCodeValid(codeHash: string): Promise<boolean>;
  /** Forget a paired secret. False when it was not paired. */
  unpair(secretHash: string): Promise<boolean>;
  /** A signed PUT URL for a session attachment (the agent's upload-url). */
  uploadUrl(spec: SpecAddress, filename: string, contentType: string, size: number): Promise<UploadUrlAnswer>;
  /** A signed GET URL for an attachment of this deploy, or null. */
  downloadUrl(objectKey: string): Promise<string | null>;
  /** Every message that waits for the Mac, in seq order per spec. */
  pendingForMac(): Promise<Array<{ spec: SpecAddress; message: Message }>>;
}

export { storeAgentBackend } from "./store/adapters.js";
