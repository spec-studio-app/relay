// The rules of the two inboxes that a spec and a desk have: the messages from
// Claude AI that wait for the Mac, and the messages for Claude AI. They work
// on an Inboxes value, so the spec rules (specRules.ts) and the desk rules
// (deskRules.ts) share them. The message type is generic: a spec message has
// `spec` and `stage`, a desk message has `desk`. The caller gives these
// fields as the origin.
//
// This file runs in the workflow sandbox too: keep it free of Node APIs. Keep
// the order of the `newId()` calls: a change in that order changes the replay
// of a history.
import { COORDINATOR_KEEP, type Attachment, type InboxMessage, type Inboxes } from "./types.js";

export type { InboxMessage, Inboxes };

/** What a rule needs from its store. */
export interface RuleEnv {
  /** A new message ID (uuid4 in the workflow, randomUUID in the JSON store). */
  newId(): string;
  /** The time now, as ISO 8601. */
  now(): string;
}

/** The fields of a message that tell where it belongs (a spec and its stage, or a desk). */
export type Origin<M extends InboxMessage> = Omit<M, keyof InboxMessage>;

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

/** The longest wait of one read. */
export const MAX_WAIT_MS = 20_000;

/** The text of the note for Claude AI when the Mac did not take a message. */
export function bounceText(reason: string): string {
  return `The Mac did not take the message: ${reason}`;
}

/** Add a message for Claude AI. The inbox keeps its newest COORDINATOR_KEEP messages. */
export function addForCoordinator<M extends InboxMessage>(box: Inboxes<M>, message: Omit<M, "seq">): void {
  const seq = box.coordinator.nextSeq++;
  box.coordinator.messages.push({ ...message, seq } as M);
  if (box.coordinator.messages.length > COORDINATOR_KEEP) {
    box.coordinator.messages.splice(0, box.coordinator.messages.length - COORDINATOR_KEEP);
  }
}

/** Tell Claude AI that the Mac will not take the messages that wait for it. */
export function bounceMacMessages<M extends InboxMessage>(
  box: Inboxes<M>,
  reason: string,
  at: string,
  env: RuleEnv,
  origin: () => Origin<M>,
): void {
  for (const message of box.mac.messages) {
    addForCoordinator(box, {
      id: env.newId(),
      from: "session",
      ...origin(),
      text: bounceText(reason),
      replyTo: message.id,
      createdAt: at,
    } as Omit<M, "seq">);
  }
  box.mac.messages = [];
}

/** The attachments of a session event, or undefined when it has none. */
export function attachmentsOf(value: unknown): Attachment[] | undefined {
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

/** True when the inbox for Claude AI has a message with this ID. */
export function hasCoordinatorMessage<M extends InboxMessage>(box: Inboxes<M>, id: unknown): boolean {
  return typeof id === "string" && id !== "" && box.coordinator.messages.some((m) => m.id === id);
}

/**
 * Add a `message` event of the session (`studio message`) for Claude AI. An
 * event with no ID gets a new one.
 */
export function addSessionMessage<M extends InboxMessage>(
  box: Inboxes<M>,
  event: Record<string, unknown>,
  at: string,
  env: RuleEnv,
  origin: Origin<M>,
): void {
  addForCoordinator(box, {
    id: typeof event.id === "string" && event.id ? event.id : env.newId(),
    // A local answer: Spec Studio copied the user's prompt in the terminal.
    from: event.from === "user" ? "user" : "session",
    ...origin,
    text: typeof event.text === "string" ? event.text : "",
    ...(typeof event.replyTo === "string" ? { replyTo: event.replyTo } : {}),
    ...(attachmentsOf(event.attachments) ? { attachments: attachmentsOf(event.attachments) } : {}),
    createdAt: typeof event.t === "string" ? event.t : at,
  } as Omit<M, "seq">);
}

/** Add a message from Claude AI to the Mac inbox. Always a change. */
export function addForMac<M extends InboxMessage>(box: Inboxes<M>, send: SendInput, origin: Origin<M>): { seq: number; message: M } {
  const seq = box.mac.nextSeq++;
  const message = {
    id: send.id,
    seq,
    from: "coordinator",
    ...origin,
    text: send.text,
    ...(send.replyTo ? { replyTo: send.replyTo } : {}),
    ...(send.attachments && send.attachments.length > 0 ? { attachments: send.attachments } : {}),
    createdAt: send.at,
  } as unknown as M;
  box.mac.messages.push(message);
  return { seq, message };
}

/** The Mac took a message (or refused it with an error: then Claude AI gets a note). */
export function ackMac<M extends InboxMessage>(
  box: Inboxes<M>,
  ack: AckInput,
  env: RuleEnv,
  origin: () => Origin<M>,
): { result: { removed: boolean }; changed: boolean } {
  const index = box.mac.messages.findIndex((m) => m.seq === ack.seq);
  if (index < 0) return { result: { removed: false }, changed: false };
  const [dropped] = box.mac.messages.splice(index, 1);
  if (ack.error) {
    addForCoordinator(box, {
      id: env.newId(),
      from: "session",
      ...origin(),
      text: bounceText(ack.error),
      replyTo: dropped.id,
      createdAt: env.now(),
    } as Omit<M, "seq">);
  }
  return { result: { removed: true }, changed: true };
}

/** The cursor of a read: the messages above it are the answer. */
export function readCursor<M extends InboxMessage>(box: Inboxes<M>, read: ReadInput): number {
  return read.unreadOnly ? box.coordinator.readUpTo : (read.after ?? 0);
}

/** True when a message above the cursor exists (the end of a read's wait). */
export function hasNewer<M extends InboxMessage>(box: Inboxes<M>, after: number): boolean {
  return box.coordinator.nextSeq - 1 > after;
}

/** The messages above the cursor. It moves readUpTo when it returns a newer message. */
export function collectMessages<M extends InboxMessage>(box: Inboxes<M>, after: number): { messages: M[]; changed: boolean } {
  const messages = box.coordinator.messages.filter((m) => m.seq > after);
  let changed = false;
  if (messages.length > 0) {
    const last = messages[messages.length - 1].seq;
    if (last > box.coordinator.readUpTo) {
      box.coordinator.readUpTo = last;
      changed = true;
    }
  }
  return { messages, changed };
}
