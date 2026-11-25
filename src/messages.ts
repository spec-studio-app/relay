// The tools send_message, read_messages and upload_attachment. They use the
// spec store and the desk store, with the Mac online or offline. A repo with a
// slug means the spec; a repo with no slug means the repo's desk.
import { repoKeyOf } from "./store/address.js";
import { DeskNotFoundError, DeskRefusedError, SpecClosingError, SpecDroppedError, SpecNotFoundError } from "./store/errors.js";
import type { DeskStore, SpecStore } from "./store/types.js";
import type { ToolText } from "./offline.js";
import type { Attachment, DeskAddress, DeskMessage, DeskState, Message, SpecAddress } from "./workflows/types.js";
import { limitText, NO_BUCKET_TEXT, type AttachmentOwner, type Attachments } from "./attachments.js";

export const READ_WAIT_MS = 20_000;
const POLL_MS = 1000;

export interface MessageToolsOptions {
  specs: SpecStore;
  /** Push a new message to the Mac when the agent is connected. */
  push: (spec: SpecAddress, message: Message) => void;
  /** The desks. Without it a slug is required. */
  desks?: DeskStore;
  /** Push a new desk message to the Mac. False when no agent that takes desk messages is connected. */
  pushDesk?: (desk: DeskAddress, message: DeskMessage) => boolean;
  attachments: Attachments;
  /** How long read_messages waits for a new message (tests set less). */
  waitMs?: number;
  /** The time of the newest sync of any spec, for the "no synced spec" error. */
  lastSyncAt?: () => Promise<string | null>;
}

function str(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** The tool's text when the relay has no desk for the repo. */
export function noDeskText(repo: string): string {
  return `The relay has no desk for the repo "${repo}". Call list_repos for the repo paths.`;
}

/** The last part of a repo path: its name. */
function repoName(path: string): string {
  const parts = path.replace(/\/+$/, "").split("/");
  return parts[parts.length - 1] ?? path;
}

/** A desk message as read_messages shows it: its repo and "desk": true in place of the spec. */
function shownDesk(message: DeskMessage, repo: string | undefined) {
  return {
    seq: message.seq,
    id: message.id,
    from: message.from,
    desk: true as const,
    repo: repo ?? message.desk.repo ?? null,
    text: message.text,
    ...(message.replyTo ? { replyTo: message.replyTo } : {}),
    ...(message.attachments ? { attachments: message.attachments } : {}),
    createdAt: message.createdAt,
  };
}

/** A message as read_messages shows it. */
function shown(message: Message, repo: string | undefined) {
  return {
    seq: message.seq,
    id: message.id,
    from: message.from,
    spec: { repo: repo ?? null, slug: message.spec.slug },
    stage: message.stage,
    text: message.text,
    ...(message.replyTo ? { replyTo: message.replyTo } : {}),
    ...(message.attachments ? { attachments: message.attachments } : {}),
    createdAt: message.createdAt,
  };
}

export class MessageTools {
  constructor(private readonly o: MessageToolsOptions) {}

  private get waitMs(): number {
    return this.o.waitMs ?? READ_WAIT_MS;
  }

  async answer(tool: string, args: Record<string, unknown>): Promise<ToolText> {
    try {
      if (tool === "send_message") return await this.send(args);
      if (tool === "read_messages") return await this.read(args);
      if (tool === "upload_attachment") return await this.upload(args);
      return { ok: false, text: `The relay has no tool ${tool}.` };
    } catch (error) {
      if (error instanceof SpecDroppedError) {
        return { ok: false, text: `${error.message} It takes no more messages.` };
      }
      if (error instanceof SpecClosingError) {
        return { ok: false, text: `${error.message} It takes no more messages.` };
      }
      if (error instanceof SpecNotFoundError) {
        return { ok: false, text: `The relay has no synced spec with this repo and slug. ${await this.lastSyncText()} Check them with list_specs.` };
      }
      if (error instanceof DeskRefusedError) return { ok: false, text: error.message };
      if (error instanceof DeskNotFoundError) return { ok: false, text: noDeskText(str(args.repo) ?? "") };
      return { ok: false, text: "The relay cannot reach its store now. Try again later." };
    }
  }

  /** The time of the last sync of any spec, so a caller can tell a stopped sync from a wrong slug. */
  private async lastSyncText(): Promise<string> {
    if (!this.o.lastSyncAt) return "";
    try {
      const at = await this.o.lastSyncAt();
      return at ? `The last sync of any spec was at ${at}.` : "No spec has synced to this relay.";
    } catch {
      return "";
    }
  }

  /**
   * The desk of a repo: by its path, or by its name when one desk has that
   * name. Null when the relay has no desk for the repo.
   */
  private async findDesk(repo: string): Promise<DeskState | null> {
    const desks = this.o.desks!;
    const byPath = await desks.get({ repoKey: repoKeyOf(repo) });
    if (byPath) return byPath;
    if (repo.includes("/")) return null;
    const named = (await desks.list()).filter((d) => d.memo.repo && repoName(d.memo.repo) === repo);
    return named.length === 1 ? desks.get({ repoKey: named[0].repoKey }) : null;
  }

  /** The attachments of a message, from their object keys. A string is the tool's error. */
  private async attachmentsOf(args: Record<string, unknown>, owner: AttachmentOwner, what: string): Promise<Attachment[] | string> {
    const keys = Array.isArray(args.attachments) ? args.attachments.filter((k): k is string => typeof k === "string") : [];
    const attachments: Attachment[] = [];
    if (keys.length > 0 && !this.o.attachments.enabled) return NO_BUCKET_TEXT;
    for (const key of keys) {
      const found = this.o.attachments.belongsTo(key, owner) ? await this.o.attachments.describe(key) : null;
      if (!found) return `The relay has no attachment ${key} for this ${what}. Upload it with upload_attachment.`;
      attachments.push(found);
    }
    return attachments;
  }

  private async send(args: Record<string, unknown>): Promise<ToolText> {
    const repo = str(args.repo);
    const slug = str(args.slug);
    const text = str(args.text);
    if (!slug && this.o.desks) {
      if (!repo || !text) return { ok: false, text: "Give the repo and the text." };
      return this.sendToDesk(repo, text, args);
    }
    if (!repo || !slug || !text) return { ok: false, text: "Give the repo, the slug and the text." };
    const spec: SpecAddress = { repoKey: repoKeyOf(repo), slug, repo };
    const attachments = await this.attachmentsOf(args, spec, "spec");
    if (typeof attachments === "string") return { ok: false, text: attachments };
    const { seq, message } = await this.o.specs.send(spec, {
      text,
      replyTo: str(args.replyTo),
      ...(attachments.length > 0 ? { attachments } : {}),
    });
    this.o.push(spec, message);
    return {
      ok: true,
      text: JSON.stringify({ seq, id: message.id, spec: { repo, slug }, createdAt: message.createdAt }),
    };
  }

  /** send_message with no slug: the message goes to the repo's desk. */
  private async sendToDesk(repo: string, text: string, args: Record<string, unknown>): Promise<ToolText> {
    const state = await this.findDesk(repo);
    if (!state) return { ok: false, text: noDeskText(repo) };
    if (!state.canMessage) throw new DeskRefusedError(state.reason);
    const desk: DeskAddress = state.desk;
    const attachments = await this.attachmentsOf(args, { ...desk, desk: true }, "desk");
    if (typeof attachments === "string") return { ok: false, text: attachments };
    const { seq, message } = await this.o.desks!.send(desk, {
      text,
      replyTo: str(args.replyTo),
      ...(attachments.length > 0 ? { attachments } : {}),
    });
    const pushed = this.o.pushDesk?.(desk, message) ?? false;
    const path = desk.repo ?? repo;
    const note = pushed
      ? `The message is queued for the desk of the repo ${path}.`
      : `The message is queued for the desk of the repo ${path}. It waits for the Mac, and the Mac takes it after it connects.`;
    return {
      ok: true,
      text: JSON.stringify({ seq, id: message.id, desk: { repo: path }, createdAt: message.createdAt, note }),
    };
  }

  private async read(args: Record<string, unknown>): Promise<ToolText> {
    const repo = str(args.repo);
    const slug = str(args.slug);
    if (!slug && repo && this.o.desks) return this.readDesk(repo, args);
    if (slug) {
      if (!repo) return { ok: false, text: "Give the repo with the slug." };
      const after = typeof args.after === "number" ? args.after : undefined;
      const spec = { repoKey: repoKeyOf(repo), slug };
      const result = await this.o.specs.read(spec, after, this.waitMs);
      const messages = await this.withUrls(result.messages.map((m) => shown(m, repo)));
      const cursor = messages.length > 0 ? messages[messages.length - 1].seq : (after ?? 0);
      return {
        ok: true,
        text: JSON.stringify({ spec: { repo, slug }, stage: result.stage, syncedAt: result.syncedAt, cursor, messages }, null, 2),
      };
    }
    return this.readAll(repo);
  }

  /** read_messages with a repo and no slug: the messages of the repo's desk after the cursor. */
  private async readDesk(repo: string, args: Record<string, unknown>): Promise<ToolText> {
    const state = await this.findDesk(repo);
    if (!state) return { ok: false, text: noDeskText(repo) };
    const after = typeof args.after === "number" ? args.after : undefined;
    const result = await this.o.desks!.read(state.desk, after, this.waitMs);
    const path = state.desk.repo ?? repo;
    const messages = await this.withUrls(result.messages.map((m) => shownDesk(m, path)));
    const cursor = messages.length > 0 ? messages[messages.length - 1].seq : (after ?? 0);
    return {
      ok: true,
      text: JSON.stringify({ desk: { repo: path }, stateAt: result.stateAt, cursor, messages }, null, 2),
    };
  }

  /** Add a signed download URL (15 minutes) to each attachment with an object key. */
  private async withUrls<T extends { attachments?: Attachment[] }>(messages: T[]): Promise<T[]> {
    if (!this.o.attachments.enabled) return messages;
    return Promise.all(
      messages.map(async (m) => {
        if (!m.attachments) return m;
        const attachments = await Promise.all(
          m.attachments.map(async (a) => {
            const url = a.objectKey ? await this.o.attachments.downloadUrl(a.objectKey).catch(() => null) : null;
            return url ? { ...a, url } : a;
          }),
        );
        return { ...m, attachments };
      }),
    );
  }

  private async upload(args: Record<string, unknown>): Promise<ToolText> {
    const repo = str(args.repo);
    const slug = str(args.slug);
    const filename = str(args.filename);
    const contentType = str(args.contentType);
    // Wrapped base64 (for example 76-character lines) is valid: drop the whitespace first.
    const base64 = str(args.base64)?.replace(/\s+/g, "");
    const toDesk = !slug && this.o.desks !== undefined;
    if (!repo || (!slug && !toDesk) || !filename || !contentType || !base64) {
      return {
        ok: false,
        text: toDesk
          ? "Give the repo, the filename, the contentType and the base64 contents."
          : "Give the repo, the slug, the filename, the contentType and the base64 contents.",
      };
    }
    if (!this.o.attachments.enabled) return { ok: false, text: NO_BUCKET_TEXT };
    const max = this.o.attachments.maxBytes;
    // Check the size before the decode: base64 is 4 characters for 3 bytes.
    const estimate = Math.floor((base64.replace(/=+$/, "").length * 3) / 4);
    if (estimate > max) return { ok: false, text: `The file is ${estimate} bytes. ${limitText(max)}` };
    if (!/^[A-Za-z0-9+/_-]+={0,2}$/.test(base64)) {
      return { ok: false, text: "The base64 contents are not valid." };
    }
    const body = Buffer.from(base64, "base64");
    if (body.length > max) return { ok: false, text: `The file is ${body.length} bytes. ${limitText(max)}` };
    let owner: AttachmentOwner;
    if (slug) {
      const spec: SpecAddress = { repoKey: repoKeyOf(repo), slug, repo };
      if (!(await this.o.specs.state(spec))) throw new SpecNotFoundError();
      owner = spec;
    } else {
      const state = await this.findDesk(repo);
      if (!state) return { ok: false, text: noDeskText(repo) };
      owner = { ...state.desk, desk: true };
    }
    const attachment = await this.o.attachments.upload(owner, filename, contentType, body);
    return { ok: true, text: JSON.stringify(attachment) };
  }

  /**
   * The unread messages of every spec of this deploy (PendingForCoordinator).
   * With no repo, the unread messages of every desk too.
   */
  private async readAll(repo: string | undefined): Promise<ToolText> {
    const end = Date.now() + this.waitMs;
    for (;;) {
      const listed = await this.o.specs.list({
        pendingForCoordinator: true,
        repoKey: repo ? repoKeyOf(repo) : undefined,
      });
      const specs: Array<{ repo: string | null; slug: string; stage: string; cursor: number }> = [];
      const desks: Array<{ repo: string | null; cursor: number }> = [];
      const messages: Array<ReturnType<typeof shown> | ReturnType<typeof shownDesk>> = [];
      for (const item of listed) {
        const result = await this.o.specs.read(item, undefined, 0, true).catch(() => null);
        if (!result || result.messages.length === 0) continue;
        const path = item.memo?.repo ?? undefined;
        messages.push(...result.messages.map((m) => shown(m, path)));
        specs.push({
          repo: path ?? null,
          slug: item.slug,
          stage: result.stage,
          cursor: result.messages[result.messages.length - 1].seq,
        });
      }
      if (!repo && this.o.desks) {
        for (const item of await this.o.desks.list({ pendingForCoordinator: true })) {
          const result = await this.o.desks.read(item, undefined, 0, true).catch(() => null);
          if (!result || result.messages.length === 0) continue;
          const path = item.memo.repo ?? undefined;
          messages.push(...result.messages.map((m) => shownDesk(m, path)));
          desks.push({ repo: path ?? null, cursor: result.messages[result.messages.length - 1].seq });
        }
      }
      if (messages.length > 0 || Date.now() >= end) {
        messages.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
        const body = this.o.desks ? { specs, desks, messages: await this.withUrls(messages) } : { specs, messages: await this.withUrls(messages) };
        return { ok: true, text: JSON.stringify(body, null, 2) };
      }
      await sleep(Math.min(POLL_MS, Math.max(0, end - Date.now())));
    }
  }
}
