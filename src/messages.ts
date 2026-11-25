// The tools send_message and read_messages. They use the spec store, with the
// Mac online or offline.
import { repoKeyOf } from "./store/address.js";
import { SpecClosingError, SpecDroppedError, SpecNotFoundError } from "./store/errors.js";
import type { SpecStore } from "./store/types.js";
import type { ToolText } from "./offline.js";
import type { Attachment, Message, SpecAddress } from "./workflows/types.js";
import { limitText, NO_BUCKET_TEXT, type Attachments } from "./attachments.js";

export const READ_WAIT_MS = 20_000;
const POLL_MS = 1000;

export interface MessageToolsOptions {
  specs: SpecStore;
  /** Push a new message to the Mac when the agent is connected. */
  push: (spec: SpecAddress, message: Message) => void;
  attachments: Attachments;
  /** How long read_messages waits for a new message (tests set less). */
  waitMs?: number;
}

function str(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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
        return { ok: false, text: "The relay has no synced spec with this repo and slug. Check them with list_specs." };
      }
      return { ok: false, text: "The relay cannot reach its store now. Try again later." };
    }
  }

  private async send(args: Record<string, unknown>): Promise<ToolText> {
    const repo = str(args.repo);
    const slug = str(args.slug);
    const text = str(args.text);
    if (!repo || !slug || !text) return { ok: false, text: "Give the repo, the slug and the text." };
    const spec: SpecAddress = { repoKey: repoKeyOf(repo), slug, repo };
    const keys = Array.isArray(args.attachments) ? args.attachments.filter((k): k is string => typeof k === "string") : [];
    const attachments: Attachment[] = [];
    if (keys.length > 0 && !this.o.attachments.enabled) return { ok: false, text: NO_BUCKET_TEXT };
    for (const key of keys) {
      const found = this.o.attachments.belongsTo(key, spec) ? await this.o.attachments.describe(key) : null;
      if (!found) {
        return { ok: false, text: `The relay has no attachment ${key} for this spec. Upload it with upload_attachment.` };
      }
      attachments.push(found);
    }
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

  private async read(args: Record<string, unknown>): Promise<ToolText> {
    const repo = str(args.repo);
    const slug = str(args.slug);
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
    if (!repo || !slug || !filename || !contentType || !base64) {
      return { ok: false, text: "Give the repo, the slug, the filename, the contentType and the base64 contents." };
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
    const spec: SpecAddress = { repoKey: repoKeyOf(repo), slug, repo };
    if (!(await this.o.specs.state(spec))) throw new SpecNotFoundError();
    const attachment = await this.o.attachments.upload(spec, filename, contentType, body);
    return { ok: true, text: JSON.stringify(attachment) };
  }

  /** The unread messages of every spec of this deploy (PendingForCoordinator). */
  private async readAll(repo: string | undefined): Promise<ToolText> {
    const end = Date.now() + this.waitMs;
    for (;;) {
      const listed = await this.o.specs.list({
        pendingForCoordinator: true,
        repoKey: repo ? repoKeyOf(repo) : undefined,
      });
      const specs: Array<{ repo: string | null; slug: string; stage: string; cursor: number }> = [];
      const messages: ReturnType<typeof shown>[] = [];
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
      if (messages.length > 0 || Date.now() >= end) {
        messages.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
        return { ok: true, text: JSON.stringify({ specs, messages: await this.withUrls(messages) }, null, 2) };
      }
      await sleep(Math.min(POLL_MS, Math.max(0, end - Date.now())));
    }
  }
}
