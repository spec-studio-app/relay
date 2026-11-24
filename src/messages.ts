// The tools send_message and read_messages. They use the spec workflows, with
// the Mac online or offline.
import { repoKeyOf, SpecNotFoundError, type Mirror } from "./mirror.js";
import type { ToolText } from "./offline.js";
import type { Message, SpecAddress } from "./workflows/types.js";

export const READ_WAIT_MS = 20_000;
const POLL_MS = 1000;

export interface MessageToolsOptions {
  mirror: Mirror;
  /** Push a new message to the Mac when the agent is connected. */
  push: (spec: SpecAddress, message: Message) => void;
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
      return { ok: false, text: `The relay has no tool ${tool}.` };
    } catch (error) {
      if (error instanceof SpecNotFoundError) {
        return { ok: false, text: "The relay has no synced spec with this repo and slug. Check them with list_specs." };
      }
      return { ok: false, text: "The relay cannot reach Temporal now. Try again later." };
    }
  }

  private async send(args: Record<string, unknown>): Promise<ToolText> {
    const repo = str(args.repo);
    const slug = str(args.slug);
    const text = str(args.text);
    if (!repo || !slug || !text) return { ok: false, text: "Give the repo, the slug and the text." };
    const spec: SpecAddress = { repoKey: repoKeyOf(repo), slug, repo };
    const { seq, message } = await this.o.mirror.send(spec, { text, replyTo: str(args.replyTo) });
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
      const result = await this.o.mirror.read(spec, after, this.waitMs);
      const messages = result.messages.map((m) => shown(m, repo));
      const cursor = messages.length > 0 ? messages[messages.length - 1].seq : (after ?? 0);
      return {
        ok: true,
        text: JSON.stringify({ spec: { repo, slug }, stage: result.stage, syncedAt: result.syncedAt, cursor, messages }, null, 2),
      };
    }
    return this.readAll(repo);
  }

  /** The unread messages of every spec of this deploy (PendingForCoordinator). */
  private async readAll(repo: string | undefined): Promise<ToolText> {
    const end = Date.now() + this.waitMs;
    for (;;) {
      const listed = await this.o.mirror.list({
        pendingForCoordinator: true,
        repoKey: repo ? repoKeyOf(repo) : undefined,
      });
      const specs: Array<{ repo: string | null; slug: string; stage: string; cursor: number }> = [];
      const messages: ReturnType<typeof shown>[] = [];
      for (const item of listed) {
        const result = await this.o.mirror.read(item, undefined, 0, true).catch(() => null);
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
        return { ok: true, text: JSON.stringify({ specs, messages }, null, 2) };
      }
      await sleep(Math.min(POLL_MS, Math.max(0, end - Date.now())));
    }
  }
}
