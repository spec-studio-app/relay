// Answers for list_repos, list_specs and read_spec while the Mac is offline.
// They come from Temporal: the repos from the agent's last hello, the list from
// visibility (search attributes and memo, scoped by RelayDeployId), and one
// spec from a query of its workflow. Each answer says how old it is.
import type { ConfigStore } from "./configStore.js";
import { repoKeyOf, type Mirror } from "./mirror.js";
import { EVENT_LIMIT, effectiveTurn, mirrorFields } from "./workflows/types.js";

export interface ToolText {
  ok: boolean;
  text: string;
}

export const NO_SPEC_TEXT =
  "The spec.md and plan.md text are not available while the Mac is offline. Read the spec again when the Mac is online.";

export interface OfflineReader {
  answer(tool: string, args: Record<string, unknown>, offlineText: string): Promise<ToolText>;
}

export const OFFLINE_READ_TOOLS = new Set(["list_repos", "list_specs", "read_spec"]);

function str(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

export class OfflineTools implements OfflineReader {
  constructor(private readonly deps: { mirror: Mirror; configStore: ConfigStore }) {}

  async answer(tool: string, args: Record<string, unknown>, offlineText: string): Promise<ToolText> {
    try {
      if (tool === "list_repos") return await this.listRepos(offlineText);
      if (tool === "list_specs") return await this.listSpecs(args, offlineText);
      if (tool === "read_spec") return await this.readSpec(args, offlineText);
    } catch {
      return { ok: false, text: `${offlineText} The relay cannot read its copy of the specs now.` };
    }
    return { ok: false, text: offlineText };
  }

  private async listRepos(offlineText: string): Promise<ToolText> {
    const repos = await this.deps.configStore.repos();
    if (!repos) return { ok: false, text: `${offlineText} The relay has no list of repos from the Mac.` };
    return {
      ok: true,
      text: `${offlineText} This list of repos is from ${repos.at}.\n${JSON.stringify(repos.repos)}`,
    };
  }

  private async listSpecs(args: Record<string, unknown>, offlineText: string): Promise<ToolText> {
    const repo = str(args.repo);
    const all = args.all === true;
    // Like `studio specs`: a merged spec shows only with all.
    const listed = await this.deps.mirror.list({ repoKey: repo ? repoKeyOf(repo) : undefined, all, openOnly: !all });
    const specs = listed
      .filter((s) => s.memo !== null && (all || (s.memo.stage !== "merged" && !s.memo.dropped)))
      .map((s) => ({
        repo: s.memo!.repo,
        slug: s.slug,
        title: s.memo!.title,
        mode: s.memo!.mode,
        stage: s.memo!.stage,
        status: s.memo!.status,
        note: s.memo!.note,
        turn: s.memo!.turn?.turn ?? null,
        needsYou: s.memo!.turn?.needsYou ?? false,
        syncedAt: s.memo!.syncedAt,
        messagesForMac: s.memo!.waitingForMac,
        unreadMessages: s.memo!.unreadForCoordinator,
        ...(s.memo!.dropped ? { dropped: true } : {}),
      }));
    const times = specs.map((s) => s.syncedAt).filter((t): t is string => !!t).sort();
    const asOf = times.length > 0 ? times[times.length - 1] : null;
    const head =
      `${offlineText} The list comes from the relay's copy of each spec. ` +
      `Each spec shows the time of its last sync (syncedAt).`;
    return { ok: true, text: `${head}\n${JSON.stringify({ asOf, specs }, null, 2)}` };
  }

  private async readSpec(args: Record<string, unknown>, offlineText: string): Promise<ToolText> {
    const repo = str(args.repo);
    const slug = str(args.slug);
    if (!repo || !slug) return { ok: false, text: "Give the repo and the slug of the spec." };
    const state = await this.deps.mirror.state({ repoKey: repoKeyOf(repo), slug });
    if (!state) {
      return { ok: false, text: `${offlineText} The relay has no copy of the spec ${slug} in this repo.` };
    }
    const fields = mirrorFields(state.state);
    const waiting = state.mac.messages.length;
    const turn = effectiveTurn(state.turn, state.state);
    const parts = [
      `${offlineText} This copy of the spec is from its last sync at ${state.syncedAt ?? "an unknown time"}.`,
      NO_SPEC_TEXT,
      "## State\n\n```json\n" + JSON.stringify(state.state ?? {}, null, 2) + "\n```",
      `## Turn\n\n${turn ? `${turn.turn}. Needs the user: ${turn.needsYou ? "yes" : "no"}.` : "Not known."}`,
      `## Summary\n\nStage: ${fields.stage || "unknown"}. Status: ${fields.status || "unknown"}.` +
        (fields.note ? ` Note: ${fields.note}` : "") +
        (state.dropped ? " The spec no longer exists on the Mac." : ""),
      `## Messages\n\n${waiting === 1 ? "1 message waits" : `${waiting} messages wait`} for the Mac.`,
      `## The last ${EVENT_LIMIT} activity events\n\n` +
        "```jsonl\n" +
        state.events.map((e) => JSON.stringify(e.event)).join("\n") +
        "\n```",
    ];
    return { ok: true, text: parts.join("\n\n") };
  }
}
