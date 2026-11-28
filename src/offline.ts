// Answers for list_repos, list_specs and read_spec while the Mac is offline.
// They come from the store: the repos from the agent's last hello, the list of
// specs with their summaries, and the state of one spec. Each answer says how
// old it is.
import type { DeskStore, RelayConfigStore, SpecStore } from "./store/types.js";
import { repoKeyOf } from "./store/address.js";
import { StoreUnavailableError } from "./store/errors.js";
import { ARCHIVED_REPO_TEXT } from "./messages.js";
import { DESK_GONE_TEXT, EVENT_LIMIT, effectiveTurn, mirrorFields, pauseOf, type Turn } from "./workflows/types.js";

export interface ToolText {
  ok: boolean;
  text: string;
}

export const NO_SPEC_TEXT =
  "The spec.md and plan.md text are not available while the Mac is offline. Read the spec again when the Mac is online.";

export interface OfflineReader {
  answer(tool: string, args: Record<string, unknown>, offlineText: string): Promise<ToolText>;
  /** The time of the newest sync of any spec (ISO 8601), or null when none. */
  lastSyncAt?(): Promise<string | null>;
}

/** The newest syncedAt of the store's specs (ISO 8601), or null when none. */
export async function lastSyncAt(specs: Pick<SpecStore, "list">): Promise<string | null> {
  const listed = await specs.list({ all: true });
  const times = listed.map((s) => s.memo?.syncedAt).filter((t): t is string => !!t).sort();
  return times.length > 0 ? times[times.length - 1] : null;
}

export const OFFLINE_READ_TOOLS = new Set(["list_repos", "list_specs", "read_spec"]);

function str(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

/** " The spec is paused" with the note and the time, or "" when the spec is not paused or is merged. */
function pausedSummary(state: Record<string, unknown> | null): string {
  const paused = pauseOf(state);
  if (!paused) return "";
  return ` The spec is paused${paused.note ? `: ${paused.note}` : ""}.${paused.at ? ` Paused at ${paused.at}.` : ""}`;
}

export class OfflineTools implements OfflineReader {
  constructor(private readonly deps: { specs: SpecStore; config: RelayConfigStore; desks?: DeskStore }) {}

  lastSyncAt(): Promise<string | null> {
    return lastSyncAt(this.deps.specs);
  }

  async answer(tool: string, args: Record<string, unknown>, offlineText: string): Promise<ToolText> {
    try {
      if (tool === "list_repos") return await this.listRepos(args, offlineText);
      if (tool === "list_specs") return await this.listSpecs(args, offlineText);
      if (tool === "read_spec") return await this.readSpec(args, offlineText);
    } catch (error) {
      if (error instanceof StoreUnavailableError) return { ok: false, text: `${offlineText} ${error.message}` };
      return { ok: false, text: `${offlineText} The relay cannot read its copy of the specs now.` };
    }
    return { ok: false, text: offlineText };
  }

  private async listRepos(args: Record<string, unknown>, offlineText: string): Promise<ToolText> {
    const repos = await this.deps.config.repos();
    if (!repos) return { ok: false, text: `${offlineText} The relay has no list of repos from the Mac.` };
    // The relay does not know the area of a repo, so it cannot filter by it.
    const notice = str(args.area)
      ? " The relay does not know the area of a repo, so this list shows all repos. The area filter needs the Mac online."
      : "";
    let archivedNotice = "";
    let archivedList = "";
    if (args.archived === true) {
      // The Mac leaves archived repos out of the hello list. It sends them in their own list.
      const archived = await this.deps.config.archivedRepos();
      if (archived) archivedList = `\nThe archived repos, from the list that the Mac sent at ${archived.at}:\n${JSON.stringify(archived.repos)}`;
      else archivedNotice = " The Mac sent no list of archived repos, so the relay cannot show them. The archived filter needs the Mac online.";
    }
    const text =
      `${offlineText} This list of repos is from ${repos.at}.${notice}${archivedNotice}\n${JSON.stringify(repos.repos)}` + archivedList;
    if (!this.deps.desks) return { ok: true, text };
    // The desk of each repo, as the Mac last sent it, with the time of that state.
    // Only the desks of the repos in the list: a desk record stays after its repo moves or leaves.
    // With archived, the desks of the archived repos too (each one says that the repo is archived).
    const keys = new Set(repos.repos.map((path) => repoKeyOf(path)));
    if (args.archived === true) for (const key of await this.archivedKeys()) keys.add(key);
    const desks = (await this.deps.desks.list())
      .filter((d) => keys.has(d.repoKey) && d.memo.reason !== DESK_GONE_TEXT)
      .map((d) => ({
        repo: d.memo.repo,
        open: d.memo.open,
        turn: d.memo.turn,
        canMessage: d.memo.canMessage,
        reason: d.memo.reason,
        stateAt: d.memo.stateAt,
      }))
      .sort((a, b) => (a.repo ?? "").localeCompare(b.repo ?? ""));
    return {
      ok: true,
      text: `${text}\nThe desk of each repo, from the last state that the Mac sent (stateAt):\n${JSON.stringify(desks)}`,
    };
  }

  /** The repo keys of the archived repos that the Mac sent last. Empty when it sent no list. */
  private async archivedKeys(): Promise<Set<string>> {
    const archived = await this.deps.config.archivedRepos();
    return new Set((archived?.repos ?? []).map((path) => repoKeyOf(path)));
  }

  /** The turn of a spec of an archived repo, from its mirrored state. Without the state, the memo's turn with needsYou false. */
  private async archivedTurn(repoKey: string, slug: string, memoTurn: Turn | null): Promise<Turn | null> {
    const state = await this.deps.specs.state({ repoKey, slug });
    if (state) return effectiveTurn(state.turn, state.state, true);
    return memoTurn ? { turn: memoTurn.turn, needsYou: false } : { turn: "archived", needsYou: false };
  }

  private async listSpecs(args: Record<string, unknown>, offlineText: string): Promise<ToolText> {
    const repo = str(args.repo);
    const all = args.all === true;
    // Like `studio specs`: a merged spec shows only with all.
    const archived = args.archived === true;
    const listed = await this.deps.specs.list({ repoKey: repo ? repoKeyOf(repo) : undefined, all, openOnly: !all });
    // Only the repos that the Mac says are archived. With no list (an older Mac), nothing is filtered.
    const archivedKeys = await this.archivedKeys();
    const isArchived = (repoKey: string) => archivedKeys.has(repoKey);
    const shown = listed
      .filter((s) => s.memo !== null && (all || (s.memo.stage !== "merged" && !s.memo.dropped)))
      .filter((s) => archived || !isArchived(s.repoKey));
    // The memo's turn does not know the archived list: a spec of an archived repo never needs the user.
    const turns = await Promise.all(
      shown.map(async (s) => (isArchived(s.repoKey) ? this.archivedTurn(s.repoKey, s.slug, s.memo!.turn) : s.memo!.turn)),
    );
    const specs = shown
      .map((s, i) => ({
        repo: s.memo!.repo,
        slug: s.slug,
        title: s.memo!.title,
        mode: s.memo!.mode,
        stage: s.memo!.stage,
        status: s.memo!.status,
        note: s.memo!.note,
        turn: turns[i]?.turn ?? null,
        needsYou: turns[i]?.needsYou ?? false,
        syncedAt: s.memo!.syncedAt,
        messagesForMac: s.memo!.waitingForMac,
        unreadMessages: s.memo!.unreadForCoordinator,
        ...(s.memo!.dropped ? { dropped: true } : {}),
        ...(s.memo!.paused ? { paused: s.memo!.paused } : {}),
        ...(isArchived(s.repoKey) ? { archived: true } : {}),
      }));
    const times = specs.map((s) => s.syncedAt).filter((t): t is string => !!t).sort();
    const asOf = times.length > 0 ? times[times.length - 1] : null;
    // The mirror has no areas and no collections, so it cannot filter by them.
    // project is the older name of collection.
    const filters = [str(args.area) && "area", (str(args.collection) || str(args.project)) && "collection"].filter(Boolean);
    const notice =
      filters.length > 0
        ? ` The relay does not know the ${filters.join(" and the ")} of a spec, so this list shows all specs. ` +
          `The ${filters.join(" and ")} filter needs the Mac online.`
        : "";
    const head =
      `${offlineText} The list comes from the relay's copy of each spec. ` +
      `Each spec shows the time of its last sync (syncedAt).${notice}`;
    return { ok: true, text: `${head}\n${JSON.stringify({ asOf, specs }, null, 2)}` };
  }

  private async readSpec(args: Record<string, unknown>, offlineText: string): Promise<ToolText> {
    const repo = str(args.repo);
    const slug = str(args.slug);
    if (!repo || !slug) return { ok: false, text: "Give the repo and the slug of the spec." };
    const state = await this.deps.specs.state({ repoKey: repoKeyOf(repo), slug });
    if (!state) {
      return { ok: false, text: `${offlineText} The relay has no copy of the spec ${slug} in this repo.` };
    }
    const fields = mirrorFields(state.state);
    const waiting = state.mac.messages.length;
    const archived = (await this.archivedKeys()).has(repoKeyOf(repo));
    const turn = effectiveTurn(state.turn, state.state, archived);
    const parts = [
      `${offlineText} This copy of the spec is from its last sync at ${state.syncedAt ?? "an unknown time"}.`,
      NO_SPEC_TEXT,
      "## State\n\n```json\n" + JSON.stringify(state.state ?? {}, null, 2) + "\n```",
      `## Turn\n\n${turn ? `${turn.turn}. Needs the user: ${turn.needsYou ? "yes" : "no"}.` : "Not known."}`,
      `## Summary\n\nStage: ${fields.stage || "unknown"}. Status: ${fields.status || "unknown"}.` +
        (fields.note ? ` Note: ${fields.note}` : "") +
        pausedSummary(state.state) +
        (archived ? ` ${ARCHIVED_REPO_TEXT}` : "") +
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
