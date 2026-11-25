// The JSON desk store: one file per desk in <data>/desks, with the same rules
// as the desk workflow (workflows/deskRules.ts). The relay process owns the
// files: it keeps every desk in memory, serializes the changes of each desk,
// and writes the file after each change. A desk lives as long as the relay.
import { createHash, randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { readdir } from "node:fs/promises";
import { join } from "node:path";
import type { Logger } from "../../log.js";
import {
  initialDeskState,
  type Attachment,
  type DeskAddress,
  type DeskMessage,
  type DeskReport,
  type DeskState,
} from "../../workflows/types.js";
import { MAX_WAIT_MS, hasNewer, readCursor, type RuleEnv } from "../../workflows/inboxRules.js";
import {
  applyDeskAck,
  applyDeskSend,
  applyDeskSync,
  applyReport,
  collectDeskRead,
  deskMemo,
  deskRefusal,
  matchesDeskFilter,
  type DeskListFilter,
  type DeskReadResult,
  type DeskSyncLine,
  type DeskSyncResult,
} from "../../workflows/deskRules.js";
import { DeskNotFoundError, DeskRefusedError } from "../errors.js";
import type { DeskStore, ListedDesk } from "../types.js";
import { TEMP_SUFFIX, readText, removeFile, writeAtomic, type WriteHooks } from "./files.js";

interface DeskFile {
  version: 1;
  state: DeskState;
}

interface Entry {
  state: DeskState;
  /** The tail of the desk's queue of calls. */
  queue: Promise<unknown>;
}

export interface JsonDeskStoreOptions {
  /** The data folder (RELAY_DATA_DIR). */
  dir: string;
  deployId: string;
  log: Logger;
  now?: () => Date;
  /** Test hooks for the file writes. */
  writeHooks?: WriteHooks;
}

/** The file name of a desk: a hash of its address. */
export function deskFileName(deployId: string, repoKey: string): string {
  return createHash("sha256").update(`desk/${deployId}/${repoKey}`, "utf8").digest("hex").slice(0, 32) + ".json";
}

/** A desk file that the relay cannot read or parse at start. The relay does not start. */
export class DeskFileError extends Error {
  constructor(file: string, what: "read" | "parse") {
    super(
      `The relay cannot ${what} the desk file ${file} in RELAY_DATA_DIR/desks. ` +
        (what === "read" ? "Give the relay's user (uid 1000) access to it, " : "Correct the file, ") +
        "or remove it. A removed desk comes back with the next desks frame of the Mac, without the messages that waited in it.",
    );
  }
}

export class JsonDeskStore implements DeskStore {
  private readonly entries = new Map<string, Entry>();
  /** Emits the repo key of a desk when it gets a new message for Claude AI. */
  private readonly wake = new EventEmitter();
  private readonly env: RuleEnv;

  constructor(private readonly o: JsonDeskStoreOptions) {
    this.env = { newId: () => randomUUID(), now: () => this.nowDate().toISOString() };
    this.wake.setMaxListeners(0);
  }

  private nowDate(): Date {
    return this.o.now?.() ?? new Date();
  }

  private get desksDir(): string {
    return join(this.o.dir, "desks");
  }

  private pathOf(repoKey: string): string {
    return join(this.desksDir, deskFileName(this.o.deployId, repoKey));
  }

  /**
   * Read the desk files. A file that the relay cannot read or parse stops the
   * start (DeskFileError), as a spec file does: the owner fixes it, so no
   * desk message is lost or made again in silence.
   */
  async load(): Promise<void> {
    let names: string[] = [];
    try {
      names = await readdir(this.desksDir);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
    for (const name of names) {
      const path = join(this.desksDir, name);
      if (name.endsWith(TEMP_SUFFIX)) {
        await removeFile(path);
        continue;
      }
      if (!name.endsWith(".json")) continue;
      let text: string | null;
      try {
        text = await readText(path);
      } catch {
        this.o.log({ event: "desk_file_read_failed", file: name });
        throw new DeskFileError(name, "read");
      }
      let file: DeskFile;
      try {
        file = JSON.parse(text ?? "") as DeskFile;
        if (file?.version !== 1 || !file.state?.desk?.repoKey) throw new Error("not a desk file");
      } catch {
        this.o.log({ event: "desk_file_unreadable", file: name });
        throw new DeskFileError(name, "parse");
      }
      if (file.state.deployId !== this.o.deployId) continue;
      this.entries.set(file.state.desk.repoKey, { state: file.state, queue: Promise.resolve() });
    }
  }

  private queued<T>(entry: Entry, fn: () => Promise<T>): Promise<T> {
    const run = entry.queue.then(fn, fn);
    entry.queue = run.catch(() => undefined);
    return run;
  }

  private entryOf(desk: { repoKey: string }): Entry {
    const entry = this.entries.get(desk.repoKey);
    if (!entry) throw new DeskNotFoundError();
    return entry;
  }

  /** The desk's entry; a new one when the store has none (it is kept after its first write). */
  private entryFor(desk: DeskAddress): { entry: Entry; created: boolean } {
    const found = this.entries.get(desk.repoKey);
    if (found) return { entry: found, created: false };
    const entry: Entry = { state: initialDeskState(this.o.deployId, desk), queue: Promise.resolve() };
    this.entries.set(desk.repoKey, entry);
    return { entry, created: true };
  }

  /**
   * Apply a change to a desk: write the file when the rule changed the state,
   * and wake the reads when a message for Claude AI came. A failed write
   * leaves the state as it was.
   */
  private async change<T>(entry: Entry, fn: (s: DeskState) => { result: T; changed: boolean }): Promise<T> {
    return this.queued(entry, async () => {
      const draft = structuredClone(entry.state);
      const before = draft.coordinator.nextSeq;
      const out = fn(draft);
      if (out.changed) {
        const file: DeskFile = { version: 1, state: draft };
        await writeAtomic(this.pathOf(draft.desk.repoKey), JSON.stringify(file), this.o.writeHooks);
        entry.state = draft;
        // A new desk whose first call failed was dropped meanwhile: this write keeps it.
        if (!this.entries.has(draft.desk.repoKey)) this.entries.set(draft.desk.repoKey, entry);
      }
      if (entry.state.coordinator.nextSeq !== before) this.wake.emit(entry.state.desk.repoKey);
      return out.result;
    });
  }

  /** A change that may make the desk: a new desk whose first write failed is not kept. */
  private async changeOrMake<T>(desk: DeskAddress, fn: (s: DeskState) => { result: T; changed: boolean }): Promise<T> {
    const { entry, created } = this.entryFor(desk);
    try {
      return await this.change(entry, fn);
    } catch (error) {
      if (created && this.entries.get(desk.repoKey) === entry && entry.state.stateAt === null && entry.state.line === 0) {
        // The desk has no file yet (no write succeeded): do not keep it.
        this.entries.delete(desk.repoKey);
      }
      throw error;
    }
  }

  async report(report: DeskReport): Promise<void> {
    const { repoKey, ...rest } = report;
    await this.changeOrMake({ repoKey, repo: report.repo }, (s) => applyReport(s, { ...rest, at: this.env.now() }));
  }

  async sync(desk: DeskAddress, lines: DeskSyncLine[]): Promise<DeskSyncResult> {
    return this.changeOrMake(desk, (s) => applyDeskSync(s, { lines, at: this.env.now(), ...(desk.repo ? { repo: desk.repo } : {}) }, this.env));
  }

  async get(desk: { repoKey: string }): Promise<DeskState | null> {
    const entry = this.entries.get(desk.repoKey);
    return entry ? structuredClone(entry.state) : null;
  }

  async send(
    desk: { repoKey: string },
    input: { text: string; replyTo?: string; attachments?: Attachment[] },
  ): Promise<{ seq: number; message: DeskMessage }> {
    const entry = this.entryOf(desk);
    return this.change(entry, (s) => {
      const refusal = deskRefusal(s);
      if (refusal) throw new DeskRefusedError(refusal);
      return { result: applyDeskSend(s, { ...input, id: randomUUID(), at: this.env.now() }), changed: true };
    });
  }

  async ack(desk: { repoKey: string }, seq: number, error?: string): Promise<{ removed: boolean }> {
    const entry = this.entryOf(desk);
    return this.change(entry, (s) => applyDeskAck(s, { seq, ...(error ? { error } : {}) }, this.env));
  }

  async read(desk: { repoKey: string }, after: number | undefined, waitMs: number, unreadOnly = false): Promise<DeskReadResult> {
    const entry = this.entryOf(desk);
    const key = desk.repoKey;
    const cursor = await this.queued(entry, async () => readCursor(entry.state, { after, unreadOnly }));
    if (!hasNewer(entry.state, cursor) && waitMs > 0) {
      // Wait outside the queue, so syncs and sends go on meanwhile.
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer);
          this.wake.off(key, onWake);
          resolve();
        };
        const onWake = () => {
          if (hasNewer(entry.state, cursor)) done();
        };
        const timer = setTimeout(done, Math.min(waitMs, MAX_WAIT_MS));
        this.wake.on(key, onWake);
      });
    }
    return this.change(entry, (s) => collectDeskRead(s, cursor));
  }

  async list(filter: DeskListFilter = {}): Promise<ListedDesk[]> {
    const out: ListedDesk[] = [];
    for (const [repoKey, entry] of this.entries) {
      const memo = deskMemo(entry.state);
      if (matchesDeskFilter(repoKey, memo, filter)) out.push({ repoKey, memo });
    }
    return out;
  }
}
