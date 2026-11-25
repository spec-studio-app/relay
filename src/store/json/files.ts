// The file operations of the JSON store. A write goes to a temporary file in
// the same folder, then a rename replaces the old file, so a reader never sees
// a half-written file. A lock file (made whole with link, which fails when it
// exists) serializes the writes of one file, also between the relay and the
// `relay pair-code` command.
import { randomBytes } from "node:crypto";
import { link, mkdir, open, readFile, rename, rm, stat, unlink, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

/** A test hook: runs after the temporary file is written and before the rename. */
export interface WriteHooks {
  afterTempWrite?: () => void | Promise<void>;
}

/** The suffix of a temporary file. A start removes the ones that a crash left. */
export const TEMP_SUFFIX = ".tmp";

/** Write the file with write-and-rename, and flush the file and its folder. */
export async function writeAtomic(path: string, data: string, hooks: WriteHooks = {}): Promise<void> {
  const folder = dirname(path);
  await mkdir(folder, { recursive: true });
  const temp = `${path}.${process.pid}.${randomBytes(4).toString("hex")}${TEMP_SUFFIX}`;
  const handle = await open(temp, "w", 0o600);
  try {
    await handle.writeFile(data, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await hooks.afterTempWrite?.();
    await rename(temp, path);
  } catch (error) {
    await unlink(temp).catch(() => undefined);
    throw error;
  }
  // The rename is durable only when the folder is flushed too.
  const dir = await open(folder, "r");
  try {
    await dir.sync().catch(() => undefined);
  } finally {
    await dir.close();
  }
}

/** The file's text, or null when it does not exist. */
export async function readText(path: string): Promise<string | null> {
  try {
    return await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

/** Remove a file. No error when it does not exist. */
export async function removeFile(path: string): Promise<void> {
  await rm(path, { force: true });
}

export interface LockOptions {
  /** A lock older than this is stale (its holder crashed). */
  staleMs?: number;
  /** Wait between tries. */
  retryMs?: number;
  /** Give up after this time. */
  timeoutMs?: number;
}

export class LockTimeoutError extends Error {
  constructor(path: string) {
    super(`The relay could not lock ${path} in time.`);
  }
}

function alive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  if (pid === process.pid) return true;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM: the process exists but belongs to another user.
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

/**
 * The text of a stale lock: its holder is gone, or it is older than staleMs.
 * Null when the lock is live. A lock file without a valid pid is live until
 * staleMs (the lock is made whole with link, so this is only a damaged file).
 */
async function staleText(lock: string, staleMs: number): Promise<string | null> {
  try {
    const [text, info] = await Promise.all([readFile(lock, "utf8"), stat(lock)]);
    if (Date.now() - info.mtimeMs > staleMs) return text;
    const pid = Number(text.split(" ")[0]);
    if (!Number.isInteger(pid) || pid <= 0) return null;
    // A lock of this process is held by a call in its queue.
    return pid !== process.pid && !alive(pid) ? text : null;
  } catch (error) {
    // The lock went away between the tries: try again.
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

/**
 * Remove a stale lock, but never a new lock that another waiter made after
 * the check: move the file away, and put it back when its text is not the
 * stale text.
 */
async function removeStale(lock: string, text: string): Promise<void> {
  const aside = `${lock}.${process.pid}.${randomBytes(4).toString("hex")}.stale`;
  try {
    await rename(lock, aside);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  try {
    if ((await readFile(aside, "utf8")) !== text) await link(aside, lock).catch(() => undefined);
  } finally {
    await unlink(aside).catch(() => undefined);
  }
}

/** Make the lock file with its text in one step: link fails when the lock exists. */
async function tryLock(lock: string): Promise<boolean> {
  const temp = `${lock}.${process.pid}.${randomBytes(4).toString("hex")}${TEMP_SUFFIX}`;
  await writeFile(temp, `${process.pid} ${Date.now()} ${randomBytes(4).toString("hex")}`, { mode: 0o600 });
  try {
    await link(temp, lock);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
    throw error;
  } finally {
    await unlink(temp).catch(() => undefined);
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Run fn while this process holds the lock file of `path`. */
export async function withFileLock<T>(path: string, fn: () => Promise<T>, options: LockOptions = {}): Promise<T> {
  const lock = `${path}.lock`;
  const staleMs = options.staleMs ?? 30_000;
  const retryMs = options.retryMs ?? 20;
  const end = Date.now() + (options.timeoutMs ?? 10_000);
  await mkdir(dirname(path), { recursive: true });
  for (;;) {
    if (await tryLock(lock)) break;
    const stale = await staleText(lock, staleMs);
    if (stale !== null) {
      await removeStale(lock, stale);
      continue;
    }
    if (Date.now() > end) throw new LockTimeoutError(path);
    await sleep(retryMs);
  }
  try {
    return await fn();
  } finally {
    await unlink(lock).catch(() => undefined);
  }
}
