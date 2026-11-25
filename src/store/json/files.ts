// The file operations of the JSON store. A write goes to a temporary file in
// the same folder, then a rename replaces the old file, so a reader never sees
// a half-written file. Each file has one writer process (the relay, or
// `relay pair-code` for pair-code.json), so no lock between processes is needed.
import { randomBytes } from "node:crypto";
import { mkdir, open, readFile, rename, rm, unlink } from "node:fs/promises";
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
