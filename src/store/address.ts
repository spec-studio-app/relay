// The address of a spec: its repo key (a hash of the home repo's path) and slug.
import { createHash } from "node:crypto";
import type { SpecAddress } from "../workflows/types.js";

/** The first 12 hex characters of the SHA-256 of the home repo's path. */
export function repoKeyOf(repoPath: string): string {
  const path = repoPath.length > 1 ? repoPath.replace(/\/+$/, "") : repoPath;
  return createHash("sha256").update(path, "utf8").digest("hex").slice(0, 12);
}

export function specAddress(repo: string, slug: string): SpecAddress {
  return { repoKey: repoKeyOf(repo), slug, repo };
}
