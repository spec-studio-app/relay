// The activities of the spec workflow. They run in the relay's worker.
import type { Bucket } from "./bucket.js";
import type { Logger } from "./log.js";

export interface Activities {
  /** Remove every attachment of a spec from the bucket. */
  removeAttachments(input: { deployId: string; repoKey: string; slug: string }): Promise<number>;
}

/** The object key prefix of a spec's attachments. */
export function attachmentPrefix(deployId: string, repoKey: string, slug: string): string {
  return `${deployId}/${repoKey}/${slug}/`;
}

/**
 * The segment of a desk in an object key, in place of the slug. A slug starts
 * with a letter or a digit, so no slug can be this segment.
 */
export const DESK_SEGMENT = "_desk";

/** The object key prefix of a desk's attachments. */
export function deskAttachmentPrefix(deployId: string, repoKey: string): string {
  return `${deployId}/${repoKey}/${DESK_SEGMENT}/`;
}

export function createActivities(deps: { bucket: Bucket | null; log: Logger }): Activities {
  return {
    async removeAttachments({ deployId, repoKey, slug }) {
      if (!deps.bucket) return 0;
      const removed = await deps.bucket.removePrefix(attachmentPrefix(deployId, repoKey, slug));
      deps.log({ event: "attachments_removed", count: removed });
      return removed;
    },
  };
}
