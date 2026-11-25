// The startup parts that both stores use: the startup error, the bucket check,
// the pairing text and a new pairing code. No Temporal import: the JSON store
// uses this file.
import { newPairCode, pairCodeHash, PAIR_CODE_TTL_MS } from "./pairing.js";
import type { Config } from "./config.js";
import type { Bucket } from "./bucket.js";
import type { Logger } from "./log.js";
import type { RelayConfigStore } from "./store/types.js";

export class StartupError extends Error {}

/** Make a new pairing code in a store (the command `relay pair-code`). */
export async function newPairCodeIn(store: RelayConfigStore, timeoutMs = 15_000): Promise<string> {
  const code = newPairCode();
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error("timeout")), timeoutMs);
  });
  try {
    await Promise.race([store.setPairCode({ hash: pairCodeHash(code), expiresAt: Date.now() + PAIR_CODE_TTL_MS }), timeout]);
  } finally {
    clearTimeout(timer);
  }
  return code;
}

/** The bucket check of both stores: no bucket, or a bucket that the relay can reach. */
export async function checkBucket(o: { bucket: Bucket | null; log: Logger }): Promise<void> {
  if (!o.bucket) {
    o.log({ event: "bucket_not_set" });
    return;
  }
  try {
    await o.bucket.check();
  } catch {
    throw new StartupError(
      "The relay cannot reach the bucket. Check BUCKET_ENDPOINT, BUCKET_NAME, BUCKET_ACCESS_KEY and BUCKET_SECRET_KEY.",
    );
  }
}

/** The text that tells the owner how to pair the Mac. */
export function pairingText(config: Pick<Config, "publicUrl">, code: string): string {
  const relayUrl = new URL("/agent", config.publicUrl);
  relayUrl.protocol = relayUrl.protocol === "http:" ? "ws:" : "wss:";
  return [
    `Pairing code: ${code}`,
    `It works once, for 24 hours. On the Mac, run:`,
    `  studio agent pair ${relayUrl.href} ${code}`,
    `Or enter the relay URL and the code in Spec Studio > Settings > Remote Relay.`,
    `For a new code, run: relay pair-code`,
  ].join("\n");
}
