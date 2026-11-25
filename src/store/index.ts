// Open the relay's store from the config: the JSON store (default) or Temporal.
import type { Config } from "../config.js";
import type { Bucket } from "../bucket.js";
import type { Logger } from "../log.js";
import { openJsonStore } from "./json/index.js";
import { openTemporalStore } from "./temporal.js";
import type { RelayStore } from "./types.js";

export type { RelayStore, SpecStore, RelayConfigStore, ListedSpec, SpecRef, StoreKind } from "./types.js";

/** Why the relay uses its store, for the log. */
export function storeReasonText(config: Pick<Config, "store" | "storeReason">): string {
  if (config.storeReason === "set") return `RELAY_STORE is ${config.store}`;
  if (config.storeReason === "temporal-address") return "RELAY_STORE is not set and TEMPORAL_ADDRESS is set";
  return "RELAY_STORE is not set";
}

/**
 * Open the store. `worker: false` is for the `relay` commands: the Temporal
 * store then runs no worker (the running relay's worker applies the updates).
 */
export async function openStore(config: Config, deps: { bucket: Bucket | null; log: Logger; worker?: boolean }): Promise<RelayStore> {
  if (config.store === "temporal") return openTemporalStore(config, deps);
  return openJsonStore(config, { bucket: deps.bucket, log: deps.log });
}
