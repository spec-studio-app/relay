// Open the relay's store from the config: the JSON store (default), Temporal, or a backend plugin.
import type { Config } from "../config.js";
import type { Bucket } from "../bucket.js";
import type { Logger } from "../log.js";
import { openJsonStore } from "./json/index.js";
import type { RelayStore } from "./types.js";

export type { RelayStore, SpecStore, DeskStore, RelayConfigStore, ListedSpec, ListedDesk, SpecRef, StoreKind } from "./types.js";

/** Why the relay uses its store, for the log. */
export function storeReasonText(config: Pick<Config, "store" | "storeReason"> & { plugin?: Config["plugin"] }): string {
  if (config.storeReason === "plugin") return `${config.plugin?.from ?? "RELAY_PLUGIN"} is set`;
  if (config.storeReason === "set") return `RELAY_STORE is ${config.store}`;
  if (config.storeReason === "temporal-address") return "RELAY_STORE is not set and TEMPORAL_ADDRESS is set";
  return "RELAY_STORE is not set";
}

/**
 * Open the store. `worker: false` is for the `relay` commands: the Temporal
 * store then runs no worker (the running relay's worker applies the updates).
 */
export async function openStore(config: Config, deps: { bucket: Bucket | null; log: Logger; worker?: boolean }): Promise<RelayStore> {
  if (config.store === "plugin") {
    const { openPluginStore } = await import("./plugin/index.js");
    return openPluginStore(config, { bucket: deps.bucket, log: deps.log });
  }
  if (config.store === "temporal") {
    // Loaded only for the Temporal store, so a JSON relay never loads the Temporal SDK.
    const { openTemporalStore } = await import("./temporal.js");
    return openTemporalStore(config, deps);
  }
  return openJsonStore(config, { bucket: deps.bucket, log: deps.log });
}
