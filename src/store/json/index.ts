// The JSON store: the relay's state in files on a volume (RELAY_DATA_DIR).
// One relay process owns the folder: run one replica.
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import type { Config } from "../../config.js";
import type { Bucket } from "../../bucket.js";
import type { Logger } from "../../log.js";
import { createActivities } from "../../activities.js";
import { newPairCode, pairCodeHash, PAIR_CODE_TTL_MS } from "../../pairing.js";
import { checkBucket, pairingText, StartupError } from "../../startupCommon.js";
import type { RelayStore } from "../types.js";
import { removeFile, writeAtomic, type WriteHooks } from "./files.js";
import { ConfigFileError, JsonConfigStore } from "./jsonConfigStore.js";
import { JsonSpecStore, SpecFileError } from "./jsonSpecStore.js";
import { DeskFileError, JsonDeskStore } from "./jsonDeskStore.js";

export { JsonConfigStore } from "./jsonConfigStore.js";
export { JsonSpecStore } from "./jsonSpecStore.js";
export { JsonDeskStore } from "./jsonDeskStore.js";

export function dataDirText(dir: string): string {
  return (
    `The relay cannot write to RELAY_DATA_DIR (${dir}). ` +
    `Give the relay's user (uid 1000) write access to the folder, or set RELAY_DATA_DIR to a folder that it can write.`
  );
}

export interface JsonStoreOptions {
  bucket: Bucket | null;
  log: Logger;
  /** How long a finished spec stays after its last call (tests set less). */
  graceMs?: number;
  writeHooks?: WriteHooks;
}

export interface JsonRelayStore extends RelayStore {
  kind: "json";
  specs: JsonSpecStore;
  desks: JsonDeskStore;
  config: JsonConfigStore;
}

export function openJsonStore(config: Pick<Config, "dataDir" | "deployId" | "publicUrl">, o: JsonStoreOptions): JsonRelayStore {
  const dir = config.dataDir;
  const activities = createActivities({ bucket: o.bucket, log: o.log });
  const specs = new JsonSpecStore({
    dir,
    deployId: config.deployId,
    log: o.log,
    removeAttachments: (input) => activities.removeAttachments(input),
    graceMs: o.graceMs,
    writeHooks: o.writeHooks,
  });
  const desks = new JsonDeskStore({ dir, deployId: config.deployId, log: o.log, writeHooks: o.writeHooks });
  const configStore = new JsonConfigStore({ dir, deployId: config.deployId, writeHooks: o.writeHooks });
  return {
    kind: "json",
    specs,
    desks,
    config: configStore,
    async start({ log, print }) {
      // The folder must take a write: a missing volume or a wrong owner stops the start.
      try {
        await mkdir(join(dir, "specs"), { recursive: true });
        await mkdir(join(dir, "desks"), { recursive: true });
        const probe = join(dir, `.probe-${process.pid}`);
        await writeAtomic(probe, "ok");
        await removeFile(probe);
      } catch {
        throw new StartupError(dataDirText(dir));
      }
      await checkBucket({ bucket: o.bucket, log });
      // The specs load first: a bad spec file stops the start before a new
      // config makes a pairing code that would never be printed.
      try {
        await specs.load();
      } catch (error) {
        if (error instanceof SpecFileError) throw new StartupError(error.message);
        throw error;
      }
      try {
        await desks.load();
      } catch (error) {
        if (error instanceof DeskFileError) throw new StartupError(error.message);
        throw error;
      }
      const code = newPairCode();
      let made: boolean;
      try {
        made = await configStore.create({ hash: pairCodeHash(code), expiresAt: Date.now() + PAIR_CODE_TTL_MS });
      } catch (error) {
        if (error instanceof ConfigFileError) throw new StartupError(error.message);
        throw error;
      }
      if (!made) return { pairCode: null };
      log({ event: "relay_config_started" });
      print(pairingText(config, code));
      return { pairCode: code };
    },
    async close() {
      specs.close();
    },
  };
}
