// The Temporal store: one workflow per spec, one per desk, and the relay-config workflow,
// with the relay's own worker in its process. Its behavior is the relay's
// behavior before 0.4.0.
import type { Connection } from "@temporalio/client";
import type { NativeConnection } from "@temporalio/worker";
import type { Config, TemporalSettings } from "../config.js";
import type { Bucket } from "../bucket.js";
import type { Logger } from "../log.js";
import { connectClient, connectNative, makeClients } from "../temporal/connection.js";
import { runStartupChecks, StartupError, temporalAdmin } from "../startup.js";
import { createWorkers, type RelayWorkers } from "../worker.js";
import { createActivities } from "../activities.js";
import { Mirror } from "../mirror.js";
import { DeskMirror } from "../deskMirror.js";
import { ConfigStore } from "../configStore.js";
import type { RelayStore } from "./types.js";

export const TEMPORAL_UNREACHABLE =
  "The relay cannot reach Temporal. Check TEMPORAL_ADDRESS and the Temporal auth settings (TEMPORAL_AUTH, TEMPORAL_API_KEY, TEMPORAL_TLS_CERT, TEMPORAL_TLS_KEY).";

/**
 * Connect to Temporal. With `worker: false` (the `relay pair-code` command) it
 * makes only the clients: the running relay's worker applies the updates.
 */
export async function openTemporalStore(
  config: Config,
  deps: { bucket: Bucket | null; log: Logger; worker?: boolean },
): Promise<RelayStore> {
  const t: TemporalSettings | null = config.temporal;
  const keys = config.payloadKeys;
  if (!t || !keys) throw new StartupError("The Temporal store needs TEMPORAL_ADDRESS and PAYLOAD_KEYS.");
  let connection: Connection;
  try {
    connection = await connectClient(t);
  } catch {
    throw new StartupError(TEMPORAL_UNREACHABLE);
  }
  const clients = makeClients(connection, t.namespace, keys);
  const specs = new Mirror({ client: clients.data, deployId: config.deployId, taskQueue: t.taskQueue });
  const desks = new DeskMirror({ client: clients.data, deployId: config.deployId, taskQueue: t.taskQueue });
  const configStore = new ConfigStore(clients.config, config.deployId);
  let native: NativeConnection | null = null;
  let workers: RelayWorkers | null = null;
  let workersDone: Promise<void> | null = null;

  return {
    kind: "temporal",
    specs,
    desks,
    config: configStore,
    codecDataKeys: keys.data,
    async start({ log, print }) {
      try {
        native = await connectNative(t);
      } catch {
        throw new StartupError(TEMPORAL_UNREACHABLE);
      }
      const result = await runStartupChecks({
        config,
        admin: temporalAdmin(connection),
        configClient: clients.config,
        bucket: deps.bucket,
        log,
        print,
      });
      if (deps.worker !== false) {
        workers = await createWorkers({
          connection: native,
          namespace: t.namespace,
          taskQueue: t.taskQueue,
          keys,
          activities: createActivities({ bucket: deps.bucket, log }),
        });
        workersDone = workers.run().catch(() => {
          log({ event: "worker_failed" });
          process.exit(1);
        });
      }
      return result;
    },
    async close() {
      await workers?.shutdown();
      await workersDone;
      await native?.close();
      await connection.close();
    },
  };
}
