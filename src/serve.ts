// Run the relay: the startup checks, the in-process worker and the HTTP server.
import { createServer } from "node:http";
import type { Config } from "./config.js";
import { createRelay } from "./server.js";
import { consoleLogger, type Logger } from "./log.js";
import { connectClient, connectNative, makeClients } from "./temporal/connection.js";
import { runStartupChecks, StartupError, temporalAdmin } from "./startup.js";
import { S3Bucket } from "./bucket.js";
import { createWorkers } from "./worker.js";
import { createActivities } from "./activities.js";
import { Mirror } from "./mirror.js";
import { ConfigStore, temporalOAuthStore } from "./configStore.js";
import { temporalAgentBackend } from "./backend.js";
import { OfflineTools, lastSyncAt } from "./offline.js";
import { MessageTools } from "./messages.js";
import { Attachments } from "./attachments.js";

export const TEMPORAL_UNREACHABLE =
  "The relay cannot reach Temporal. Check TEMPORAL_ADDRESS and the Temporal auth settings (TEMPORAL_AUTH, TEMPORAL_API_KEY, TEMPORAL_TLS_CERT, TEMPORAL_TLS_KEY).";

/** The /mcp body limit: room for a base64 attachment at the size limit. */
export function mcpBodyLimit(attachmentMaxBytes: number): number {
  return Math.max(1024 * 1024, Math.ceil((attachmentMaxBytes * 4) / 3) * 2 + 1024 * 1024);
}

export async function serve(config: Config, log: Logger = consoleLogger, print = (text: string) => process.stdout.write(text + "\n")) {
  let connection;
  let native;
  try {
    connection = await connectClient(config.temporal);
    native = await connectNative(config.temporal);
  } catch {
    throw new StartupError(TEMPORAL_UNREACHABLE);
  }
  const clients = makeClients(connection, config.temporal.namespace, config.payloadKeys);
  const bucket = config.bucket ? new S3Bucket(config.bucket) : null;

  await runStartupChecks({ config, admin: temporalAdmin(connection), configClient: clients.config, bucket, log, print });

  const workers = await createWorkers({
    connection: native,
    namespace: config.temporal.namespace,
    taskQueue: config.temporal.taskQueue,
    keys: config.payloadKeys,
    activities: createActivities({ bucket, log }),
  });
  const workersDone = workers.run().catch(() => {
    log({ event: "worker_failed" });
    process.exit(1);
  });

  const mirror = new Mirror({ client: clients.data, deployId: config.deployId, taskQueue: config.temporal.taskQueue });
  const configStore = new ConfigStore(clients.config, config.deployId);
  const attachments = new Attachments({ bucket, deployId: config.deployId, maxBytes: config.attachmentMaxBytes });

  const relay = createRelay({
    publicUrl: config.publicUrl,
    loginPasswordBcrypt: config.ownerPasswordHash,
    callTimeoutMs: config.callTimeoutMs,
    extraRedirectUris: config.extraRedirectUris,
    log,
    mcpBodyLimit: mcpBodyLimit(config.attachmentMaxBytes),
    codecDataKeys: config.payloadKeys.data,
    uiOrigin: config.uiOrigin,
    agentBackend: temporalAgentBackend({ mirror, configStore, attachments }),
    offline: new OfflineTools({ mirror, configStore }),
    oauthStore: temporalOAuthStore(configStore),
    localTools: (hub) =>
      new MessageTools({
        mirror,
        attachments,
        push: (spec, message) => hub.pushMessage(spec, message),
        lastSyncAt: () => lastSyncAt(mirror),
      }),
  });
  const server = createServer(relay.app);
  relay.attach(server);
  server.listen(config.port, () => log({ event: "listening", port: config.port }));

  const shutdown = () => {
    relay.close();
    server.close();
    void workers.shutdown().then(async () => {
      await workersDone;
      await native.close();
      await connection.close();
      process.exit(0);
    });
    setTimeout(() => process.exit(0), 10_000).unref();
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}
