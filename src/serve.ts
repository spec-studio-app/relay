// Run the relay: open the store, its startup checks, and the HTTP server.
import { createServer } from "node:http";
import type { Config } from "./config.js";
import { createRelay } from "./server.js";
import { consoleLogger, type Logger } from "./log.js";
import { S3Bucket } from "./bucket.js";
import { OfflineTools, lastSyncAt } from "./offline.js";
import { MessageTools } from "./messages.js";
import { Attachments } from "./attachments.js";
import { openStore, storeReasonText } from "./store/index.js";
import { storeAgentBackend, storeOAuthStore } from "./store/adapters.js";

/** The /mcp body limit: room for a base64 attachment at the size limit. */
export function mcpBodyLimit(attachmentMaxBytes: number): number {
  return Math.max(1024 * 1024, Math.ceil((attachmentMaxBytes * 4) / 3) * 2 + 1024 * 1024);
}

export async function serve(config: Config, log: Logger = consoleLogger, print = (text: string) => process.stdout.write(text + "\n")) {
  const bucket = config.bucket ? new S3Bucket(config.bucket) : null;
  log({ event: "store_selected", store: config.store, reason: storeReasonText(config) });
  const store = await openStore(config, { bucket, log });
  await store.start({ log, print });

  const attachments = new Attachments({ bucket, deployId: config.deployId, maxBytes: config.attachmentMaxBytes });
  const relay = createRelay({
    publicUrl: config.publicUrl,
    loginPasswordBcrypt: config.ownerPasswordHash,
    callTimeoutMs: config.callTimeoutMs,
    extraRedirectUris: config.extraRedirectUris,
    log,
    mcpBodyLimit: mcpBodyLimit(config.attachmentMaxBytes),
    // The codec endpoints decode Temporal payloads: only the Temporal store has them.
    codecDataKeys: store.codecDataKeys,
    uiOrigin: config.uiOrigin,
    agentBackend: storeAgentBackend({ specs: store.specs, config: store.config, attachments }),
    offline: new OfflineTools({ specs: store.specs, config: store.config }),
    oauthStore: storeOAuthStore(store.config),
    localTools: (hub) =>
      new MessageTools({
        specs: store.specs,
        attachments,
        push: (spec, message) => hub.pushMessage(spec, message),
        lastSyncAt: () => lastSyncAt(store.specs),
      }),
  });
  const server = createServer(relay.app);
  relay.attach(server);
  server.listen(config.port, () => log({ event: "listening", port: config.port }));

  const shutdown = () => {
    relay.close();
    server.close();
    void store.close().then(() => process.exit(0));
    setTimeout(() => process.exit(0), 10_000).unref();
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}
