// A Temporal dev server (TestWorkflowEnvironment.createLocal) with the relay's
// search attributes, clients and workers. It uses the `temporal` CLI when one
// is installed (TEMPORAL_CLI, or /opt/homebrew/bin/temporal); else the SDK
// downloads the dev server once.
import { existsSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { loadConfig, parsePayloadKeys, type Config, type PayloadKeys } from "../src/config.js";
import { makeClients, type TemporalClients } from "../src/temporal/connection.js";
import { createWorkers, type RelayWorkers } from "../src/worker.js";
import { createActivities } from "../src/activities.js";
import { REQUIRED_SEARCH_ATTRIBUTES, temporalAdmin } from "../src/startup.js";
import { MemoryBucket } from "../src/bucket.js";
import { Mirror } from "../src/mirror.js";
import type { LogEntry } from "../src/log.js";

export const BCRYPT_FOR_CONFIG = "$2b$12$" + "a".repeat(53);

export function newKeysText(): string {
  const k = () => randomBytes(32).toString("base64");
  return `data:d1:${k()},config:c1:${k()}`;
}

/** A valid configuration with these overrides. */
export function testConfig(overrides: Record<string, string> = {}): Config {
  return loadConfig({
    PUBLIC_URL: "https://relay.example.com",
    OWNER_PASSWORD_HASH: BCRYPT_FOR_CONFIG,
    TEMPORAL_ADDRESS: "127.0.0.1:7233",
    PAYLOAD_KEYS: newKeysText(),
    ...overrides,
  });
}

function cliPath(): string | undefined {
  const candidates = [process.env.TEMPORAL_CLI, "/opt/homebrew/bin/temporal", "/usr/local/bin/temporal"];
  return candidates.find((p): p is string => !!p && existsSync(p));
}

export async function startTestServer(): Promise<TestWorkflowEnvironment> {
  const path = cliPath();
  const env = await TestWorkflowEnvironment.createLocal(
    path ? { server: { executable: { type: "existing-path", path } } } : {},
  );
  await temporalAdmin(env.connection).addSearchAttributes("default", REQUIRED_SEARCH_ATTRIBUTES);
  return env;
}

export interface Harness {
  env: TestWorkflowEnvironment;
  keys: PayloadKeys;
  keysText: string;
  clients: TemporalClients;
  deployId: string;
  taskQueue: string;
  bucket: MemoryBucket;
  mirror: Mirror;
  logs: LogEntry[];
  /** Start workers on this harness's task queues. */
  startWorkers(): Promise<RelayWorkers>;
  close(): Promise<void>;
}

/** Clients, workers and a mirror on a fresh deploy ID of a shared test server. */
export async function harness(
  env: TestWorkflowEnvironment,
  options: { deployId?: string; updateLimit?: number; keysText?: string } = {},
): Promise<Harness> {
  const keysText = options.keysText ?? newKeysText();
  const keys = parsePayloadKeys(keysText);
  const clients = makeClients(env.connection, "default", keys);
  const deployId = options.deployId ?? `t${randomBytes(4).toString("hex")}`;
  const taskQueue = `relay-${deployId}`;
  const bucket = new MemoryBucket();
  const logs: LogEntry[] = [];
  const workers: RelayWorkers[] = [];
  const startWorkers = async () => {
    const w = await createWorkers({
      connection: env.nativeConnection,
      namespace: "default",
      taskQueue,
      keys,
      activities: createActivities({ bucket, log: (e) => logs.push(e) }),
    });
    void w.run();
    workers.push(w);
    return w;
  };
  await startWorkers();
  return {
    env,
    keys,
    keysText,
    clients,
    deployId,
    taskQueue,
    bucket,
    logs,
    mirror: new Mirror({ client: clients.data, deployId, taskQueue, updateLimit: options.updateLimit }),
    startWorkers,
    async close() {
      await Promise.all(workers.map((w) => w.shutdown()));
    },
  };
}

/** Wait until the check passes (visibility is eventually consistent). */
export async function eventually<T>(check: () => Promise<T | undefined | null | false>, ms = 10_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > end) throw new Error("The condition did not pass in time.");
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}
