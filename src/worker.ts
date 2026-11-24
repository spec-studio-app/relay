// The relay's own worker, in the relay's process: one worker for the spec
// workflows (data key) and one for the relay-config workflow (config key).
import { fileURLToPath } from "node:url";
import { bundleWorkflowCode, Worker, type NativeConnection, type WorkflowBundleWithSourceMap } from "@temporalio/worker";
import type { PayloadKeys } from "./config.js";
import { configCodec, dataCodec } from "./codec.js";
import { configTaskQueue } from "./temporal/connection.js";
import type { Activities } from "./activities.js";

export const WORKFLOWS_PATH = fileURLToPath(new URL("./workflows/index.js", import.meta.url));

let bundle: Promise<WorkflowBundleWithSourceMap> | null = null;

/** Bundle the workflow code once per process. */
export function workflowBundle(): Promise<WorkflowBundleWithSourceMap> {
  bundle ??= bundleWorkflowCode({ workflowsPath: WORKFLOWS_PATH, logger: quietLogger });
  return bundle;
}

const quietLogger = {
  log() {},
  trace() {},
  debug() {},
  info() {},
  warn() {},
  error() {},
};

export interface RelayWorkers {
  run(): Promise<void>;
  shutdown(): Promise<void>;
}

export async function createWorkers(options: {
  connection: NativeConnection;
  namespace: string;
  taskQueue: string;
  keys: PayloadKeys;
  activities: Activities;
}): Promise<RelayWorkers> {
  const workflowBundleCode = await workflowBundle();
  const spec = await Worker.create({
    connection: options.connection,
    namespace: options.namespace,
    taskQueue: options.taskQueue,
    workflowBundle: workflowBundleCode,
    activities: options.activities,
    dataConverter: { payloadCodecs: [dataCodec(options.keys)] },
  });
  const config = await Worker.create({
    connection: options.connection,
    namespace: options.namespace,
    taskQueue: configTaskQueue(options.taskQueue),
    workflowBundle: workflowBundleCode,
    dataConverter: { payloadCodecs: [configCodec(options.keys)] },
  });
  let running: Promise<void> | null = null;
  return {
    run() {
      running ??= Promise.all([spec.run(), config.run()]).then(() => undefined);
      return running;
    },
    async shutdown() {
      for (const w of [spec, config]) {
        if (w.getState() === "RUNNING") w.shutdown();
      }
      await running?.catch(() => undefined);
    },
  };
}
