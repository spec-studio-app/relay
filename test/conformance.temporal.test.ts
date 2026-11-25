// The conformance suite on the Temporal store, each scenario on a new deploy ID.
import { after, before } from "node:test";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { harness, startTestServer } from "./temporal.js";
import { runConformance } from "./conformance.js";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

runConformance("Temporal store", async () => {
  const h = await harness(env, { graceMs: 60_000 });
  return {
    specs: h.mirror,
    config: h.configStore,
    startConfig: (code) => h.startConfig(code),
    close: () => h.close(),
  };
});
