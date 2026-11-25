// The workflow histories in test/fixtures were made by the relay 0.3 workflows
// (before the rules moved to specRules.ts and configRules.ts). Their replay on
// the current workflow code must give the same commands, so that the running
// workflows of a Temporal relay continue after the upgrade.
//
// RECORD_HISTORIES=1 makes the fixtures again from the current code.
import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { historyFromJSON, historyToJSON } from "@temporalio/common/lib/proto-utils";
import { eventually, harness, startTestServer } from "./temporal.js";
import { specAddress } from "../src/mirror.js";
import { parsePayloadKeys } from "../src/config.js";
import { configCodec, dataCodec } from "../src/codec.js";
import { WORKFLOWS_PATH } from "../src/worker.js";
import { relayConfigWorkflowId, specWorkflowId } from "../src/workflows/types.js";
import { pairCodeHash } from "../src/startup.js";

const FIXTURES = fileURLToPath(new URL("../../test/fixtures/", import.meta.url));
const RECORD = process.env.RECORD_HISTORIES === "1";
const REPO = "/Users/me/code/app";

interface Fixture {
  keysText: string;
  specWorkflowId: string;
  configWorkflowId: string;
  spec: unknown;
  config: unknown;
}

let env: TestWorkflowEnvironment | undefined;
before(async () => {
  if (RECORD) env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

function state(stage: string, createdAt = "2026-11-01T00:00:00Z") {
  return { slug: "replay", title: "Replay", stage, status: "working", mode: "autopilot", createdAt, repos: [] };
}

async function record(): Promise<Fixture> {
  const h = await harness(env!, { graceMs: 60_000 });
  try {
    const spec = specAddress(REPO, "replay");
    // Every update kind, with the branches that make IDs (uuid4) or use the patch.
    await h.mirror.sync(spec, {
      lines: [
        { line: 1, event: { t: "2026-11-01T00:00:01Z", kind: "status" } },
        { line: 2, event: { t: "2026-11-01T00:00:02Z", kind: "message", text: "Hello", stage: "spec" } },
        { line: 3, event: { t: "2026-11-01T00:00:03Z", kind: "message", text: "No id" } },
      ],
      first: true,
      state: state("spec"),
      turn: { turn: "claude", needsYou: false },
    });
    await h.mirror.sync(spec, { lines: [{ line: 2, event: { kind: "status" } }], state: state("plan") });
    const a = await h.mirror.send(spec, { text: "First" });
    const b = await h.mirror.send(spec, { text: "Second", replyTo: "x" });
    await h.mirror.ack(spec, a.seq);
    await h.mirror.ack(spec, b.seq, "The disk is full.");
    await h.mirror.read(spec, undefined, 0);
    await h.mirror.read(spec, undefined, 0, true);
    await h.mirror.refresh(spec, { state: state("build"), turn: null });
    await h.mirror.send(spec, { text: "Waits" });
    await h.mirror.sync(spec, { lines: [{ line: 4, event: { kind: "merged" } }], state: state("merged") });
    await h.mirror.refresh(spec, { state: state("merged"), turn: null });
    await h.mirror.sync(spec, {
      lines: [{ line: 1, event: { kind: "status" } }],
      first: true,
      state: state("spec", "2026-11-02T00:00:00Z"),
    });
    await h.mirror.gone(spec);

    await h.startConfig("ABCD-EFGH");
    await h.configStore.pair(pairCodeHash("ABCD-EFGH"), "s".repeat(64), new Date());
    await h.configStore.setRepos([REPO], new Date());
    await h.configStore.registerClient({ client_id: "c1" });
    await h.configStore.markSignedIn("c1");
    const token = (hash: string) => ({ hash, token: { clientId: "c1", scopes: ["mcp"], expiresAt: Date.now() + 3_600_000 } });
    await h.configStore.issueTokens({ access: token("a1"), refresh: token("r1") });
    await h.configStore.rotateRefresh("r1", "c1", { access: token("a2"), refresh: token("r2"), inherit: { scopes: true, resource: true } });
    await h.configStore.revoke("a2", "c1");
    await h.configStore.unpair("s".repeat(64));

    const specHandle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "replay"));
    const configHandle = h.clients.config.workflow.getHandle(relayConfigWorkflowId(h.deployId));
    const specHistory = await eventually(async () => {
      const history = await specHandle.fetchHistory();
      return (history.events?.length ?? 0) > 0 ? history : null;
    });
    const configHistory = await configHandle.fetchHistory();
    return {
      keysText: h.keysText,
      specWorkflowId: specWorkflowId(h.deployId, spec.repoKey, "replay"),
      configWorkflowId: relayConfigWorkflowId(h.deployId),
      spec: JSON.parse(historyToJSON(specHistory)),
      config: JSON.parse(historyToJSON(configHistory)),
    };
  } finally {
    await h.close();
  }
}

test("the 0.3 histories of the spec and relay-config workflows replay on the current code", async () => {
  const path = `${FIXTURES}histories-0.3.json`;
  if (RECORD) {
    mkdirSync(FIXTURES, { recursive: true });
    writeFileSync(path, JSON.stringify(await record(), null, 1) + "\n");
  }
  assert.ok(existsSync(path), "Record the fixture with RECORD_HISTORIES=1.");
  const fixture = JSON.parse(readFileSync(path, "utf8")) as Fixture;
  const keys = parsePayloadKeys(fixture.keysText);
  await Worker.runReplayHistory(
    { workflowsPath: WORKFLOWS_PATH, dataConverter: { payloadCodecs: [dataCodec(keys)] } },
    historyFromJSON(fixture.spec),
    fixture.specWorkflowId,
  );
  await Worker.runReplayHistory(
    { workflowsPath: WORKFLOWS_PATH, dataConverter: { payloadCodecs: [configCodec(keys)] } },
    historyFromJSON(fixture.config),
    fixture.configWorkflowId,
  );
});
