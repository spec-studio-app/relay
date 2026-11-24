import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { defaultPayloadConverter } from "@temporalio/common";
import { harness, startTestServer } from "./temporal.js";
import { AesGcmCodec, configCodec, dataCodec, newPayloadKeys } from "../src/codec.js";
import { parsePayloadKeys } from "../src/config.js";
import { specAddress } from "../src/mirror.js";
import { relayConfigWorkflowId, specWorkflowId } from "../src/workflows/types.js";
import { runStartupChecks, REQUIRED_SEARCH_ATTRIBUTES, type TemporalAdmin } from "../src/startup.js";
import { testConfig } from "./temporal.js";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

test("the codec encrypts with the key ID and decrypts with any loaded key", async () => {
  const keys = parsePayloadKeys(newPayloadKeys());
  const codec = dataCodec(keys);
  const plain = defaultPayloadConverter.toPayload({ text: "Use the old icon." });
  const [encoded] = await codec.encode([plain]);
  assert.equal(new TextDecoder().decode(encoded.metadata!.encoding), "binary/encrypted");
  assert.equal(new TextDecoder().decode(encoded.metadata!["encryption-key-id"]), keys.data[0].id);
  assert.ok(!Buffer.from(encoded.data!).toString("latin1").includes("old icon"));
  const [decoded] = await codec.decode([encoded]);
  assert.deepEqual(defaultPayloadConverter.fromPayload(decoded), { text: "Use the old icon." });

  // The config key cannot decrypt a data payload, and the reverse.
  await assert.rejects(configCodec(keys).decode([encoded]), /No payload key/);
  const kept = await new AesGcmCodec({ encryptKey: keys.config[0], unknownKey: "keep" }).decode([encoded]);
  assert.deepEqual(kept[0], encoded);
});

test("R10.S1: the raw history of a spec workflow and the relay-config workflow holds no plain text", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress("/Users/me/code/app", "secret-history");
    await h.mirror.sync(spec, {
      line: 1,
      event: { kind: "message", id: "m1", text: "Use the old icon." },
      state: { title: "Secret history title", stage: "build" },
      turn: null,
    });
    await h.mirror.send(spec, { text: "Use the old icon, please." });
    const admin: TemporalAdmin = {
      namespaceExists: async () => true,
      registerNamespace: async () => {},
      searchAttributes: async () => new Set(REQUIRED_SEARCH_ATTRIBUTES.map((a) => a.name)),
      addSearchAttributes: async () => {},
    };
    await runStartupChecks({
      config: testConfig({ DEPLOY_ID: h.deployId, PUBLIC_URL: "https://relay-config-marker.example.com" }),
      admin,
      configClient: h.clients.config,
      bucket: null,
      log: () => {},
      print: () => {},
    });

    for (const workflowId of [specWorkflowId(spec.repoKey, spec.slug), relayConfigWorkflowId(h.deployId)]) {
      const raw = await env.connection.workflowService.getWorkflowExecutionHistory({
        namespace: "default",
        execution: { workflowId },
      });
      const bytes = Buffer.from(JSON.stringify(raw.history)).toString();
      assert.ok((raw.history?.events?.length ?? 0) > 0);
      // Payload data is bytes: check the raw bytes of every payload too.
      const payloadText = collectPayloadData(raw.history).join("\n");
      for (const secret of ["old icon", "/Users/me/code/app"]) {
        assert.ok(!bytes.includes(secret), `${workflowId} history shows ${secret}`);
        assert.ok(!payloadText.includes(secret), `${workflowId} payload shows ${secret}`);
      }
    }
  } finally {
    await h.close();
  }
});

/** The data of every payload in a history, as latin1 text. */
function collectPayloadData(value: unknown, out: string[] = []): string[] {
  if (value instanceof Uint8Array) {
    out.push(Buffer.from(value).toString("latin1"));
  } else if (Array.isArray(value)) {
    for (const v of value) collectPayloadData(v, out);
  } else if (value && typeof value === "object") {
    for (const v of Object.values(value)) collectPayloadData(v, out);
  }
  return out;
}
