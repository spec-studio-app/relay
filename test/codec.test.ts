import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { defaultPayloadConverter } from "@temporalio/common";
import { Client } from "@temporalio/client";
import { eventually, harness, startTestServer } from "./temporal.js";
import { AesGcmCodec, configCodec, dataCodec, newPayloadKeys } from "../src/codec.js";
import { parsePayloadKeys } from "../src/config.js";
import { specAddress } from "../src/mirror.js";
import { relayConfigWorkflowId, specWorkflowId } from "../src/workflows/types.js";
import { issueTokensUpdate } from "../src/workflows/relayConfig.js";
import { sendUpdate } from "../src/workflows/spec.js";
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

test("the codec refuses a payload that is not encrypted; the Web UI codec lets it pass", async () => {
  const keys = parsePayloadKeys(newPayloadKeys());
  const plain = defaultPayloadConverter.toPayload({ access: "forged" });
  await assert.rejects(dataCodec(keys).decode([plain]), /not encrypted/);
  await assert.rejects(configCodec(keys).decode([plain]), /not encrypted/);
  await assert.rejects(dataCodec(keys).decode([{ data: new Uint8Array([1, 2]) }]), /not encrypted/);
  const ui = new AesGcmCodec({ encryptKey: keys.data[0], unknownKey: "keep", plain: "keep" });
  assert.deepEqual((await ui.decode([plain]))[0], plain);
});

test("the key ID is bound into the GCM additional data: a payload with another key ID does not decrypt", async () => {
  const key = parsePayloadKeys(newPayloadKeys()).data[0].key;
  const codec = new AesGcmCodec({ encryptKey: { id: "a", key }, decryptKeys: [{ id: "b", key }] });
  const [encoded] = await codec.encode([defaultPayloadConverter.toPayload("hi")]);
  assert.equal(defaultPayloadConverter.fromPayload((await codec.decode([encoded]))[0]), "hi");
  const relabeled = { ...encoded, metadata: { ...encoded.metadata, "encryption-key-id": new TextEncoder().encode("b") } };
  await assert.rejects(codec.decode([relabeled]));
});

test("a namespace writer without the payload keys cannot give the workflows input", async () => {
  const h = await harness(env);
  try {
    await h.startConfig();
    const spec = specAddress("/Users/me/code/plain", "plain-input");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: { title: "Plain" }, turn: null });
    // A client with no codec sends plain JSON.
    const plainClient = new Client({ connection: env.connection, namespace: "default" });
    const within = <T>(p: Promise<T>) =>
      Promise.race([p, new Promise<never>((_, reject) => setTimeout(() => reject(new Error("no answer")), 3000))]);
    const forged = { clientId: "x", scopes: [], expiresAt: Date.now() + 3_600_000 };
    await assert.rejects(
      within(
        plainClient.workflow.getHandle(relayConfigWorkflowId(h.deployId)).executeUpdate(issueTokensUpdate, {
          args: [{ access: { hash: "f".repeat(64), token: forged }, refresh: { hash: "e".repeat(64), token: forged } }],
        }),
      ),
    );
    await assert.rejects(
      within(
        plainClient.workflow
          .getHandle(specWorkflowId(spec.repoKey, spec.slug))
          .executeUpdate(sendUpdate, { args: [{ id: "x", text: "typed into the terminal", at: new Date().toISOString() }] }),
      ),
    );
    // The worker fails the workflow task of a plain update; the workflow goes on
    // when Temporal drops the update.
    const state = await eventually(() => h.mirror.state(spec).catch(() => null), 90_000);
    assert.equal(state.mac.messages.length, 0);
    assert.equal(await eventually(() => h.configStore.token("access", "f".repeat(64)).then(() => true).catch(() => false), 90_000), true);
    assert.equal(await h.configStore.token("access", "f".repeat(64)), null);
    // The workflows still work for the relay.
    const sent = await h.mirror.send(spec, { text: "From the relay." });
    assert.equal(sent.seq, 1);
  } finally {
    await h.close();
  }
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

const UI = "https://ui.example.com";

/** The first input payload of a workflow's history, as codec JSON. */
async function firstInput(workflowId: string) {
  const raw = await env.connection.workflowService.getWorkflowExecutionHistory({ namespace: "default", execution: { workflowId } });
  const p = raw.history!.events![0].workflowExecutionStartedEventAttributes!.input!.payloads![0];
  const metadata: Record<string, string> = {};
  for (const [k, v] of Object.entries(p.metadata ?? {})) metadata[k] = Buffer.from(v).toString("base64");
  return { metadata, data: Buffer.from(p.data ?? new Uint8Array()).toString("base64") };
}

function decode(base: string, payloads: unknown[], headers: Record<string, string> = {}) {
  return fetch(`${base}/codec/decode`, {
    method: "POST",
    headers: { "content-type": "application/json", origin: UI, ...headers },
    body: JSON.stringify({ payloads }),
  });
}

test("R10.S2, R10.S3: a signed-in decode from UI_ORIGIN shows spec payloads as JSON; config payloads stay encrypted; no sign-in gives 401", async () => {
  const { signIn, PASSWORD } = await import("./helpers.js");
  const h = await harness(env);
  const t = await h.relay({ uiOrigin: UI });
  try {
    const spec = specAddress("/Users/me/code/codec", "decode-me");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: { title: "Decode me" }, turn: null });
    const specPayload = await firstInput(specWorkflowId(spec.repoKey, spec.slug));
    const configPayload = await firstInput(relayConfigWorkflowId(h.deployId));

    // No sign-in: 401, and the payloads stay hidden.
    const anonymous = await decode(t.base, [specPayload]);
    assert.equal(anonymous.status, 401);
    assert.equal(anonymous.headers.get("access-control-allow-origin"), UI);

    const { accessToken } = await signIn(t.base);
    const res = await decode(t.base, [specPayload, configPayload], { authorization: `Bearer ${accessToken}` });
    assert.equal(res.status, 200);
    assert.equal(res.headers.get("access-control-allow-origin"), UI);
    assert.equal(res.headers.get("access-control-allow-credentials"), "true");
    const body = await res.json();
    const decoded = body.payloads[0];
    assert.equal(Buffer.from(decoded.metadata.encoding, "base64").toString(), "json/plain");
    const state = JSON.parse(Buffer.from(decoded.data, "base64").toString());
    assert.equal(state.spec.slug, "decode-me");
    assert.deepEqual(body.payloads[1], configPayload, "the config payload stays encrypted");

    // The session cookie from the codec sign-in also works.
    const wrong = await fetch(`${t.base}/codec/signin`, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ password: "wrong" }).toString(),
    });
    assert.equal(wrong.status, 401);
    const ok = await fetch(`${t.base}/codec/signin`, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ password: PASSWORD }).toString(),
    });
    assert.equal(ok.status, 200);
    const session = ok.headers.getSetCookie().find((c) => c.startsWith("relay_session=")) ?? "";
    const cookie = session.split(";")[0];
    assert.match(session, /SameSite=None/i);
    assert.equal((await decode(t.base, [specPayload], { cookie })).status, 200);
    assert.equal((await decode(t.base, [specPayload], { cookie: "relay_session=99999999999999.forged" })).status, 401);

    // Encode round trip with the data key.
    const plain = { metadata: { encoding: Buffer.from("json/plain").toString("base64") }, data: Buffer.from('"hi"').toString("base64") };
    const encoded = await (
      await fetch(`${t.base}/codec/encode`, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${accessToken}` },
        body: JSON.stringify({ payloads: [plain] }),
      })
    ).json();
    const back = await (await decode(t.base, encoded.payloads, { authorization: `Bearer ${accessToken}` })).json();
    assert.deepEqual(back.payloads[0], plain);
  } finally {
    await t.close();
    await h.close();
  }
});

test("CORS: a preflight from UI_ORIGIN gets the headers; another origin gets none", async () => {
  const h = await harness(env);
  const t = await h.relay({ uiOrigin: UI });
  try {
    const preflight = (origin: string) =>
      fetch(`${t.base}/codec/decode`, {
        method: "OPTIONS",
        headers: { origin, "access-control-request-method": "POST", "access-control-request-headers": "content-type,x-namespace" },
      });
    const good = await preflight(UI);
    assert.equal(good.status, 204);
    assert.equal(good.headers.get("access-control-allow-origin"), UI);
    assert.match(good.headers.get("access-control-allow-headers") ?? "", /x-namespace/);
    const bad = await preflight("https://evil.example.com");
    assert.equal(bad.headers.get("access-control-allow-origin"), null);
  } finally {
    await t.close();
    await h.close();
  }
});
