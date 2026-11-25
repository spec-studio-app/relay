import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { harness, startTestServer, testConfig } from "./temporal.js";
import {
  REQUIRED_SEARCH_ATTRIBUTES,
  StartupError,
  newPairCode,
  pairCodeHash,
  runStartupChecks,
  temporalAdmin,
  type TemporalAdmin,
} from "../src/startup.js";
import { MemoryBucket } from "../src/bucket.js";
import type { LogEntry } from "../src/log.js";
import type { Client } from "@temporalio/client";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

class FakeAdmin implements TemporalAdmin {
  namespaces = new Set<string>(["default"]);
  attributes = new Set<string>();
  registered: Array<{ namespace: string; days: number }> = [];
  added: Array<{ name: string; type: string }> = [];
  async namespaceExists(ns: string) {
    return this.namespaces.has(ns);
  }
  async registerNamespace(ns: string, days: number) {
    this.namespaces.add(ns);
    this.registered.push({ namespace: ns, days });
  }
  async searchAttributes() {
    return new Set(this.attributes);
  }
  async addSearchAttributes(_ns: string, attrs: Array<{ name: string; type: string }>) {
    for (const a of attrs) this.attributes.add(a.name);
    this.added.push(...attrs);
  }
}

/** A config client that the checks must not reach (the checks fail before). */
const unusedClient = {} as Client;

test("R2.S1: on a self-hosted Temporal the relay registers the missing search attributes", async () => {
  const h = await harness(env);
  try {
    const admin = new FakeAdmin();
    admin.attributes.add("RepoKey");
    const logs: LogEntry[] = [];
    const printed: string[] = [];
    await runStartupChecks({
      config: testConfig({ DEPLOY_ID: h.deployId }),
      admin,
      configClient: h.clients.config,
      bucket: null,
      log: (e) => logs.push(e),
      print: (t) => printed.push(t),
    });
    assert.deepEqual(
      admin.added.map((a) => a.name).sort(),
      REQUIRED_SEARCH_ATTRIBUTES.map((a) => a.name).filter((n) => n !== "RepoKey").sort(),
    );
    assert.deepEqual(admin.added.find((a) => a.name === "SpecTitle"), { name: "SpecTitle", type: "Text" });
    assert.deepEqual(admin.added.find((a) => a.name === "NeedsYou"), { name: "NeedsYou", type: "Bool" });
    assert.ok(logs.some((e) => e.event === "search_attributes_registered"));
  } finally {
    await h.close();
  }
});

test("R2.S1: on Temporal Cloud the relay prints each missing attribute with its type and stops", async () => {
  const admin = new FakeAdmin();
  const printed: string[] = [];
  await assert.rejects(
    runStartupChecks({
      config: testConfig({ TEMPORAL_API_KEY: "cloud-key-secret" }),
      admin,
      configClient: unusedClient,
      bucket: null,
      log: () => {},
      print: (t) => printed.push(t),
    }),
    (error: Error) => error instanceof StartupError && /search attributes/.test(error.message),
  );
  const text = printed.join("\n");
  for (const a of REQUIRED_SEARCH_ATTRIBUTES) assert.match(text, new RegExp(`${a.name} \\(${a.type}\\)`));
  assert.ok(!text.includes("cloud-key-secret"));
  assert.equal(admin.added.length, 0);
});

test("R2.S1: Temporal Cloud with mTLS (a *.tmprl.cloud address) also prints the attributes and does not register", async () => {
  const { isTemporalCloud } = await import("../src/startup.js");
  const pemCert = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
  const pemKey = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n";
  const config = testConfig({
    TEMPORAL_ADDRESS: "acct.studio.tmprl.cloud:7233",
    TEMPORAL_NAMESPACE: "acct.studio",
    TEMPORAL_TLS_CERT: pemCert,
    TEMPORAL_TLS_KEY: pemKey,
  });
  assert.equal(config.temporal!.auth, "mtls");
  assert.equal(isTemporalCloud(config.temporal!), true);
  assert.equal(isTemporalCloud(testConfig({ TEMPORAL_ADDRESS: "temporal.example.com:7233" }).temporal!), false);
  assert.equal(isTemporalCloud(testConfig({ TEMPORAL_ADDRESS: "eu-west-1.aws.api.temporal.io:7233", TEMPORAL_API_KEY: "k" }).temporal!), true);

  const admin = new FakeAdmin();
  admin.namespaces.add("acct.studio");
  const printed: string[] = [];
  await assert.rejects(
    runStartupChecks({ config, admin, configClient: unusedClient, bucket: null, log: () => {}, print: (t) => printed.push(t) }),
    /search attributes/,
  );
  const text = printed.join("\n");
  for (const a of REQUIRED_SEARCH_ATTRIBUTES) assert.match(text, new RegExp(`${a.name} \\(${a.type}\\)`));
  assert.match(text, /tcld namespace search-attributes add/);
  assert.equal(admin.added.length, 0);

  // A missing namespace on Cloud with mTLS: the relay names it and does not register it.
  await assert.rejects(
    runStartupChecks({ config, admin: new FakeAdmin(), configClient: unusedClient, bucket: null, log: () => {}, print: () => {} }),
    /namespace acct\.studio does not exist/,
  );
});

test("a self-hosted Temporal that refuses the register prints the attributes with their types", async () => {
  const admin = new FakeAdmin();
  admin.addSearchAttributes = async () => {
    throw new Error("PermissionDenied");
  };
  const printed: string[] = [];
  await assert.rejects(
    runStartupChecks({ config: testConfig(), admin, configClient: unusedClient, bucket: null, log: () => {}, print: (t) => printed.push(t) }),
    /cannot register the search attributes/,
  );
  const text = printed.join("\n");
  for (const a of REQUIRED_SEARCH_ATTRIBUTES) assert.match(text, new RegExp(`${a.name} \\(${a.type}\\)`));
});

test("a missing namespace: self-hosted registers it (30 days), Temporal Cloud stops and names it", async () => {
  const h = await harness(env);
  try {
    const admin = new FakeAdmin();
    for (const a of REQUIRED_SEARCH_ATTRIBUTES) admin.attributes.add(a.name);
    const logs: LogEntry[] = [];
    await runStartupChecks({
      config: testConfig({ DEPLOY_ID: h.deployId, TEMPORAL_NAMESPACE: "studio-relay" }),
      admin,
      configClient: h.clients.config,
      bucket: null,
      log: (e) => logs.push(e),
      print: () => {},
    });
    assert.deepEqual(admin.registered, [{ namespace: "studio-relay", days: 30 }]);
    assert.ok(logs.some((e) => e.event === "namespace_registered" && e.namespace === "studio-relay"));
  } finally {
    await h.close();
  }

  await assert.rejects(
    runStartupChecks({
      config: testConfig({ TEMPORAL_API_KEY: "k", TEMPORAL_NAMESPACE: "acct.studio" }),
      admin: new FakeAdmin(),
      configClient: unusedClient,
      bucket: null,
      log: () => {},
      print: () => {},
    }),
    /namespace acct\.studio does not exist/,
  );
});

test("the real admin registers a new namespace and its search attributes on the dev server", async () => {
  const namespace = `ns-${randomBytes(4).toString("hex")}`;
  const admin = temporalAdmin(env.connection);
  assert.equal(await admin.namespaceExists(namespace), false);
  const h = await harness(env);
  try {
    await runStartupChecks({
      config: testConfig({ DEPLOY_ID: h.deployId, TEMPORAL_NAMESPACE: namespace }),
      admin,
      configClient: h.clients.config,
      bucket: null,
      log: () => {},
      print: () => {},
      retryDelayMs: 200,
    });
    assert.equal(await admin.namespaceExists(namespace), true);
    const have = await admin.searchAttributes(namespace);
    for (const a of REQUIRED_SEARCH_ATTRIBUTES) assert.ok(have.has(a.name), a.name);
  } finally {
    await h.close();
  }
});

test("a bucket that cannot be reached stops the start and names the bucket settings", async () => {
  const admin = new FakeAdmin();
  for (const a of REQUIRED_SEARCH_ATTRIBUTES) admin.attributes.add(a.name);
  const bucket = new MemoryBucket();
  bucket.failCheck = true;
  await assert.rejects(
    runStartupChecks({ config: testConfig(), admin, configClient: unusedClient, bucket, log: () => {}, print: () => {} }),
    /BUCKET_ENDPOINT/,
  );
});

test("R2.S2: the first start starts the relay-config workflow and prints a pairing code; the second prints none", async () => {
  const h = await harness(env);
  try {
    const admin = new FakeAdmin();
    for (const a of REQUIRED_SEARCH_ATTRIBUTES) admin.attributes.add(a.name);
    const config = testConfig({ DEPLOY_ID: h.deployId, PUBLIC_URL: "https://relay.example.com" });
    const printed: string[] = [];
    const logs: LogEntry[] = [];
    const first = await runStartupChecks({
      config,
      admin,
      configClient: h.clients.config,
      bucket: new MemoryBucket(),
      log: (e) => logs.push(e),
      print: (t) => printed.push(t),
    });
    assert.match(first.pairCode ?? "", /^[A-Z2-9]{4}-[A-Z2-9]{4}-[A-Z2-9]{4}$/);
    assert.match(printed.join("\n"), new RegExp(`studio agent pair wss://relay\\.example\\.com/agent ${first.pairCode}`));
    assert.ok(!JSON.stringify(logs).includes(first.pairCode!), "the pairing code is not in the log");

    const second = await runStartupChecks({
      config,
      admin,
      configClient: h.clients.config,
      bucket: null,
      log: () => {},
      print: (t) => printed.push(t),
    });
    assert.equal(second.pairCode, null);
    assert.equal(printed.length, 1);
  } finally {
    await h.close();
  }
});

test("pairing codes: the format and the hash ignore case and dashes", () => {
  const code = newPairCode();
  assert.match(code, /^[A-HJ-NP-Z2-9]{4}-[A-HJ-NP-Z2-9]{4}-[A-HJ-NP-Z2-9]{4}$/);
  assert.equal(pairCodeHash(code), pairCodeHash(code.toLowerCase().replace(/-/g, " ")));
  assert.notEqual(pairCodeHash(code), pairCodeHash(newPairCode()));
});

test("relay pair-code fails in time when no relay runs (no worker answers the update)", async () => {
  const { makePairCode } = await import("../src/startup.js");
  const client = {
    workflow: { getHandle: () => ({ executeUpdate: () => new Promise(() => {}) }) },
  } as unknown as Client;
  const started = Date.now();
  await assert.rejects(makePairCode(client, "d", 200), /timeout/);
  assert.ok(Date.now() - started < 2000);
});
