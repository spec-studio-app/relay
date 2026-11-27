// The relay and the Mac on the same store (plugins-are-the-backend R10). The
// fake plugin (test/fixtures/fakePlugin.mjs, with --events) is the shared
// store: the test writes the Mac's state and activity events to it, as the
// app does, and the relay reads them for a sync frame with "linesInStore".
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Attachments } from "../src/attachments.js";
import { MemoryBucket } from "../src/bucket.js";
import { repoKeyOf } from "../src/store/address.js";
import { storeAgentBackend } from "../src/store/adapters.js";
import { openJsonStore } from "../src/store/json/index.js";
import { openPluginStore, type PluginRelayStore } from "../src/store/plugin/index.js";
import { ACTIVITY_EVENT, APP_SPECS, specKey, specStream } from "../src/store/plugin/pluginSpecStore.js";
import type { RelayStore } from "../src/store/types.js";
import { AGENT_SECRET, connectAgent, sleep, startRelay } from "./helpers.js";
import { FAKE_PLUGIN } from "./plugin.js";

const REPO = "/Users/me/code/shared";
const STORE_ID = "temporal:localhost:7233/default/deploy-1";
const HELLO = { type: "hello", version: 3, agentVersion: "0.9.0", wires: [3], repos: [REPO] };

function address(slug: string) {
  return { repoKey: repoKeyOf(REPO), slug, repo: REPO };
}

function appState(slug: string, over: Record<string, unknown> = {}) {
  return { slug, title: `Title of ${slug}`, stage: "build", status: "working", mode: "autopilot", createdAt: "2026-11-01T00:00:00Z", ...over };
}

async function openShared(flags: string[] = ["--events", "--store-id", STORE_ID]): Promise<PluginRelayStore> {
  const store = await openPluginStore(
    { deployId: "default", publicUrl: "https://relay.example.com", plugin: { transport: "stdio", command: process.execPath, args: [FAKE_PLUGIN, ...flags], settings: {}, from: "RELAY_PLUGIN" } },
    { bucket: null, log: () => {} },
  );
  await store.start({ log: () => {}, print: () => {} });
  return store;
}

/** The Mac writes its state and its activity lines to the shared store with state.commit (protocol 1.1), as the app does. */
async function macCommit(store: PluginRelayStore, slug: string, state: Record<string, unknown>, lines: Array<unknown>): Promise<void> {
  const spec = address(slug);
  const key = specKey(spec);
  const doc = await store.client.state.get({ collection: APP_SPECS, key });
  await store.client.state.commit({
    collection: APP_SPECS,
    key,
    value: state as never,
    attributes: { repoKey: spec.repoKey, stage: String(state.stage), status: String(state.status), repo: REPO },
    expectedVersion: doc.exists ? doc.version : null,
    stream: specStream(spec),
    events: lines.map((data) => ({
      type: ACTIVITY_EVENT,
      data: data as never,
      ...(typeof data === "object" && data !== null && "t" in data ? { at: String((data as { t: unknown }).t) } : {}),
    })),
  });
}

async function streamOf(store: PluginRelayStore, slug: string) {
  return store.client.events.readAll({ stream: specStream(address(slug)) });
}

function relayOn(store: RelayStore, o: { identity?: boolean } = {}) {
  const attachments = new Attachments({ bucket: new MemoryBucket(), deployId: "default", maxBytes: 1024 * 1024 });
  return startRelay({
    agentBackend: storeAgentBackend({
      specs: store.specs,
      desks: store.desks,
      config: store.config,
      attachments,
      ...(o.identity !== false && store.identity ? { identity: () => store.identity!() } : {}),
    }),
  });
}

test("R10.S1: a linesInStore sync applies the Mac's lines from the stream once, and appends nothing", async () => {
  const store = await openShared();
  try {
    const spec = address("same-deploy");
    await macCommit(store, "same-deploy", appState("same-deploy", { stage: "plan" }), [
      { kind: "created", t: "2026-11-01T00:00:01Z" },
      { kind: "status", t: "2026-11-01T00:00:02Z", status: "working" },
      { kind: "note", t: "2026-11-01T00:00:03Z", text: "Planning." },
    ]);

    const first = await store.specs.syncFromStore(spec, { first: true, turn: { turn: "claude", needsYou: false } });
    assert.deepEqual(first, { line: 3, applied: true });
    const state = await store.specs.state(spec);
    assert.ok(state);
    assert.deepEqual(state.events.map((e) => e.line), [1, 2, 3]);
    assert.equal(state.events[2].event.text, "Planning.");
    assert.equal(state.state?.stage, "plan", "the state comes from app-specs");
    assert.deepEqual(state.turn, { turn: "claude", needsYou: false });

    // A second sync with no new event changes nothing: not even the document's version.
    const before = await store.client.state.get({ collection: "specs", key: specKey(spec) });
    const again = await store.specs.syncFromStore(spec, {});
    assert.deepEqual(again, { line: 3, applied: false });
    const after = await store.client.state.get({ collection: "specs", key: specKey(spec) });
    assert.ok(before.exists && after.exists);
    assert.equal(after.version, before.version);

    // A message from Claude AI puts the relay's own event in the same stream; it is not a line of the Mac.
    await store.specs.send(spec, { text: "Use the old icon." });
    await macCommit(store, "same-deploy", appState("same-deploy", { stage: "build" }), [
      { kind: "status", t: "2026-11-01T00:00:04Z", status: "building" },
      { kind: "note", t: "2026-11-01T00:00:05Z", text: "Building." },
    ]);
    const next = await store.specs.syncFromStore(spec, {});
    assert.deepEqual(next, { line: 5, applied: true });
    const later = await store.specs.state(spec);
    assert.ok(later);
    assert.deepEqual(later.events.map((e) => e.line), [1, 2, 3, 4, 5]);
    assert.equal(later.state?.stage, "build");
    assert.equal(later.mac.messages.length, 1, "the message for the Mac stays");

    // The stream has each line of the Mac once, and no sync event of the relay.
    const events = await streamOf(store, "same-deploy");
    assert.deepEqual(events.map((e) => e.type), ["activity", "activity", "activity", "message", "activity", "activity"]);
    const texts = events.filter((e) => e.type === ACTIVITY_EVENT).map((e) => JSON.stringify(e.data));
    assert.equal(new Set(texts).size, texts.length, "no line is in the stream two times");

    // A first sync again (the Mac lost its cursor) reads from the start and adds no line twice.
    const restart = await store.specs.syncFromStore(spec, { first: true });
    assert.equal(restart.line, 5);
    const end = await store.specs.state(spec);
    assert.deepEqual(end?.events.map((e) => e.line), [1, 2, 3, 4, 5]);
  } finally {
    await store.close();
  }
});

test("R10.S1: a new state with no new line still applies, once", async () => {
  const store = await openShared();
  try {
    const spec = address("state-only");
    await macCommit(store, "state-only", appState("state-only"), [{ kind: "created", t: "2026-11-01T00:00:01Z" }]);
    await store.specs.syncFromStore(spec, { first: true });
    await macCommit(store, "state-only", appState("state-only", { status: "waiting" }), []);
    const result = await store.specs.syncFromStore(spec, {});
    assert.deepEqual(result, { line: 1, applied: false });
    assert.equal((await store.specs.state(spec))?.state?.status, "waiting");
    const v1 = await store.client.state.get({ collection: "specs", key: specKey(spec) });
    await store.specs.syncFromStore(spec, {});
    const v2 = await store.client.state.get({ collection: "specs", key: specKey(spec) });
    assert.ok(v1.exists && v2.exists);
    assert.equal(v2.version, v1.version, "the same state again changes nothing");
  } finally {
    await store.close();
  }
});

test("R10.S1: the welcome carries the store, and a linesInStore frame gets synced with the last line", async () => {
  const store = await openShared();
  const t = await relayOn(store);
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    const welcome = await agent.next("welcome");
    assert.deepEqual(welcome.store, { plugin: "fake", storeId: STORE_ID });

    await macCommit(store, "wire", appState("wire"), [
      { kind: "created", t: "2026-11-01T00:00:01Z" },
      { kind: "note", t: "2026-11-01T00:00:02Z", text: "Hi." },
    ]);
    const spec = address("wire");
    agent.send({ type: "sync", spec, linesInStore: true, first: true, turn: { turn: "you", needsYou: true } });
    const synced = await agent.next("synced");
    assert.deepEqual(synced, { type: "synced", spec: { repoKey: spec.repoKey, slug: "wire" }, linesInStore: true, line: 2 });
    const state = await store.specs.state(spec);
    assert.deepEqual(state?.events.map((e) => e.line), [1, 2]);
    assert.deepEqual(state?.turn, { turn: "you", needsYou: true });
    assert.deepEqual((await streamOf(store, "wire")).map((e) => e.type), ["activity", "activity"]);

    // A linesInStore frame with lines breaks the contract: refused.
    agent.send({ type: "sync", spec, linesInStore: true, lines: [{ line: 3, event: { kind: "note" } }] });
    const refused = await agent.next("synced");
    assert.match(refused.error, /no lines and no state/);

    // A normal sync of another spec on the same relay works as before, and appends its sync event.
    const other = address("normal");
    agent.send({ type: "sync", spec: other, first: true, lines: [{ line: 1, event: { kind: "created" } }], state: appState("normal"), turn: null });
    const normal = await agent.next("synced", (f) => f.spec.slug === "normal");
    assert.deepEqual(normal, { type: "synced", spec: { repoKey: other.repoKey, slug: "normal" }, line: 1 });
    assert.deepEqual((await store.specs.state(other))?.events.map((e) => e.line), [1]);
    assert.deepEqual((await streamOf(store, "normal")).map((e) => e.type), ["sync"]);
    await agent.close();
  } finally {
    await t.close();
    await store.close();
  }
});

test("R10.S1: the relay offers protocol 1.1, so a plugin that speaks it chooses 1.1 and gives its storeId", async () => {
  const store = await openShared();
  try {
    assert.equal(store.client.info.protocolVersion, "1.1");
    assert.deepEqual(store.identity(), { plugin: "fake", storeId: STORE_ID });
  } finally {
    await store.close();
  }
});

test("R10.S2: a plugin that speaks only 1.0 chooses 1.0 and gives no storeId: no store in the welcome", async () => {
  const store = await openShared(["--events", "--store-id", STORE_ID, "--only-1.0"]);
  assert.equal(store.client.info.protocolVersion, "1.0");
  assert.equal(store.identity(), null);
  const t = await relayOn(store);
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    const welcome = await agent.next("welcome");
    assert.equal("store" in welcome, false);
    await agent.close();
  } finally {
    await t.close();
    await store.close();
  }
});

test("R10.S2: a plugin with no storeId: no store in the welcome, and a linesInStore frame is refused", async () => {
  const store = await openShared(["--events"]);
  assert.equal(store.identity(), null);
  const t = await relayOn(store);
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    const welcome = await agent.next("welcome");
    assert.equal("store" in welcome, false);
    const spec = address("no-id");
    agent.send({ type: "sync", spec, linesInStore: true });
    const synced = await agent.next("synced");
    assert.equal(synced.linesInStore, true);
    assert.match(synced.error, /does not read the Mac's lines from a shared store/);
    assert.equal(await store.specs.state(spec), null);
    await agent.close();
  } finally {
    await t.close();
    await store.close();
  }
});

test("R10.S2: the JSON store: no store in the welcome, a linesInStore frame is refused, and a normal sync works", async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-lines-json-"));
  const store = openJsonStore({ dataDir: dir, deployId: "default", publicUrl: "https://relay.example.com" }, { bucket: null, log: () => {} });
  await store.start({ log: () => {}, print: () => {} });
  const t = await relayOn(store);
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    const welcome = await agent.next("welcome");
    assert.equal("store" in welcome, false);
    const spec = address("json");
    agent.send({ type: "sync", spec, linesInStore: true, first: true });
    const refused = await agent.next("synced");
    assert.match(refused.error, /Send the sync frame with its lines/);

    agent.send({ type: "sync", spec, first: true, lines: [{ line: 1, event: { kind: "created" } }], state: appState("json"), turn: null });
    const synced = await agent.next("synced");
    assert.deepEqual(synced, { type: "synced", spec: { repoKey: spec.repoKey, slug: "json" }, line: 1 });
    assert.equal((await store.specs.state(spec))?.line, 1);
    await sleep(50);
    await agent.close();
  } finally {
    await t.close();
    await store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
