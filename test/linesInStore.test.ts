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
import { ACTIVITY_EVENT, APP_SPECS, FIRST_SYNC_TAIL, NOT_UPLOADED_TEXT, firstSyncLines, specKey, specStream } from "../src/store/plugin/pluginSpecStore.js";
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

async function openShared(flags: string[] = ["--events", "--store-id", STORE_ID], o: { graceMs?: number } = {}): Promise<PluginRelayStore> {
  const store = await openPluginStore(
    { deployId: "default", publicUrl: "https://relay.example.com", plugin: { transport: "stdio", command: process.execPath, args: [FAKE_PLUGIN, ...flags], settings: {}, from: "RELAY_PLUGIN" } },
    { bucket: null, log: () => {}, ...(o.graceMs !== undefined ? { graceMs: o.graceMs } : {}) },
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

async function eventually(check: () => Promise<boolean>, ms = 10_000): Promise<void> {
  const end = Date.now() + ms;
  while (!(await check())) {
    if (Date.now() > end) throw new Error("The condition did not come true in time.");
    await sleep(25);
  }
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

test("T41: on a shared store, the close of a spec synced with normal frames keeps the Mac's activity stream", async () => {
  const store = await openShared(undefined, { graceMs: 300 });
  try {
    const spec = address("merged-shared");
    const created = { kind: "created", t: "2026-11-01T00:00:01Z" };
    const merged = { kind: "merged", t: "2026-11-01T00:00:02Z" };
    // The Mac's history is in the stream; the relay got the lines in normal sync frames (no app cursor).
    await macCommit(store, "merged-shared", appState("merged-shared", { stage: "merged" }), [created, merged]);
    await store.specs.sync(spec, { lines: [{ line: 1, event: created }, { line: 2, event: merged }], first: true, state: appState("merged-shared", { stage: "merged" }), turn: null });
    assert.ok((await store.specs.state(spec))?.finished);
    await eventually(async () => (await store.specs.state(spec)) === null);
    const left = await streamOf(store, "merged-shared");
    assert.deepEqual(
      left.filter((e) => e.type === ACTIVITY_EVENT).map((e) => (e.data as { kind: string }).kind),
      ["created", "merged"],
      "the Mac's activity events stay",
    );
  } finally {
    await store.close();
  }
});

test("T41: on a separate store, the close of a spec deletes the stream with only the relay's own events", async () => {
  const store = await openShared(["--events"], { graceMs: 300 });
  try {
    const spec = address("merged-own");
    await store.specs.sync(spec, {
      lines: [{ line: 1, event: { kind: "created" } }, { line: 2, event: { kind: "merged" } }],
      first: true,
      state: appState("merged-own", { stage: "merged" }),
      turn: null,
    });
    assert.deepEqual((await streamOf(store, "merged-own")).map((e) => e.type), ["sync"]);
    await eventually(async () => (await store.specs.state(spec)) === null);
    await eventually(async () => (await streamOf(store, "merged-own")).length === 0);
  } finally {
    await store.close();
  }
});

test("T43: an activity line that is not a JSON object counts for the line number, but does not go into the mirror", async () => {
  const store = await openShared();
  try {
    const spec = address("text-line");
    await macCommit(store, "text-line", appState("text-line"), [
      { kind: "created", t: "2026-11-01T00:00:01Z" },
      "an old line that is not JSON",
      { kind: "note", t: "2026-11-01T00:00:03Z", text: "After." },
    ]);
    assert.deepEqual(await store.specs.syncFromStore(spec, { first: true }), { line: 3, applied: true });
    const state = await store.specs.state(spec);
    assert.deepEqual(state?.events.map((e) => e.line), [1, 3]);
    assert.ok(state?.events.every((e) => typeof e.event.kind === "string"));
  } finally {
    await store.close();
  }
});

test("T44: a plugin that refuses the kept cursor (the stream was deleted): the sync reads from the start, and does not fail", async () => {
  const store = await openShared();
  try {
    const spec = address("refused");
    await macCommit(store, "refused", appState("refused"), [
      { kind: "created", t: "2026-11-01T00:00:01Z" },
      { kind: "note", t: "2026-11-01T00:00:02Z", text: "One." },
    ]);
    assert.deepEqual(await store.specs.syncFromStore(spec, { first: true }), { line: 2, applied: true });
    // The stream goes and comes back: the fake plugin refuses the old cursor.
    await store.client.events.delete(specStream(spec));
    await macCommit(store, "refused", appState("refused"), [
      { kind: "created", t: "2026-11-01T00:00:01Z" },
      { kind: "note", t: "2026-11-01T00:00:02Z", text: "One." },
      { kind: "note", t: "2026-11-01T00:00:03Z", text: "Two." },
    ]);
    assert.deepEqual(await store.specs.syncFromStore(spec, {}), { line: 3, applied: true });
    assert.deepEqual((await store.specs.state(spec))?.events.map((e) => e.line), [1, 2, 3]);
    // The next sync goes on from the new cursor.
    assert.deepEqual(await store.specs.syncFromStore(spec, {}), { line: 3, applied: false });
  } finally {
    await store.close();
  }
});

test("T44: a new spec with the same slug (another createdAt): the mirror starts again from the stream's start", async () => {
  const store = await openShared();
  try {
    const spec = address("again");
    const old = appState("again", { createdAt: "2026-11-01T00:00:00Z" });
    await macCommit(store, "again", old, [
      { kind: "created", t: "2026-11-01T00:00:01Z" },
      { kind: "note", t: "2026-11-01T00:00:02Z", text: "Old one." },
      { kind: "note", t: "2026-11-01T00:00:03Z", text: "Old two." },
    ]);
    await store.specs.syncFromStore(spec, { first: true });
    await store.specs.send(spec, { text: "For the old spec." });
    // The app deletes the spec (its document and its stream) and makes a new spec with the same slug.
    // The relay does not use the old cursor (on plugin-temporal it stays valid, and would number the new lines on from 3).
    const fresh = appState("again", { createdAt: "2026-11-02T00:00:00Z" });
    const doc = await store.client.state.get({ collection: APP_SPECS, key: specKey(spec) });
    assert.ok(doc.exists);
    await store.client.state.delete({ collection: APP_SPECS, key: specKey(spec), expectedVersion: doc.version });
    assert.ok((await store.client.events.delete(specStream(spec))).deleted);
    await macCommit(store, "again", fresh, []);
    // No line of the new spec yet: nothing changes.
    assert.deepEqual(await store.specs.syncFromStore(spec, {}), { line: 0, applied: false });
    assert.equal((await store.specs.state(spec))?.line, 3);
    await macCommit(store, "again", fresh, [
      { kind: "created", t: "2026-11-02T00:00:01Z" },
      { kind: "note", t: "2026-11-02T00:00:02Z", text: "New one." },
    ]);
    assert.deepEqual(await store.specs.syncFromStore(spec, {}), { line: 2, applied: true });
    const state = await store.specs.state(spec);
    assert.ok(state);
    assert.deepEqual(state.events.map((e) => [e.line, e.event.text ?? e.event.kind]), [[1, "created"], [2, "New one."]]);
    assert.equal(state.state?.createdAt, "2026-11-02T00:00:00Z");
    assert.equal(state.finished, false);
    assert.equal(state.mac.messages.length, 0, "the old spec's message for the Mac bounced");
  } finally {
    await store.close();
  }
});

test("T39: a linesInStore sync reads no line after the frame's toLine; the next sync goes on from there", async () => {
  const store = await openShared();
  try {
    const spec = address("to-line");
    await macCommit(store, "to-line", appState("to-line"), [1, 2, 3, 4, 5].map((n) => ({ kind: "note", t: `2026-11-01T00:00:0${n}Z`, text: `Line ${n}.` })));
    assert.deepEqual(await store.specs.syncFromStore(spec, { first: true, toLine: 3 }), { line: 3, applied: true });
    assert.deepEqual((await store.specs.state(spec))?.events.map((e) => e.line), [1, 2, 3]);
    assert.deepEqual(await store.specs.syncFromStore(spec, { toLine: 3 }), { line: 3, applied: false });
    assert.deepEqual(await store.specs.syncFromStore(spec, { toLine: 5 }), { line: 5, applied: true });
    assert.deepEqual((await store.specs.state(spec))?.events.map((e) => e.line), [1, 2, 3, 4, 5]);
  } finally {
    await store.close();
  }
});

test("T39: from an app with no toLine, a spec of an archived repo gets no line logged after the archived list's time", async () => {
  const store = await openShared();
  const t = await relayOn(store);
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, { ...HELLO, archivedRepos: [REPO] });
    await agent.next("welcome");
    await eventually(async () => (await store.config.archivedRepos()) !== null);
    const spec = address("archived");
    await macCommit(store, "archived", appState("archived"), [
      { kind: "created", t: "2026-11-01T00:00:01Z" },
      { kind: "paused", t: "2026-11-01T00:00:02Z" },
      { kind: "note", t: "2099-01-01T00:00:00Z", text: "After the archive." },
    ]);
    agent.send({ type: "sync", spec, linesInStore: true, first: true, turn: null });
    assert.equal((await agent.next("synced")).line, 2);
    assert.deepEqual((await store.specs.state(spec))?.events.map((e) => e.line), [1, 2]);
    // An app that sends toLine decides the cut itself.
    agent.send({ type: "sync", spec, linesInStore: true, turn: null, toLine: 3 });
    assert.equal((await agent.next("synced")).line, 3);
    // A toLine that is not a line number is refused.
    agent.send({ type: "sync", spec, linesInStore: true, turn: null, toLine: -1 });
    assert.match((await agent.next("synced")).error, /not valid/);
    await agent.close();
  } finally {
    await t.close();
    await store.close();
  }
});

test("T39: a first linesInStore sync takes the short first-sync lines: the tail, earlier messages, and the newest earlier status", async () => {
  const lines = Array.from({ length: 30 }, (_, i) => ({ line: i + 1, event: { kind: i === 2 ? "message" : i === 4 ? "status" : i === 6 ? "status" : "note" } as Record<string, unknown> }));
  const picked = firstSyncLines(lines);
  assert.deepEqual(picked.map((l) => l.line), [3, 7, ...Array.from({ length: FIRST_SYNC_TAIL }, (_, i) => i + 11)]);
  assert.deepEqual(firstSyncLines(lines.slice(0, 20)).length, 20);

  const store = await openShared();
  try {
    const spec = address("tail");
    await macCommit(
      store,
      "tail",
      appState("tail"),
      lines.map((l) => ({ ...l.event, t: "2026-11-01T00:00:01Z", ...(l.event.kind === "message" ? { id: "m-early", text: "Early." } : {}) })),
    );
    assert.deepEqual(await store.specs.syncFromStore(spec, { first: true }), { line: 30, applied: true });
    const state = await store.specs.state(spec);
    assert.ok(state);
    assert.equal(state.lastStatusLine, 7, "the newest earlier status change");
    assert.deepEqual(state.coordinator.messages.map((m) => m.text), ["Early."], "an earlier message");
  } finally {
    await store.close();
  }
});

test("T42: a message's files: the frame's uploads give the objectKey; without them each file has an error, and no local path", async () => {
  const store = await openShared();
  const t = await relayOn(store);
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    await agent.next("welcome");
    const file = { path: "specs/files/relay/outbox/m1-0-a.png", filename: "a.png", contentType: "image/png", size: 3, sha256: "abc" };
    await macCommit(store, "files", appState("files"), [
      { kind: "created", t: "2026-11-01T00:00:01Z" },
      { kind: "message", t: "2026-11-01T00:00:02Z", id: "m1", text: "See the file.", attachments: [file] },
    ]);
    const spec = address("files");
    // An app that sends no uploads: Claude AI is told that the file did not come.
    agent.send({ type: "sync", spec, linesInStore: true, first: true, turn: null });
    assert.equal((await agent.next("synced")).line, 2);
    const before = (await store.specs.state(spec))?.coordinator.messages[0];
    assert.deepEqual(before?.attachments, [{ filename: "a.png", contentType: "image/png", size: 3, sha256: "abc", error: NOT_UPLOADED_TEXT }]);

    // An app that uploads the files sends their objectKeys by line.
    await macCommit(store, "files", appState("files"), [
      { kind: "message", t: "2026-11-01T00:00:03Z", id: "m2", text: "And this one.", attachments: [{ ...file, path: "specs/files/relay/outbox/m2-0-a.png" }] },
    ]);
    const uploaded = { objectKey: "default/files/a.png", filename: "a.png", contentType: "image/png", size: 3, sha256: "abc" };
    agent.send({ type: "sync", spec, linesInStore: true, turn: null, attachments: [{ line: 3, attachments: [uploaded] }] });
    assert.equal((await agent.next("synced")).line, 3);
    const state = await store.specs.state(spec);
    assert.deepEqual(state?.coordinator.messages[1]?.attachments, [uploaded]);
    assert.ok(state?.events.every((e) => !JSON.stringify(e.event).includes("outbox")), "no Mac-local path in the mirror");

    // An uploads list that breaks the rules is refused.
    agent.send({ type: "sync", spec, linesInStore: true, turn: null, attachments: [{ line: 0, attachments: [] }] });
    assert.match((await agent.next("synced")).error, /not valid/);
    await agent.close();
  } finally {
    await t.close();
    await store.close();
  }
});
