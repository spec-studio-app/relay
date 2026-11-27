// The plugin store (relay-plugin-host R1 to R4): the start, the handshake,
// the waits, the conflicts, the grace period, the restart of a stdio plugin,
// and the close. The memory plugin comes from test/plugin.ts; the fake plugin
// (test/fixtures/fakePlugin.mjs) gives the failures.
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig, type PluginSettings } from "../src/config.js";
import { MemoryBucket } from "../src/bucket.js";
import { Attachments } from "../src/attachments.js";
import { attachmentPrefix } from "../src/activities.js";
import { MessageTools } from "../src/messages.js";
import { openStore } from "../src/store/index.js";
import { openPluginStore, type PluginRelayStore } from "../src/store/plugin/index.js";
import { StoreUnavailableError, STORE_UNAVAILABLE_TEXT } from "../src/store/errors.js";
import { StartupError } from "../src/startupCommon.js";
import { newPairCodeIn } from "../src/startupCommon.js";
import { pairCodeHash } from "../src/pairing.js";
import { specAddress } from "../src/store/address.js";
import type { Logger } from "../src/log.js";
import { FAKE_PLUGIN, httpMemoryPlugin, memoryPlugin, memoryStore, skipWithoutPlugin } from "./plugin.js";

const skip = skipWithoutPlugin();
const BCRYPT = "$2b$12$" + "a".repeat(53);

function repo(): string {
  return `/Users/me/code/plug-${randomBytes(4).toString("hex")}`;
}

function show(slug: string, over: Record<string, unknown> = {}) {
  return { slug, title: `Title of ${slug}`, stage: "build", status: "working", mode: "autopilot", createdAt: "2026-11-01T00:00:00Z", ...over };
}

function fake(flags: string[], settings: Record<string, string> = {}): PluginSettings {
  return { transport: "stdio", command: process.execPath, args: [FAKE_PLUGIN, ...flags], settings, from: "RELAY_PLUGIN" };
}

async function openFake(flags: string[], o: { settings?: Record<string, string>; log?: Logger } = {}): Promise<PluginRelayStore> {
  return openPluginStore(
    { deployId: "default", publicUrl: "https://relay.example.com", plugin: fake(flags, o.settings) },
    { bucket: null, log: o.log ?? (() => {}), restart: { minMs: 50, maxMs: 200 } },
  );
}

async function startError(flags: string[], settings: Record<string, string> = {}): Promise<string> {
  try {
    const store = await openFake(flags, { settings });
    await store.close();
  } catch (error) {
    assert.ok(error instanceof StartupError, `a StartupError, not ${(error as Error).name}`);
    return error.message;
  }
  assert.fail("the start did not fail");
}

async function eventually<T>(check: () => Promise<T | null | undefined | false> | T | null | undefined | false, ms = 10_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > end) throw new Error("The condition did not pass in time.");
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

// ---- R1: the settings choose the plugin ----

test("R1.S2: RELAY_PLUGIN starts the command, with its arguments, as the store", { skip }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-plugin-cmd-"));
  const config = loadConfig({
    PUBLIC_URL: "https://relay.example.com",
    OWNER_PASSWORD_HASH: BCRYPT,
    RELAY_PLUGIN: `"${memoryPlugin()}" --data "${join(dir, "my data.json")}"`,
  });
  const store = await openStore(config, { bucket: null, log: () => {} });
  try {
    assert.equal(store.kind, "plugin");
    await store.start({ log: () => {}, print: () => {} });
    const spec = specAddress(repo(), "cmd");
    await store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("cmd"), turn: null });
    assert.equal((await store.specs.state(spec))?.line, 1);
  } finally {
    await store.close();
  }
  assert.ok(existsSync(join(dir, "my data.json")), "the plugin got the quoted argument");
  rmSync(dir, { recursive: true, force: true });
});

test("R1.S4: the RELAY_PLUGIN_ settings reach the handshake", async () => {
  const store = await openFake(["--echo-settings"], { settings: { address: "temporal:7233", api_key: "secret-key" } });
  try {
    assert.deepEqual(JSON.parse(store.client.info.plugin.repo), { address: "temporal:7233", api_key: "secret-key" });
  } finally {
    await store.close();
  }
});

// ---- R2: the handshake at start ----

test("R2.S1: the log names the plugin, its version, its repo and its capabilities, with no secret", async () => {
  const logs: Array<Record<string, unknown>> = [];
  const store = await openFake([], { settings: { api_key: "very-secret-value" }, log: (e) => logs.push(e) });
  try {
    const line = logs.find((e) => e.event === "plugin_handshake");
    assert.deepEqual(line && { plugin: line.plugin, version: line.version, repo: line.repo, capabilities: line.capabilities }, {
      plugin: "fake",
      version: "9.9.9",
      repo: "https://example.com/fake-plugin",
      capabilities: "state",
    });
    assert.ok(!JSON.stringify(logs).includes("very-secret-value"));
  } finally {
    await store.close();
  }
});

test("R2.S2: a failed handshake or a command that does not start stops the start with the reason and what to check", async () => {
  const refused = await startError(["--refuse-settings"], { address: "x" });
  assert.match(refused, /The relay cannot use the plugin from RELAY_PLUGIN/);
  assert.match(refused, /The setting address is required\./);
  assert.match(refused, /Check the command and its arguments in RELAY_PLUGIN, and the RELAY_PLUGIN_ settings\./);

  let missing = "";
  try {
    await openPluginStore(
      { deployId: "default", publicUrl: "https://relay.example.com", plugin: { transport: "stdio", command: "/no/such/plugin", args: [], settings: {}, from: "RELAY_PLUGIN" } },
      { bucket: null, log: () => {} },
    );
  } catch (error) {
    assert.ok(error instanceof StartupError);
    missing = error.message;
  }
  assert.match(missing, /\/no\/such\/plugin/);
  assert.match(missing, /did not start/);

  let http = "";
  try {
    await openPluginStore(
      { deployId: "default", publicUrl: "https://relay.example.com", plugin: { transport: "http", url: "http://127.0.0.1:9/rpc", token: "secret-token", settings: {}, from: "RELAY_PLUGIN_URL" } },
      { bucket: null, log: () => {} },
    );
  } catch (error) {
    assert.ok(error instanceof StartupError);
    http = error.message;
  }
  assert.match(http, /RELAY_PLUGIN_URL, RELAY_PLUGIN_TOKEN/);
  assert.ok(!http.includes("secret-token"));
});

test("R2.S2: the memory plugin over HTTP with a wrong token stops the start", { skip }, async () => {
  const sidecar = await httpMemoryPlugin();
  try {
    await assert.rejects(
      openPluginStore(
        { deployId: "default", publicUrl: "https://relay.example.com", plugin: { transport: "http", url: sidecar.url, token: "wrong-token", settings: {}, from: "RELAY_PLUGIN_URL" } },
        { bucket: null, log: () => {} },
      ),
      (error: Error) => error instanceof StartupError && /401/.test(error.message) && !error.message.includes("wrong-token"),
    );
  } finally {
    await sidecar.stop();
  }
});

test("R2.S3: a plugin with no shared protocol version: the message names both lists", async () => {
  const message = await startError(["--version2"]);
  assert.match(message, /the plugin speaks 2\.0/);
  assert.match(message, /The relay speaks protocol version 1\.0/);
});

test("R2.S4: the first start makes the config and prints the code; a second start does not", { skip }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-plugin-first-"));
  const dataFile = join(dir, "memory.json");
  const first = await memoryStore("stdio", { dataFile });
  let code: string;
  try {
    assert.equal(first.printed.length, 1);
    code = /Pairing code: (\S+)/.exec(first.printed[0])![1];
    assert.ok(await first.store.config.pairCodeValid(pairCodeHash(code)));
    assert.ok(!JSON.stringify(first.logs).includes(code), "the log has no pairing code");
    assert.ok(first.logs.some((e) => e.event === "relay_config_started"));
  } finally {
    await first.close();
  }
  const second = await memoryStore("stdio", { dataFile });
  try {
    assert.equal(second.printed.length, 0);
    assert.ok(await second.store.config.pairCodeValid(pairCodeHash(code)), "the first code still works");
  } finally {
    await second.close();
  }
  rmSync(dir, { recursive: true, force: true });
});

// ---- R3: the relay works the same with a plugin ----

test("R3.S2: writes to one spec at the same time all apply, each with its own seq", { skip }, async () => {
  const s = await memoryStore("http");
  try {
    const spec = specAddress(repo(), "race");
    await s.store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("race"), turn: null });
    // A second store on the same sidecar is a second writer: its writes race this store's writes.
    const other = await openPluginStore(
      { deployId: "default", publicUrl: "https://relay.example.com", plugin: s.store.plugin },
      { bucket: null, log: () => {} },
    );
    try {
      const sends = await Promise.all(
        Array.from({ length: 8 }, (_, i) => (i % 2 ? s.store.specs : other.specs).send(spec, { text: `m${i}` }).then((r) => r.seq, (e: Error) => e)),
      );
      const seqs = sends.filter((r): r is number => typeof r === "number");
      // A call can lose 5 races in a row and fail as "not available"; every other call has its own seq.
      for (const r of sends) if (typeof r !== "number") assert.ok(r instanceof StoreUnavailableError, String(r));
      assert.equal(new Set(seqs).size, seqs.length);
      const state = await s.store.specs.state(spec);
      assert.deepEqual(state!.mac.messages.map((m) => m.seq).sort((a, b) => a - b), [...seqs].sort((a, b) => a - b));
    } finally {
      await other.close();
    }
  } finally {
    await s.close();
  }
});

test("R3.S2: after 5 conflicts in a row, the call fails as the store not available", async () => {
  const store = await openFake(["--conflict"]);
  try {
    const spec = specAddress(repo(), "conflict");
    await assert.rejects(
      store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("conflict"), turn: null }),
      (e: Error) => e instanceof StoreUnavailableError && e.message === STORE_UNAVAILABLE_TEXT,
    );
  } finally {
    await store.close();
  }
});

for (const transport of ["stdio", "http"] as const) {
  test(`R3.S3: a long poll ends at the Mac's reply, before waitMs ends (${transport})`, { skip }, async () => {
    const s = await memoryStore(transport);
    try {
      const spec = specAddress(repo(), "poll");
      await s.store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("poll"), turn: null });
      const started = Date.now();
      const reading = s.store.specs.read(spec, undefined, 10_000);
      await new Promise((resolve) => setTimeout(resolve, 300));
      // A write with no message first: the read waits again.
      await s.store.specs.refresh(spec, { state: show("poll", { status: "waiting" }), turn: null });
      await s.store.specs.sync(spec, { lines: [{ line: 2, event: { kind: "message", from: "claude", id: "r1", text: "The reply", t: "2026-11-27T10:00:00Z" } }] });
      const result = await reading;
      assert.deepEqual(result.messages.map((m) => m.text), ["The reply"]);
      assert.ok(Date.now() - started < 3000, `the read took ${Date.now() - started} ms`);
    } finally {
      await s.close();
    }
  });
}

test("R3.S3: a plugin with no watch: a read polls and ends at the reply", async () => {
  const store = await openFake([]);
  try {
    const spec = specAddress(repo(), "nowatch");
    await store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("nowatch"), turn: null });
    const reading = store.specs.read(spec, undefined, 10_000);
    await new Promise((resolve) => setTimeout(resolve, 200));
    await store.specs.sync(spec, { lines: [{ line: 2, event: { kind: "message", from: "claude", id: "r1", text: "Polled", t: "2026-11-27T10:00:00Z" } }] });
    const started = Date.now();
    assert.deepEqual((await reading).messages.map((m) => m.text), ["Polled"]);
    assert.ok(Date.now() - started < 2500);
  } finally {
    await store.close();
  }
});

/** A merged spec with empty inboxes, and an attachment in the bucket. */
async function mergedSpec(s: { store: PluginRelayStore }, bucket: MemoryBucket, slug: string) {
  const spec = specAddress(repo(), slug);
  await s.store.specs.sync(spec, {
    lines: [
      { line: 1, event: { kind: "status" } },
      { line: 2, event: { kind: "stage", to: "merged" } },
    ],
    first: true,
    state: show(slug, { stage: "merged" }),
    turn: null,
  });
  const key = `${attachmentPrefix("default", spec.repoKey, slug)}file.txt`;
  bucket.objects.set(key, { body: Buffer.from("x"), contentType: "text/plain", metadata: {} });
  return { spec, key };
}

test("R3.S4: a finished spec closes after its grace period, and stays in a list with all", { skip }, async () => {
  const bucket = new MemoryBucket();
  const s = await memoryStore("stdio", { graceMs: 400, bucket });
  try {
    const { spec, key } = await mergedSpec(s, bucket, "grace");
    assert.ok((await s.store.specs.state(spec))?.finished, "the spec is finished");
    await eventually(async () => (await s.store.specs.state(spec)) === null);
    assert.equal(bucket.objects.has(key), false, "the attachments are gone");
    const all = await s.store.specs.list({ all: true, repoKey: spec.repoKey });
    assert.deepEqual(all.map((l) => ({ slug: l.slug, running: l.running })), [{ slug: "grace", running: false }]);
    assert.deepEqual(await s.store.specs.list({ repoKey: spec.repoKey }), []);
    // A late line is not applied; a first sync starts the spec again.
    assert.deepEqual(await s.store.specs.sync(spec, { lines: [{ line: 3, event: { kind: "status" } }] }), { line: 3, applied: false });
    assert.equal(await s.store.specs.state(spec), null);
    await s.store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("grace"), turn: null });
    assert.equal((await s.store.specs.list({ all: true, repoKey: spec.repoKey })).map((l) => l.running).join(), "true");
  } finally {
    await s.close();
  }
});

test("R3.S5: the grace period survives a restart of the relay", { skip }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-plugin-grace-"));
  const dataFile = join(dir, "memory.json");
  const bucket = new MemoryBucket();
  const first = await memoryStore("stdio", { graceMs: 1500, bucket, dataFile });
  const { spec } = await mergedSpec(first, bucket, "restart");
  await first.close();
  const second = await memoryStore("stdio", { graceMs: 1500, bucket, dataFile });
  try {
    assert.ok(await second.store.specs.state(spec), "the spec is there after the restart");
    await eventually(async () => (await second.store.specs.state(spec)) === null, 8000);
  } finally {
    await second.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("R3.S6: relay pair-code next to a relay on an HTTP sidecar: the new code works, the old one does not", { skip }, async () => {
  const sidecar = await httpMemoryPlugin();
  const plugin: PluginSettings = { transport: "http", url: sidecar.url, token: sidecar.token, settings: {}, from: "RELAY_PLUGIN_URL" };
  const config = { deployId: "default", publicUrl: "https://relay.example.com", plugin };
  const printed: string[] = [];
  const relay = await openPluginStore(config, { bucket: null, log: () => {} });
  try {
    await relay.start({ log: () => {}, print: (t) => printed.push(t) });
    const oldCode = /Pairing code: (\S+)/.exec(printed[0])![1];
    // `relay pair-code`: its own store on the same sidecar.
    const command = await openPluginStore(config, { bucket: null, log: () => {} });
    const newCode = await newPairCodeIn(command.config);
    await command.close();
    assert.equal(await relay.config.pair(pairCodeHash(oldCode), "a".repeat(64), new Date()), false);
    assert.equal(await relay.config.pair(pairCodeHash(newCode), "b".repeat(64), new Date()), true);
    assert.equal(await relay.config.paired(), 1);
    // The command did not stop the sidecar.
    assert.equal((await relay.client.ping()).status, "ok");
  } finally {
    await relay.close();
    await sidecar.stop();
  }
});

// ---- R4: a stdio plugin that exits ----

test("R4.S1: a plugin that exits: the log has the exit code and stderr, and the plugin comes back with the same handshake", async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-plugin-exit-"));
  const exitFile = join(dir, "exit");
  const logs: Array<Record<string, unknown>> = [];
  const store = await openFake(["--exit-file", exitFile], { log: (e) => logs.push(e) });
  try {
    writeFileSync(exitFile, "");
    // The call that meets the exit fails as not available.
    await assert.rejects(store.config.paired(), (e: Error) => e instanceof StoreUnavailableError);
    const exited = logs.find((e) => e.event === "plugin_exited");
    assert.equal(exited?.code, 3);
    assert.match(String(exited?.stderr), /told to exit/);
    assert.equal(exited?.restartInMs, 50);
    await eventually(() => logs.some((e) => e.event === "plugin_restarted"));
    assert.equal(logs.filter((e) => e.event === "plugin_handshake").length, 2, "the same handshake again");
    assert.equal(await store.config.paired(), 0);
  } finally {
    await store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("R4.S2: while the plugin is down a tool says the store is not available; after the restart it works with the data from before", { skip }, async () => {
  const s = await memoryStore("stdio", { restart: { minMs: 1500, maxMs: 1500 } });
  try {
    const path = repo();
    const spec = specAddress(path, "down");
    await s.store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("down"), turn: null });
    const tools = new MessageTools({ specs: s.store.specs, push: () => {}, attachments: new Attachments({ bucket: null, deployId: "default", maxBytes: 1024 }) });
    process.kill(s.store.client.pid!, "SIGKILL");
    await eventually(() => s.logs.some((e) => e.event === "plugin_exited"));
    assert.deepEqual(await tools.answer("send_message", { repo: path, slug: "down", text: "Hi" }), { ok: false, text: STORE_UNAVAILABLE_TEXT });
    await eventually(() => s.logs.some((e) => e.event === "plugin_restarted"), 8000);
    const sent = await tools.answer("send_message", { repo: path, slug: "down", text: "Hi" });
    assert.equal(sent.ok, true, sent.text);
    assert.equal((await s.store.specs.state(spec))?.line, 1, "the data from before the exit");
  } finally {
    await s.close();
  }
});

test("R4.S2: the tools answer the text of StoreUnavailableError", async () => {
  const down = { send: async () => Promise.reject(new StoreUnavailableError()) } as unknown as ConstructorParameters<typeof MessageTools>[0]["specs"];
  const tools = new MessageTools({ specs: down, push: () => {}, attachments: new Attachments({ bucket: null, deployId: "default", maxBytes: 1024 }) });
  assert.deepEqual(await tools.answer("send_message", { repo: "/r", slug: "s", text: "Hi" }), { ok: false, text: "The relay's store is not available. Try again in a minute." });
});

test("R4.S3: the relay stops a stdio plugin with plugin.shutdown; a sidecar gets no shutdown", { skip }, async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-plugin-stop-"));
  const record = join(dir, "calls");
  const logs: Array<Record<string, unknown>> = [];
  const store = await openFake(["--record", record], { log: (e) => logs.push(e) });
  await store.close();
  assert.match(readFileSync(record, "utf8"), /plugin\.shutdown/);
  const stopped = logs.find((e) => e.event === "plugin_stopped");
  assert.equal(stopped?.code, 0);
  rmSync(dir, { recursive: true, force: true });

  const s = await memoryStore("http");
  await s.store.close();
  // The sidecar still answers: the relay did not shut it down.
  const again = await openPluginStore({ deployId: "default", publicUrl: "https://relay.example.com", plugin: s.store.plugin }, { bucket: null, log: () => {} });
  assert.equal((await again.client.ping()).status, "ok");
  await again.close();
  await s.close();
});
