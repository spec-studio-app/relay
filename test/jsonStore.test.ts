// The JSON store's own behavior (pluggable-relay-store R2, R4): the files on
// the volume, a restart, a crash during a write, the clean-up of a finished
// spec, the data folder check, and the pairing code.
import { test } from "node:test";
import assert from "node:assert/strict";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { ConfigMissingError } from "../src/store/json/jsonConfigStore.js";
import { openJsonStore, type JsonStoreOptions } from "../src/store/json/index.js";
import { specFileName } from "../src/store/json/jsonSpecStore.js";
import { writeAtomic } from "../src/store/json/files.js";
import { SpecClosingError } from "../src/store/errors.js";
import { specAddress } from "../src/store/address.js";
import { attachmentPrefix } from "../src/activities.js";
import { MemoryBucket } from "../src/bucket.js";
import { pairCodeHash } from "../src/pairing.js";
import { newPairCodeIn, StartupError } from "../src/startup.js";
import type { LogEntry } from "../src/log.js";

const REPO = "/Users/me/code/app";
const CONFIG = { deployId: "default", publicUrl: "https://relay.example.com" };

function tempDir(): string {
  return mkdtempSync(join(tmpdir(), "relay-json-"));
}

function open(dir: string, o: Partial<JsonStoreOptions> = {}) {
  return openJsonStore({ ...CONFIG, dataDir: dir }, { bucket: o.bucket ?? new MemoryBucket(), log: o.log ?? (() => {}), ...o });
}

function show(slug: string, over: Record<string, unknown> = {}) {
  return { slug, title: slug, stage: "build", status: "working", mode: "autopilot", createdAt: "2026-11-01T00:00:00Z", ...over };
}

const quiet = { log: () => {}, print: () => {} };
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test("R2.S1: the state stays after a restart", async () => {
  const dir = tempDir();
  try {
    const first = open(dir);
    await first.start(quiet);
    await first.config.setPairCode({ hash: pairCodeHash("ABCD-EFGH"), expiresAt: Date.now() + 60_000 });
    assert.equal(await first.config.pair(pairCodeHash("ABCD-EFGH"), "s".repeat(64), new Date()), true);
    await first.config.registerClient({ client_id: "c1" });
    const token = { clientId: "c1", scopes: ["mcp"], expiresAt: Date.now() + 3_600_000 };
    await first.config.issueTokens({ access: { hash: "a1", token }, refresh: { hash: "r1", token } });
    const spec = specAddress(REPO, "keep");
    await first.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }, { line: 2, event: { kind: "status" } }], first: true, state: show("keep"), turn: null });
    await first.specs.send(spec, { text: "Waits for the Mac" });
    await first.close();

    const second = open(dir);
    const started = await second.start(quiet);
    assert.equal(started.pairCode, null, "a restart makes no new pairing code");
    assert.equal(await second.config.verifyAgent("s".repeat(64)), true);
    assert.equal((await second.config.token("access", "a1"))?.clientId, "c1");
    const state = await second.specs.state(spec);
    assert.equal(state?.line, 2);
    assert.deepEqual(state?.mac.messages.map((m) => m.text), ["Waits for the Mac"]);
    assert.deepEqual((await second.specs.list({ pendingForMac: true })).map((s) => s.slug), ["keep"]);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("R2.S2: a crash during a write leaves the old file whole, and a start removes the temporary file", async () => {
  const dir = tempDir();
  try {
    const path = join(dir, "file.json");
    await writeAtomic(path, JSON.stringify({ n: 1 }));
    await assert.rejects(
      writeAtomic(path, JSON.stringify({ n: 2 }), {
        afterTempWrite: () => {
          throw new Error("the process stops here");
        },
      }),
    );
    assert.deepEqual(JSON.parse(readFileSync(path, "utf8")), { n: 1 });

    // A store whose write fails keeps its state as before the write.
    let fail = false;
    const store = open(dir, { writeHooks: { afterTempWrite: () => { if (fail) throw new Error("crash"); } } });
    await store.start(quiet);
    const spec = specAddress(REPO, "crash");
    await store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("crash"), turn: null });
    fail = true;
    await assert.rejects(store.specs.sync(spec, { lines: [{ line: 2, event: { kind: "status" } }] }));
    assert.equal((await store.specs.state(spec))?.line, 1);
    await store.close();

    // A temporary file that a crash left: the next start removes it and reads the old file.
    const specs = join(dir, "specs");
    writeFileSync(join(specs, `${specFileName("default", spec.repoKey, "crash")}.123.abcd.tmp`), "{ half");
    const again = open(dir);
    await again.start(quiet);
    assert.equal((await again.specs.state(spec))?.line, 1);
    assert.deepEqual(readdirSync(specs).filter((n) => n.endsWith(".tmp")), []);
    await again.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("R2.S3: writes to one spec do not interleave", async () => {
  const dir = tempDir();
  try {
    const store = open(dir);
    await store.start(quiet);
    const spec = specAddress(REPO, "busy");
    await store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("busy"), turn: null });
    const lines = Array.from({ length: 10 }, (_, i) => ({ line: i + 2, event: { kind: "status" } }));
    await Promise.all([
      store.specs.sync(spec, { lines }),
      ...Array.from({ length: 20 }, (_, i) => store.specs.send(spec, { text: `m${i}` })),
    ]);
    await store.close();
    const again = open(dir);
    await again.start(quiet);
    const state = await again.specs.state(spec);
    assert.equal(state?.line, 11);
    assert.deepEqual(state?.mac.messages.map((m) => m.seq), Array.from({ length: 20 }, (_, i) => i + 1));
    await again.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

class GatedBucket extends MemoryBucket {
  release: () => void = () => {};
  readonly gate = new Promise<void>((resolve) => (this.release = resolve));
  started = false;
  override async removePrefix(prefix: string): Promise<number> {
    this.started = true;
    await this.gate;
    return super.removePrefix(prefix);
  }
}

test("R2.S5: a finished spec is removed with its attachments after its grace period; calls during the clean-up are refused", async () => {
  const dir = tempDir();
  try {
    const bucket = new GatedBucket();
    const store = open(dir, { bucket, graceMs: 1500 });
    await store.start(quiet);
    const spec = specAddress(REPO, "done");
    const key = `${attachmentPrefix("default", spec.repoKey, "done")}a.png`;
    await bucket.put(key, Buffer.from("x"), "image/png", {});
    await store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "merged" } }], first: true, state: show("done", { stage: "merged" }), turn: null });
    const file = join(dir, "specs", specFileName("default", spec.repoKey, "done"));
    assert.ok(existsSync(file));
    // A call in the period starts it again.
    await sleep(1000);
    await store.specs.read(spec, 0, 0);
    await sleep(1000);
    assert.equal(bucket.started, false, "the period started again with the read");
    for (let i = 0; i < 200 && !bucket.started; i++) await sleep(20);
    assert.equal(bucket.started, true);
    await assert.rejects(store.specs.send(spec, { text: "Late" }), (error) => error instanceof SpecClosingError);
    // A late line of the closing spec changes nothing; a first sync must come again.
    assert.deepEqual(await store.specs.sync(spec, { lines: [{ line: 2, event: { kind: "status" } }] }), { line: 2, applied: false });
    await assert.rejects(store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true }), (error) => error instanceof SpecClosingError);
    bucket.release();
    for (let i = 0; i < 250 && existsSync(file); i++) await sleep(20);
    assert.equal(existsSync(file), false);
    assert.equal(bucket.objects.has(key), false);
    assert.equal(await store.specs.state(spec), null);
    // A late line of the closed spec starts nothing; it shows in lists with all.
    assert.deepEqual(await store.specs.sync(spec, { lines: [{ line: 3, event: { kind: "status" } }] }), { line: 3, applied: false });
    assert.equal(await store.specs.state(spec), null);
    const all = await store.specs.list({ all: true });
    assert.deepEqual(all.map((s) => [s.slug, s.running, s.memo?.stage]), [["done", false, "merged"]]);
    assert.deepEqual(await store.specs.list(), []);
    // A first sync of a new spec with the slug starts it again.
    assert.deepEqual(await store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("done", { createdAt: "2026-11-03T00:00:00Z" }), turn: null }), { line: 1, applied: true });
    await store.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("R2.S5: a relay that restarts in the grace period still removes the spec when the period ends", async () => {
  const dir = tempDir();
  try {
    const first = open(dir, { graceMs: 400 });
    await first.start(quiet);
    const spec = specAddress(REPO, "restart");
    await first.specs.sync(spec, { lines: [{ line: 1, event: { kind: "merged" } }], first: true, state: show("restart", { stage: "merged" }), turn: null });
    await first.close();
    const file = join(dir, "specs", specFileName("default", spec.repoKey, "restart"));
    const second = open(dir, { graceMs: 400 });
    await second.start(quiet);
    assert.ok(existsSync(file));
    for (let i = 0; i < 100 && existsSync(file); i++) await sleep(20);
    assert.equal(existsSync(file), false);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("R2.S6: a data folder that the relay cannot write stops the start with a message that names RELAY_DATA_DIR", async (t) => {
  if (process.getuid?.() === 0) return t.skip("root can write any folder");
  const dir = tempDir();
  try {
    chmodSync(dir, 0o500);
    const store = open(join(dir, "data"));
    await assert.rejects(store.start(quiet), (error) => error instanceof StartupError && /RELAY_DATA_DIR/.test(error.message));
  } finally {
    chmodSync(dir, 0o700);
    rmSync(dir, { recursive: true, force: true });
  }
});

test("R4.S1: the first start prints the pairing text; a second start does not", async () => {
  const dir = tempDir();
  try {
    const printed: string[] = [];
    const first = open(dir);
    const result = await first.start({ log: () => {}, print: (t) => printed.push(t) });
    assert.ok(result.pairCode);
    assert.match(printed.join("\n"), /Pairing code: /);
    assert.match(printed.join("\n"), /studio agent pair wss:\/\/relay\.example\.com\/agent/);
    assert.equal(await first.config.pairCodeValid(pairCodeHash(result.pairCode!)), true);
    await first.close();
    printed.length = 0;
    const second = open(dir);
    assert.equal((await second.start({ log: () => {}, print: (t) => printed.push(t) })).pairCode, null);
    assert.deepEqual(printed, []);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("R4.S6: relay pair-code on the data folder of a running relay: the new code pairs, the old one fails", async () => {
  const dir = tempDir();
  try {
    const relay = open(dir);
    const old = (await relay.start(quiet)).pairCode!;
    // The command runs as another store on the same folder (another process in production).
    const command = open(dir);
    const code = await newPairCodeIn(command.config);
    assert.equal(await relay.config.pair(pairCodeHash(old), "a".repeat(64), new Date()), false);
    assert.equal(await relay.config.pair(pairCodeHash(code), "b".repeat(64), new Date()), true);
    await relay.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a slug is never part of a file name", () => {
  const name = specFileName("default", "abc", "../../etc/passwd");
  assert.match(name, /^[0-9a-f]{32}\.json$/);
});

test("review: relay pair-code in other processes while the relay writes tokens: no write is lost", async () => {
  const dir = tempDir();
  try {
    const relay = open(dir);
    await relay.start(quiet);
    const store = fileURLToPath(new URL("../src/store/json/index.js", import.meta.url));
    const startup = fileURLToPath(new URL("../src/startupCommon.js", import.meta.url));
    const script = `
      const { openJsonStore } = await import(${JSON.stringify(store)});
      const { newPairCodeIn } = await import(${JSON.stringify(startup)});
      const s = openJsonStore({ dataDir: ${JSON.stringify(dir)}, deployId: "default", publicUrl: "https://r" }, { bucket: null, log: () => {} });
      for (let i = 0; i < 10; i++) await newPairCodeIn(s.config);`;
    const commands = Array.from({ length: 3 }, () =>
      new Promise<void>((resolve, reject) => {
        const child = spawn(process.execPath, ["--input-type=module", "-e", script], { stdio: "inherit" });
        child.on("exit", (code) => (code === 0 ? resolve() : reject(new Error(`exit ${code}`))));
      }),
    );
    const token = { clientId: "c1", scopes: ["mcp"], expiresAt: Date.now() + 3_600_000 };
    const writes = Array.from({ length: 50 }, (_, i) =>
      relay.config.issueTokens({ access: { hash: `a${i}`, token }, refresh: { hash: `r${i}`, token } }),
    );
    await Promise.all([...commands, ...writes]);
    for (let i = 0; i < 50; i++) assert.ok(await relay.config.token("access", `a${i}`), `token a${i} was lost`);
    // The last code from the commands is the live code, and it pairs once.
    const last = JSON.parse(readFileSync(join(dir, "pair-code.json"), "utf8")) as { hash: string };
    assert.equal(await relay.config.pair(last.hash, "p".repeat(64), new Date()), true);
    assert.equal(await relay.config.pair(last.hash, "q".repeat(64), new Date()), false);
    await relay.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
test("review: a read in the grace period is saved, so a restart keeps the period", async () => {
  const dir = tempDir();
  try {
    const first = open(dir, { graceMs: 800 });
    await first.start(quiet);
    const spec = specAddress(REPO, "reread");
    await first.specs.sync(spec, { lines: [{ line: 1, event: { kind: "merged" } }], first: true, state: show("reread", { stage: "merged" }), turn: null });
    await sleep(500);
    // A read that changes nothing (no message): it still counts.
    await first.specs.read(spec, 0, 0);
    await first.close();
    const second = open(dir, { graceMs: 800 });
    await second.start(quiet);
    const file = join(dir, "specs", specFileName("default", spec.repoKey, "reread"));
    await sleep(500);
    assert.ok(existsSync(file), "the period counts from the read, not from the sync");
    for (let i = 0; i < 100 && existsSync(file); i++) await sleep(20);
    assert.equal(existsSync(file), false);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("review: a call that changes nothing does not start the grace period again", async () => {
  const dir = tempDir();
  try {
    const store = open(dir, { graceMs: 800 });
    await store.start(quiet);
    const spec = specAddress(REPO, "noop");
    const start = Date.now();
    await store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "merged" } }], first: true, state: show("noop", { stage: "merged" }), turn: null });
    const file = join(dir, "specs", specFileName("default", spec.repoKey, "noop"));
    await sleep(400);
    assert.deepEqual(await store.specs.ack(spec, 99), { removed: false });
    assert.deepEqual(await store.specs.sync(spec, { lines: [{ line: 1, event: { kind: "merged" } }] }), { line: 1, applied: false });
    for (let i = 0; i < 100 && existsSync(file); i++) await sleep(20);
    assert.equal(existsSync(file), false);
    assert.ok(Date.now() - start < 1150, "the spec closed at the end of the first period");
    await store.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("review: relay pair-code on a data folder with no config.json fails and makes nothing", async () => {
  const dir = tempDir();
  try {
    const command = open(dir);
    await assert.rejects(newPairCodeIn(command.config), (error) => error instanceof ConfigMissingError && /RELAY_DATA_DIR/.test(error.message));
    assert.equal(existsSync(join(dir, "config.json")), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a spec file that does not parse stops the start with a message that names it; a removed file lets the spec come again", async () => {
  const dir = tempDir();
  try {
    const first = open(dir);
    await first.start(quiet);
    const spec = specAddress(REPO, "broken");
    await first.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("broken"), turn: null });
    await first.close();
    const name = specFileName("default", spec.repoKey, "broken");
    const path = join(dir, "specs", name);
    writeFileSync(path, "{ not json");
    const logs: LogEntry[] = [];
    await assert.rejects(
      open(dir, { log: (e) => logs.push(e) }).start(quiet),
      (error) => error instanceof StartupError && error.message.includes(name) && /Correct the file, or remove it/.test(error.message),
    );
    assert.ok(logs.some((e) => e.event === "spec_file_unreadable"));
    assert.ok(existsSync(path), "the relay changes nothing on the volume");
    // The owner removes the file: the relay starts, and a first sync makes the spec again.
    rmSync(path);
    const again = open(dir);
    await again.start(quiet);
    const lines = [1, 2].map((line) => ({ line, event: { kind: "status" } }));
    assert.deepEqual(await again.specs.sync(spec, { lines, first: true, state: show("broken"), turn: null }), { line: 2, applied: true });
    await again.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a spec file that the relay cannot read stops the start; the same file starts after the owner fixes the access", async (t) => {
  if (process.getuid?.() === 0) return t.skip("root can read any file");
  const dir = tempDir();
  try {
    const first = open(dir);
    await first.start(quiet);
    const spec = specAddress(REPO, "locked");
    await first.specs.sync(spec, { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: show("locked"), turn: null });
    await first.close();
    const path = join(dir, "specs", specFileName("default", spec.repoKey, "locked"));
    chmodSync(path, 0o000);
    await assert.rejects(open(dir).start(quiet), (error) => error instanceof StartupError && /cannot read the spec file/.test(error.message));
    chmodSync(path, 0o600);
    const again = open(dir);
    await again.start(quiet);
    assert.equal((await again.specs.state(spec))?.line, 1);
    await again.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("review: a bad spec file on a new data folder stops the start before a pairing code is made", async () => {
  const dir = tempDir();
  try {
    mkdirSync(join(dir, "specs"), { recursive: true });
    writeFileSync(join(dir, "specs", "0123456789abcdef0123456789abcdef.json"), "{ not json");
    const printed: string[] = [];
    await assert.rejects(open(dir).start({ log: () => {}, print: (t) => printed.push(t) }), (error) => error instanceof StartupError);
    assert.equal(existsSync(join(dir, "config.json")), false, "no config, so the next start makes and prints a code");
    assert.equal(printed.length, 0);
    rmSync(join(dir, "specs", "0123456789abcdef0123456789abcdef.json"));
    const again = open(dir);
    const { pairCode } = await again.start({ log: () => {}, print: (t) => printed.push(t) });
    assert.ok(pairCode);
    assert.match(printed.join("\n"), /Pairing code: /);
    await again.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
