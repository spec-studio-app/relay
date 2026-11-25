// The desk store (relay-desk-messages R1, R2, R4, R5, R6): the same scenarios
// run on the JSON store and on the Temporal store, with fixed expected values,
// so both stores must give the same results.
import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { chmodSync, existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import type { DeskStore, SpecStore } from "../src/store/types.js";
import type { DeskReport } from "../src/workflows/types.js";
import { DESK_GONE_TEXT, DESK_OFF_TEXT } from "../src/workflows/types.js";
import { DeskNotFoundError, DeskRefusedError, DeskUnreadableError } from "../src/store/errors.js";
import { deskFileName } from "../src/store/json/jsonDeskStore.js";
import { MessageTools } from "../src/messages.js";
import { Attachments } from "../src/attachments.js";
import { repoKeyOf, specAddress } from "../src/store/address.js";
import { openJsonStore } from "../src/store/json/index.js";
import { MemoryBucket } from "../src/bucket.js";
import { harness, startTestServer } from "./temporal.js";

interface DesksUnderTest {
  desks: DeskStore;
  specs: SpecStore;
  close(): Promise<void>;
}

async function eventually<T>(check: () => Promise<T | null | undefined | false>, ms = 10_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > end) throw new Error("The condition did not pass in time.");
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

function repo(): string {
  return `/Users/me/code/desk-${randomBytes(4).toString("hex")}`;
}

function report(path: string, over: Partial<DeskReport> = {}): DeskReport {
  return {
    repoKey: repoKeyOf(path),
    repo: path,
    open: true,
    turn: { turn: "idle", needsYou: false },
    canMessage: true,
    reason: null,
    ...over,
  };
}

function reply(line: number, id: string, text: string, over: Record<string, unknown> = {}) {
  return { line, event: { t: "2026-11-24T10:00:00Z", kind: "message", id, text, ...over } };
}

function runDeskConformance(label: string, open: () => Promise<DesksUnderTest>): void {
  const scenario = (name: string, fn: (s: DesksUnderTest) => Promise<void>) =>
    test(`${label}: ${name}`, async () => {
      const store = await open();
      try {
        await fn(store);
      } finally {
        await store.close();
      }
    });

  scenario("R5: a desks entry makes the desk, and the store keeps its state with the time", async ({ desks }) => {
    const path = repo();
    const key = repoKeyOf(path);
    assert.equal(await desks.get({ repoKey: key }), null);
    await desks.report(report(path));
    const state = await desks.get({ repoKey: key });
    assert.equal(state?.desk.repoKey, key);
    assert.equal(state?.desk.repo, path);
    assert.equal(state?.open, true);
    assert.deepEqual(state?.turn, { turn: "idle", needsYou: false });
    assert.equal(state?.canMessage, true);
    assert.equal(state?.reason, null);
    assert.ok(state?.stateAt && !Number.isNaN(Date.parse(state.stateAt)));
    // A new entry replaces the state.
    await desks.report(report(path, { open: false, turn: null, canMessage: false, reason: "Sync is off for this repo on the Mac." }));
    const closed = await desks.get({ repoKey: key });
    assert.equal(closed?.open, false);
    assert.equal(closed?.turn, null);
    assert.equal(closed?.canMessage, false);
    assert.equal(closed?.reason, "Sync is off for this repo on the Mac.");
    const listed = await eventually(async () => {
      const all = await desks.list({ repoKey: key });
      return all.length === 1 && all[0].memo.open === false ? all : null;
    });
    assert.deepEqual(
      { ...listed[0].memo, stateAt: undefined },
      {
        repo: path,
        open: false,
        turn: null,
        canMessage: false,
        reason: "Sync is off for this repo on the Mac.",
        stateAt: undefined,
        waitingForMac: 0,
        unreadForCoordinator: 0,
      },
    );
  });

  scenario("R1.S1: a message to a desk that Claude AI can message waits in its Mac inbox", async ({ desks }) => {
    const path = repo();
    const key = repoKeyOf(path);
    await desks.report(report(path));
    const { seq, message } = await desks.send({ repoKey: key }, { text: "Is CI green on main?" });
    assert.equal(seq, 1);
    assert.equal(message.from, "coordinator");
    assert.deepEqual(message.desk, { repoKey: key, repo: path });
    assert.equal(message.text, "Is CI green on main?");
    assert.equal("spec" in message, false);
    assert.equal("stage" in message, false);
    const state = await desks.get({ repoKey: key });
    assert.deepEqual(state?.mac.messages.map((m) => m.id), [message.id]);
    const pending = await eventually(async () => {
      const all = await desks.list({ pendingForMac: true, repoKey: key });
      return all.length === 1 ? all : null;
    });
    assert.equal(pending[0].memo.waitingForMac, 1);
    // The Mac takes it.
    assert.deepEqual(await desks.ack({ repoKey: key }, seq), { removed: true });
    assert.deepEqual(await desks.ack({ repoKey: key }, seq), { removed: false });
    assert.equal((await desks.get({ repoKey: key }))?.mac.messages.length, 0);
  });

  scenario("R1.S3: a message to a desk that the relay does not have fails", async ({ desks }) => {
    await assert.rejects(desks.send({ repoKey: repoKeyOf(repo()) }, { text: "Hello" }), (error) => error instanceof DeskNotFoundError);
    await assert.rejects(desks.read({ repoKey: repoKeyOf(repo()) }, undefined, 0), (error) => error instanceof DeskNotFoundError);
  });

  scenario("R2.S2, R2.S5: a desk that Claude AI cannot message refuses with its reason", async ({ desks }) => {
    const path = repo();
    const key = repoKeyOf(path);
    await desks.report(report(path, { canMessage: false, reason: null }));
    await assert.rejects(
      desks.send({ repoKey: key }, { text: "Hello" }),
      (error) => error instanceof DeskRefusedError && error.message === DESK_OFF_TEXT,
    );
    await desks.report(report(path, { canMessage: false, reason: "Sync is off for this repo on the Mac." }));
    await assert.rejects(
      desks.send({ repoKey: key }, { text: "Hello" }),
      (error) => error instanceof DeskRefusedError && error.message === "Sync is off for this repo on the Mac.",
    );
    assert.equal((await desks.get({ repoKey: key }))?.mac.messages.length, 0);
    // R2.S3: the switch turns on.
    await desks.report(report(path));
    assert.equal((await desks.send({ repoKey: key }, { text: "Hello" })).seq, 1);
  });

  scenario("R2.S4: an ack with an error bounces the message to the coordinator inbox", async ({ desks }) => {
    const path = repo();
    const key = repoKeyOf(path);
    await desks.report(report(path));
    const { seq, message } = await desks.send({ repoKey: key }, { text: "Run the tests." });
    await desks.ack({ repoKey: key }, seq, DESK_OFF_TEXT);
    assert.equal((await desks.get({ repoKey: key }))?.mac.messages.length, 0);
    const read = await desks.read({ repoKey: key }, undefined, 0);
    assert.equal(read.messages.length, 1);
    const note = read.messages[0];
    assert.equal(note.from, "session");
    assert.equal(note.replyTo, message.id);
    assert.equal(note.text, `The Mac did not take the message: ${DESK_OFF_TEXT}`);
    assert.deepEqual(note.desk, { repoKey: key, repo: path });
  });

  scenario("R4.S1, R6.S2: desk-sync adds the replies once; a line sent again is skipped by its id", async ({ desks }) => {
    const path = repo();
    const key = repoKeyOf(path);
    await desks.report(report(path));
    const files = [{ objectKey: "k/_desk/x-build.log", filename: "build.log", contentType: "text/plain", size: 3, sha256: "abc" }];
    assert.deepEqual(
      await desks.sync({ repoKey: key, repo: path }, [reply(1, "r1", "CI is green on main.", { replyTo: "abc" }), reply(2, "r2", "The log.", { attachments: files })]),
      { line: 2, applied: true },
    );
    // The same lines again (a lost desk-synced): nothing new.
    assert.deepEqual(await desks.sync({ repoKey: key, repo: path }, [reply(1, "r1", "CI is green on main.", { replyTo: "abc" }), reply(2, "r2", "The log.")]), {
      line: 2,
      applied: false,
    });
    // A new outbox (the Mac's cursor started again) with a new reply: the id is new, so it is added.
    assert.deepEqual(await desks.sync({ repoKey: key, repo: path }, [reply(1, "r3", "Done.")]), { line: 1, applied: true });
    const read = await desks.read({ repoKey: key }, undefined, 0);
    assert.deepEqual(
      read.messages.map((m) => [m.seq, m.id, m.from, m.text, m.replyTo ?? null, m.attachments?.[0]?.filename ?? null]),
      [
        [1, "r1", "session", "CI is green on main.", "abc", null],
        [2, "r2", "session", "The log.", null, "build.log"],
        [3, "r3", "session", "Done.", null, null],
      ],
    );
    assert.equal(read.messages[0].createdAt, "2026-11-24T10:00:00Z");
    assert.deepEqual(read.messages[0].desk, { repoKey: key, repo: path });
  });

  scenario("R4.S1: a desk-sync for a desk that the store does not have makes it", async ({ desks }) => {
    const path = repo();
    const key = repoKeyOf(path);
    await desks.sync({ repoKey: key, repo: path }, [reply(1, "r1", "Hello")]);
    const state = await desks.get({ repoKey: key });
    assert.equal(state?.canMessage, false);
    assert.equal(state?.stateAt, null);
    assert.equal(state?.coordinator.messages.length, 1);
  });

  scenario("R4.S3: reads keep a cursor, and the unread read moves the read mark", async ({ desks }) => {
    const path = repo();
    const key = repoKeyOf(path);
    await desks.report(report(path));
    await desks.sync({ repoKey: key }, [reply(1, "a", "One"), reply(2, "b", "Two")]);
    const unread = await eventually(async () => {
      const all = await desks.list({ pendingForCoordinator: true, repoKey: key });
      return all.length === 1 ? all : null;
    });
    assert.equal(unread[0].memo.unreadForCoordinator, 2);
    assert.deepEqual((await desks.read({ repoKey: key }, 1, 0)).messages.map((m) => m.text), ["Two"]);
    assert.deepEqual((await desks.read({ repoKey: key }, undefined, 0, true)).messages.map((m) => m.text), []);
    await desks.sync({ repoKey: key }, [reply(3, "c", "Three")]);
    assert.deepEqual((await desks.read({ repoKey: key }, undefined, 0, true)).messages.map((m) => m.text), ["Three"]);
    assert.equal((await desks.get({ repoKey: key }))?.coordinator.readUpTo, 3);
    // A read waits for a new reply.
    const waiting = desks.read({ repoKey: key }, 3, 5000);
    await new Promise((resolve) => setTimeout(resolve, 300));
    await desks.sync({ repoKey: key }, [reply(4, "d", "Four")]);
    assert.deepEqual((await waiting).messages.map((m) => m.text), ["Four"]);
  });

  scenario("R5.S4: the spec list never shows a desk", async ({ desks, specs }) => {
    const path = repo();
    const key = repoKeyOf(path);
    await desks.report(report(path));
    await desks.send({ repoKey: key }, { text: "For the Mac" });
    await desks.sync({ repoKey: key }, [reply(1, "x", "For Claude AI")]);
    // A spec in the same repo, so the list has something to show.
    await specs.sync(specAddress(path, "themes"), { lines: [{ line: 1, event: { kind: "status" } }], first: true, state: { slug: "themes", title: "Themes", stage: "build", status: "working", mode: "autopilot" }, turn: null });
    await eventually(async () => (await desks.list({ pendingForMac: true, repoKey: key })).length === 1);
    await eventually(async () => (await specs.list({ repoKey: key })).length === 1);
    for (const filter of [{}, { all: true }, { repoKey: key }, { pendingForMac: true }, { pendingForCoordinator: true }]) {
      const listed = await specs.list(filter);
      assert.deepEqual(
        listed.filter((s) => s.repoKey === key).map((s) => s.slug),
        "pendingForMac" in filter || "pendingForCoordinator" in filter ? [] : ["themes"],
        JSON.stringify(filter),
      );
    }
  });
}

// ---- the JSON store ----

function jsonStore(dir: string) {
  return openJsonStore({ dataDir: dir, deployId: "default", publicUrl: "https://relay.example.com" }, { bucket: new MemoryBucket(), log: () => {}, graceMs: 60_000 });
}

runDeskConformance("JSON desk store", async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-desk-"));
  const store = jsonStore(dir);
  await store.specs.load();
  await store.desks.load();
  return {
    desks: store.desks,
    specs: store.specs,
    async close() {
      await store.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
});

test("JSON desk store: a desk stays after a restart, in its own file under desks/", async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-desk-"));
  try {
    const first = jsonStore(dir);
    await first.start({ log: () => {}, print: () => {} });
    const path = repo();
    const key = repoKeyOf(path);
    await first.desks.report(report(path));
    await first.desks.send({ repoKey: key }, { text: "Waits" });
    await first.close();
    assert.equal(readdirSync(join(dir, "desks")).filter((n) => n.endsWith(".json")).length, 1);
    assert.equal(existsSync(join(dir, "specs")), true);
    assert.deepEqual(readdirSync(join(dir, "specs")), []);
    const second = jsonStore(dir);
    await second.start({ log: () => {}, print: () => {} });
    const state = await second.desks.get({ repoKey: key });
    assert.equal(state?.mac.messages[0]?.text, "Waits");
    assert.equal(state?.canMessage, true);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("review: a desk file that the relay cannot read stays in place, and the next start reads it again", async (t) => {
  if (process.getuid?.() === 0) return t.skip("root can read any file");
  const dir = mkdtempSync(join(tmpdir(), "relay-desk-"));
  try {
    const first = jsonStore(dir);
    await first.start({ log: () => {}, print: () => {} });
    const path = repo();
    const key = repoKeyOf(path);
    await first.desks.report(report(path));
    await first.desks.send({ repoKey: key }, { text: "Waits" });
    await first.close();
    const file = join(dir, "desks", deskFileName("default", key));
    chmodSync(file, 0o000);
    const logs: Array<{ event: string }> = [];
    const second = openJsonStore(
      { dataDir: dir, deployId: "default", publicUrl: "https://relay.example.com" },
      { bucket: new MemoryBucket(), log: (e) => logs.push(e as { event: string }), graceMs: 60_000 },
    );
    await second.start({ log: () => {}, print: () => {} });
    assert.ok(logs.some((e) => e.event === "desk_file_read_failed"));
    // The next desks frame and a desk-sync do not write over the file.
    await assert.rejects(second.desks.report(report(path)), DeskUnreadableError);
    await assert.rejects(second.desks.sync({ repoKey: key, repo: path }, [reply(1, "r1", "Done.")]), DeskUnreadableError);
    await second.close();
    chmodSync(file, 0o600);
    const third = jsonStore(dir);
    await third.start({ log: () => {}, print: () => {} });
    assert.equal((await third.desks.get({ repoKey: key }))?.mac.messages[0]?.text, "Waits");
    await third.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("review: a repo name finds only a desk of the Mac's last desks frame, and two such repos need the path", async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-desk-"));
  try {
    const store = jsonStore(dir);
    await store.start({ log: () => {}, print: () => {} });
    const suffix = randomBytes(4).toString("hex");
    const old = `/Users/me/old/app-${suffix}`;
    const moved = `/Users/me/new/app-${suffix}`;
    await store.desks.report(report(old));
    await store.desks.report(report(moved));
    const tools = new MessageTools({
      specs: store.specs,
      desks: store.desks,
      push: () => {},
      pushDesk: () => false,
      attachments: new Attachments({ bucket: null, deployId: "default", maxBytes: 1000 }),
    });
    const both = await tools.answer("send_message", { repo: `app-${suffix}`, text: "Again?" });
    assert.equal(both.ok, false);
    assert.match(both.text, /More than one repo has the name/);
    // The next desks frame does not list the old path: the relay marks that desk gone.
    await store.desks.report(report(old, { open: false, turn: null, canMessage: false, reason: DESK_GONE_TEXT }));
    const sent = await tools.answer("send_message", { repo: `app-${suffix}`, text: "Is CI green?" });
    assert.equal(sent.ok, true, sent.text);
    assert.equal((await store.desks.get({ repoKey: repoKeyOf(moved) }))?.mac.messages.length, 1);
    assert.equal((await store.desks.get({ repoKey: repoKeyOf(old) }))?.mac.messages.length, 0);
    await store.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("review: with a connected Mac whose app does not take desk messages, a desk message says to update Spec Studio", async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-desk-"));
  try {
    const store = jsonStore(dir);
    await store.start({ log: () => {}, print: () => {} });
    let takes: boolean | null = false;
    const tools = new MessageTools({
      specs: store.specs,
      desks: store.desks,
      push: () => {},
      pushDesk: () => false,
      macTakesDesks: () => takes,
      helloRepos: async () => ["/Users/me/code/old-app"],
      attachments: new Attachments({ bucket: null, deployId: "default", maxBytes: 1000 }),
    });
    const old = await tools.answer("send_message", { repo: "/Users/me/code/old-app", text: "Hi" });
    assert.equal(old.ok, false);
    assert.match(old.text, /does not take desk messages/);
    // A repo that the Mac does not have keeps the no-desk text (R1.S3).
    const unknown = await tools.answer("send_message", { repo: "/Users/me/code/none", text: "Hi" });
    assert.match(unknown.text, /has no desk for the repo/);
    takes = null;
    const offline = await tools.answer("send_message", { repo: "/Users/me/code/none", text: "Hi" });
    assert.match(offline.text, /has no desk for the repo/);
    await store.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---- the Temporal store ----

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

runDeskConformance("Temporal desk store", async () => {
  const h = await harness(env, { graceMs: 60_000 });
  return { desks: h.desks, specs: h.mirror, close: () => h.close() };
});

test("Temporal desk store: a desk continues as new after its update limit and keeps its inboxes", async () => {
  const h = await harness(env, { updateLimit: 3 });
  try {
    const path = repo();
    const key = repoKeyOf(path);
    await h.desks.report(report(path));
    const handle = h.clients.data.workflow.getHandle(`desk-${h.deployId}-${key}`);
    const firstRun = (await handle.describe()).runId;
    for (let i = 1; i <= 5; i++) await h.desks.send({ repoKey: key }, { text: `Message ${i}` });
    await h.desks.sync({ repoKey: key }, [reply(1, "r1", "Reply")]);
    const state = await eventually(() => h.desks.get({ repoKey: key }));
    assert.deepEqual(state.mac.messages.map((m) => m.seq), [1, 2, 3, 4, 5]);
    assert.equal(state.coordinator.messages.length, 1);
    assert.equal(state.canMessage, true);
    const description = await handle.describe();
    assert.notEqual(description.runId, firstRun);
    assert.equal(description.type, "deskWorkflow");
  } finally {
    await h.close();
  }
});
