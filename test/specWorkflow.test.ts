import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { eventually, harness, startTestServer } from "./temporal.js";
import { SpecClosingError, SpecDroppedError, specAddress } from "../src/mirror.js";
import { SA, specWorkflowId } from "../src/workflows/types.js";
import { attachmentPrefix } from "../src/activities.js";
import { sleep } from "./helpers.js";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

const REPO = "/Users/me/code/app";

function showJson(over: Record<string, unknown> = {}) {
  return {
    slug: "dark-mode",
    title: "Dark mode",
    stage: "plan",
    status: "working",
    statusNote: "Planning.",
    mode: "autopilot",
    repos: [{ path: REPO, role: "home", pr: { number: 7, url: "https://github.com/o/r/pull/7" } }],
    ...over,
  };
}

test("R3.S1: the first sync starts the workflow; the state and the search attributes show the values", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress(REPO, "dark-mode");
    assert.match(spec.repoKey, /^[0-9a-f]{12}$/);
    const result = await h.mirror.sync(spec, {
      line: 1,
      event: { t: "2026-11-23T12:00:00Z", kind: "status", from: "idle", to: "working" },
      state: showJson(),
      turn: { turn: "claude", needsYou: false },
    });
    assert.deepEqual(result, { line: 1, applied: true });

    const state = await h.mirror.state(spec);
    assert.ok(state);
    assert.equal(state.line, 1);
    assert.equal((state.state as Record<string, unknown>).stage, "plan");
    assert.deepEqual(state.turn, { turn: "claude", needsYou: false });
    assert.equal(state.spec.repo, REPO);
    assert.equal(state.events.length, 1);

    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "dark-mode"));
    const sa = await eventually(async () => {
      const d = await handle.describe();
      return d.typedSearchAttributes.get(SA.Stage) === "plan" ? d.typedSearchAttributes : null;
    });
    assert.equal(sa.get(SA.RepoKey), spec.repoKey);
    assert.equal(sa.get(SA.SpecSlug), "dark-mode");
    assert.equal(sa.get(SA.SpecTitle), "Dark mode");
    assert.equal(sa.get(SA.Status), "working");
    assert.equal(sa.get(SA.Mode), "autopilot");
    assert.equal(sa.get(SA.NeedsYou), false);
    assert.equal(sa.get(SA.PendingForMac), false);
    assert.equal(sa.get(SA.PendingForCoordinator), false);
    assert.equal(sa.get(SA.RelayDeployId), h.deployId);

    const listed = await eventually(async () => {
      const l = await h.mirror.list();
      return l.length === 1 && l[0].memo?.note === "Planning." ? l : null;
    });
    assert.equal(listed[0].slug, "dark-mode");
    assert.equal(listed[0].memo?.stage, "plan");
  } finally {
    await h.close();
  }
});

test("R3.S2: a line sent twice changes nothing and answers synced for that line", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress(REPO, "twice");
    for (let line = 1; line < 40; line += 13) {
      await h.mirror.sync(spec, { line, event: { kind: "note", text: `n${line}` }, state: showJson(), turn: null });
    }
    const event = { kind: "note", text: "line 40" };
    assert.deepEqual(await h.mirror.sync(spec, { line: 40, event, state: showJson(), turn: null }), { line: 40, applied: true });
    const first = await h.mirror.state(spec);
    const again = await h.mirror.sync(spec, {
      line: 40,
      event,
      state: showJson({ stage: "build" }),
      turn: null,
    });
    assert.deepEqual(again, { line: 40, applied: false });
    const second = await h.mirror.state(spec);
    assert.deepEqual(second, first);
    assert.equal(second?.events.filter((e) => e.line === 40).length, 1);
    // An older line is also ignored.
    assert.deepEqual(await h.mirror.sync(spec, { line: 14, event, state: null, turn: null }), { line: 40, applied: false });
  } finally {
    await h.close();
  }
});

test("R3.S3: a merged line with empty inboxes completes the workflow and removes the attachments", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress(REPO, "shipped");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson(), turn: null });
    const prefix = attachmentPrefix(h.deployId, spec.repoKey, "shipped");
    await h.bucket.put(`${prefix}a-proof.png`, Buffer.from("png"), "image/png", {});
    await h.bucket.put(`${h.deployId}/${spec.repoKey}/other/b.png`, Buffer.from("png"), "image/png", {});

    await h.mirror.sync(spec, {
      line: 2,
      event: { kind: "stage", to: "merged" },
      state: showJson({ stage: "merged", status: "idle" }),
      turn: null,
    });
    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "shipped"));
    await handle.result();
    assert.equal((await handle.describe()).status.name, "COMPLETED");
    assert.equal([...h.bucket.objects.keys()].filter((k) => k.startsWith(prefix)).length, 0);
    assert.equal(h.bucket.objects.size, 1, "the other spec keeps its attachment");
    // A message to the completed spec fails.
    await assert.rejects(h.mirror.send(spec, { text: "late" }), /no synced spec/);
  } finally {
    await h.close();
  }
});

test("a finished spec with a message for the Mac stays open until the Mac takes it", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress(REPO, "dropped-later");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson(), turn: null });
    const { seq } = await h.mirror.send(spec, { text: "Wait." });
    await h.mirror.sync(spec, { line: 2, event: { kind: "merged" }, state: null, turn: null });
    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "dropped-later"));
    assert.equal((await handle.describe()).status.name, "RUNNING");
    await h.mirror.ack(spec, seq);
    await handle.result();
  } finally {
    await h.close();
  }
});

test("R3.S4: with the update limit at 3, the workflow continues as new and keeps state, cursors and messages", async () => {
  const h = await harness(env, { updateLimit: 3 });
  try {
    const spec = specAddress(REPO, "long");
    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "long"));
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson(), turn: null });
    const firstRun = (await handle.describe()).runId;
    await h.mirror.send(spec, { text: "Use the old icon." });
    await h.mirror.sync(spec, {
      line: 2,
      event: { kind: "message", id: "m1", text: "Plan ready.", t: "2026-11-23T12:00:00Z" },
      state: showJson(),
      turn: null,
    });
    // The fourth update goes to the new run.
    await h.mirror.sync(spec, { line: 3, event: { kind: "note" }, state: showJson({ stage: "build" }), turn: null });
    const secondRun = (await handle.describe()).runId;
    assert.notEqual(secondRun, firstRun);

    const state = await h.mirror.state(spec);
    assert.ok(state);
    assert.equal(state.line, 3);
    assert.equal((state.state as Record<string, unknown>).stage, "build");
    assert.equal(state.mac.messages.length, 1);
    assert.equal(state.mac.messages[0].text, "Use the old icon.");
    assert.equal(state.mac.nextSeq, 2);
    assert.equal(state.coordinator.messages[0].text, "Plan ready.");
    assert.deepEqual(state.events.map((e) => e.line), [1, 2, 3]);
    // A copy of an old line is still ignored after the new run starts.
    assert.equal((await h.mirror.sync(spec, { line: 2, event: {}, state: null, turn: null })).applied, false);
  } finally {
    await h.close();
  }
});

test("two deploys on one namespace keep separate workflows for the same repo and slug", async () => {
  const a = await harness(env);
  const b = await harness(env);
  try {
    const spec = specAddress(REPO, "same-slug");
    await a.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson({ title: "From A" }), turn: null });
    await b.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson({ title: "From B" }), turn: null });
    await b.mirror.send(spec, { text: "Only for B." });
    const stateA = await a.mirror.state(spec);
    const stateB = await b.mirror.state(spec);
    assert.equal(stateA?.deployId, a.deployId);
    assert.equal((stateA?.state as Record<string, unknown>).title, "From A");
    assert.equal(stateA?.mac.messages.length, 0);
    assert.equal(stateB?.deployId, b.deployId);
    assert.equal((stateB?.state as Record<string, unknown>).title, "From B");
    assert.equal(stateB?.mac.messages.length, 1);
    assert.equal(specWorkflowId(a.deployId, spec.repoKey, "same-slug"), `spec-${a.deployId}-${spec.repoKey}-same-slug`);
  } finally {
    await a.close();
    await b.close();
  }
});

async function mergedWithAttachment(h: Awaited<ReturnType<typeof harness>>, slug: string) {
  const spec = specAddress(REPO, slug);
  const objectKey = `${attachmentPrefix(h.deployId, spec.repoKey, slug)}a-proof.png`;
  await h.bucket.put(objectKey, Buffer.from("png"), "image/png", {});
  await h.mirror.sync(spec, {
    line: 1,
    event: { kind: "message", id: "m1", text: "Proof.", attachments: [{ objectKey, filename: "proof.png", contentType: "image/png", size: 3, sha256: "x" }] },
    state: showJson(),
    turn: null,
  });
  await h.mirror.sync(spec, { line: 2, event: { kind: "stage", to: "merged" }, state: showJson({ stage: "merged", status: "idle" }), turn: null });
  return { spec, objectKey, handle: h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, slug)) };
}

test("reading the last message of a finished spec keeps its attachments for the grace period after the last update", async () => {
  const h = await harness(env, { graceMs: 1500 });
  try {
    const { spec, objectKey, handle } = await mergedWithAttachment(h, "read-late");
    const read = await h.mirror.read(spec, undefined, 0);
    assert.equal(read.messages[0].attachments?.[0].objectKey, objectKey);
    // The read made the inboxes empty, but the attachment stays for the links in that answer.
    await sleep(1000);
    assert.ok(h.bucket.objects.has(objectKey));
    // A second read starts the grace period again.
    await h.mirror.read(spec, undefined, 0);
    await sleep(1000);
    assert.equal((await handle.describe()).status.name, "RUNNING");
    assert.ok(h.bucket.objects.has(objectKey));
    await handle.result();
    assert.ok(!h.bucket.objects.has(objectKey), "the attachments go when the workflow ends");
  } finally {
    await h.close();
  }
});

test("a message sent in the grace period of a finished spec waits for the Mac", async () => {
  const h = await harness(env, { graceMs: 500 });
  try {
    const { spec, handle } = await mergedWithAttachment(h, "late-message");
    await h.mirror.read(spec, undefined, 0);
    const { seq } = await h.mirror.send(spec, { text: "One more thing." });
    await sleep(1200);
    assert.equal((await handle.describe()).status.name, "RUNNING");
    assert.equal((await h.mirror.state(spec))?.mac.messages[0].text, "One more thing.");
    await h.mirror.ack(spec, seq);
    await handle.result();
  } finally {
    await h.close();
  }
});

test("while a finished spec removes its data, new updates are refused with a clear error and nothing accepted is lost", async () => {
  const h = await harness(env, { graceMs: 200 });
  let release!: () => void;
  let started!: () => void;
  const removing = new Promise<void>((resolve) => (started = resolve));
  const gate = new Promise<void>((resolve) => (release = resolve));
  const remove = h.bucket.removePrefix.bind(h.bucket);
  h.bucket.removePrefix = async (prefix: string) => {
    started();
    await gate;
    return remove(prefix);
  };
  try {
    const { spec, objectKey, handle } = await mergedWithAttachment(h, "closing");
    await h.mirror.read(spec, undefined, 0);
    await removing;
    await assert.rejects(h.mirror.send(spec, { text: "Too late." }), (error: Error) => {
      assert.ok(error instanceof SpecClosingError);
      assert.match(error.message, /finished, and the relay is removing its data/);
      return true;
    });
    // A late line changes nothing more: the relay answers synced for it.
    assert.deepEqual(await h.mirror.sync(spec, { line: 3, event: { kind: "note" }, state: null, turn: null }), { line: 3, applied: false });
    release();
    await handle.result();
    assert.ok(!h.bucket.objects.has(objectKey));
  } finally {
    release();
    await h.close();
  }
});

test("a line after the workflow completed answers synced and starts no new run", async () => {
  const h = await harness(env, { graceMs: 200 });
  try {
    const spec = specAddress(REPO, "after-end");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson(), turn: null });
    await h.mirror.sync(spec, { line: 2, event: { kind: "stage", to: "merged" }, state: showJson({ stage: "merged" }), turn: null });
    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "after-end"));
    await handle.result();
    const runId = (await handle.describe()).runId;
    // A cleanup line after the merge, then the same line again (the agent crashed before synced).
    for (let i = 0; i < 2; i++) {
      assert.deepEqual(
        await h.mirror.sync(spec, { line: 3, event: { kind: "worktree-removed" }, state: showJson({ stage: "merged" }), turn: null }),
        { line: 3, applied: false },
      );
    }
    const now = await handle.describe();
    assert.equal(now.runId, runId);
    assert.equal(now.status.name, "COMPLETED");
  } finally {
    await h.close();
  }
});

test("R3.S3: a gone spec is dropped: its waiting messages go back to Claude AI, sends fail, and the workflow ends", async () => {
  const h = await harness(env, { graceMs: 300 });
  try {
    const spec = specAddress(REPO, "deleted");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson(), turn: null });
    const prefix = attachmentPrefix(h.deployId, spec.repoKey, "deleted");
    await h.bucket.put(`${prefix}a.png`, Buffer.from("png"), "image/png", {});
    const { message } = await h.mirror.send(spec, { text: "Are you there?" });

    await h.mirror.gone(spec);
    const state = await h.mirror.state(spec);
    assert.equal(state?.dropped, true);
    assert.equal(state?.finished, true);
    assert.equal(state?.mac.messages.length, 0);
    const bounced = state?.coordinator.messages.at(-1);
    assert.equal(bounced?.replyTo, message.id);
    assert.match(bounced?.text ?? "", /no longer exists on the Mac/);
    await assert.rejects(h.mirror.send(spec, { text: "Hello?" }), SpecDroppedError);
    // A second gone changes nothing.
    await h.mirror.gone(spec);

    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "deleted"));
    assert.equal((await handle.describe()).status.name, "RUNNING", "the unread error keeps it open");
    await h.mirror.read(spec, undefined, 0);
    await handle.result();
    assert.equal([...h.bucket.objects.keys()].filter((k) => k.startsWith(prefix)).length, 0);
    // gone for a spec with no workflow, or a completed one, is fine.
    await h.mirror.gone(spec);
    await h.mirror.gone(specAddress(REPO, "never-synced"));
  } finally {
    await h.close();
  }
});

test("a new spec with the slug of a completed one starts a new run at line 1; a later line of the old spec stays a no-op", async () => {
  const h = await harness(env, { graceMs: 200 });
  try {
    const spec = specAddress(REPO, "reused-slug");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson({ title: "Old" }), turn: null });
    await h.mirror.sync(spec, { line: 2, event: { kind: "stage", to: "merged" }, state: showJson({ stage: "merged" }), turn: null });
    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "reused-slug"));
    await handle.result();
    const oldRun = (await handle.describe()).runId;

    // A late line of the old spec: synced, no new run.
    assert.deepEqual(await h.mirror.sync(spec, { line: 3, event: { kind: "note" }, state: null, turn: null }), { line: 3, applied: false });
    assert.equal((await handle.describe()).runId, oldRun);

    // The first line of a new spec with the same slug: a new run.
    assert.deepEqual(
      await h.mirror.sync(spec, { line: 1, event: { kind: "created" }, state: showJson({ title: "New" }), turn: null }),
      { line: 1, applied: true },
    );
    const now = await handle.describe();
    assert.notEqual(now.runId, oldRun);
    assert.equal(now.status.name, "RUNNING");
    const state = await h.mirror.state(spec);
    assert.equal((state?.state as Record<string, unknown>).title, "New");
    assert.deepEqual(state?.events.map((e) => e.line), [1]);
    assert.equal(state?.finished, false);
    // Its next line applies, and line 1 sent again is ignored.
    assert.equal((await h.mirror.sync(spec, { line: 2, event: { kind: "note" }, state: showJson({ title: "New" }), turn: null })).applied, true);
    assert.equal((await h.mirror.sync(spec, { line: 1, event: { kind: "created" }, state: null, turn: null })).applied, false);
  } finally {
    await h.close();
  }
});

test("line 1 on a finished run in its grace period starts it again as a new spec", async () => {
  const h = await harness(env, { graceMs: 60_000 });
  try {
    const spec = specAddress(REPO, "back-again");
    for (let line = 1; line <= 5; line++) {
      await h.mirror.sync(spec, { line, event: { kind: "note", text: `old ${line}` }, state: showJson({ title: "Old" }), turn: null });
    }
    const { message } = await h.mirror.send(spec, { text: "For the old spec." });
    await h.mirror.gone(spec);
    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "back-again"));
    const runId = (await handle.describe()).runId;

    // The folder came back: the agent sends from line 1 again, to the same open run.
    assert.deepEqual(
      await h.mirror.sync(spec, { line: 1, event: { kind: "created" }, state: showJson({ title: "New" }), turn: null }),
      { line: 1, applied: true },
    );
    assert.deepEqual(await h.mirror.sync(spec, { line: 2, event: { kind: "note" }, state: showJson({ title: "New" }), turn: null }), {
      line: 2,
      applied: true,
    });
    assert.equal((await handle.describe()).runId, runId);
    const state = await h.mirror.state(spec);
    assert.equal(state?.finished, false);
    assert.equal(state?.dropped, false);
    assert.equal(state?.line, 2);
    assert.deepEqual(state?.events.map((e) => e.line), [1, 2]);
    assert.equal((state?.state as Record<string, unknown>).title, "New");
    assert.ok(state?.coordinator.messages.some((m) => m.replyTo === message.id));
    // The spec takes messages again, and line 1 sent again is ignored now.
    assert.equal((await h.mirror.send(spec, { text: "For the new spec." })).message.text, "For the new spec.");
    assert.equal((await h.mirror.sync(spec, { line: 1, event: { kind: "created" }, state: null, turn: null })).applied, false);
    // The run no longer ends: it is not finished.
    await h.mirror.read(spec, undefined, 0);
    assert.equal((await handle.describe()).status.name, "RUNNING");
  } finally {
    await h.close();
  }
});

test("line 1 while the old run removes its data fails, so the agent sends it again and it starts a new run", async () => {
  const h = await harness(env, { graceMs: 200 });
  let release!: () => void;
  let started!: () => void;
  const removing = new Promise<void>((resolve) => (started = resolve));
  const gate = new Promise<void>((resolve) => (release = resolve));
  const remove = h.bucket.removePrefix.bind(h.bucket);
  h.bucket.removePrefix = async (prefix: string) => {
    started();
    await gate;
    return remove(prefix);
  };
  try {
    const spec = specAddress(REPO, "closing-recreated");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson(), turn: null });
    await h.mirror.gone(spec);
    await removing;
    await assert.rejects(h.mirror.sync(spec, { line: 1, event: { kind: "created" }, state: showJson({ title: "New" }), turn: null }), SpecClosingError);
    release();
    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec.repoKey, "closing-recreated"));
    await handle.result();
    assert.deepEqual(
      await h.mirror.sync(spec, { line: 1, event: { kind: "created" }, state: showJson({ title: "New" }), turn: null }),
      { line: 1, applied: true },
    );
    const state = await h.mirror.state(spec);
    assert.ok(!state?.dropped);
    assert.equal((state?.state as Record<string, unknown>).title, "New");
  } finally {
    release();
    await h.close();
  }
});

test("a state update on a merged run in its grace period applies; on a dropped run it changes nothing", async () => {
  const h = await harness(env, { graceMs: 60_000 });
  try {
    const spec = specAddress(REPO, "finished-state");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson(), turn: null });
    assert.equal(await h.mirror.refresh(spec, { state: showJson({ status: "blocked" }), turn: { turn: "you", needsYou: true } }), true);
    await h.mirror.sync(spec, { line: 2, event: { kind: "stage", to: "merged" }, state: showJson({ stage: "merged" }), turn: null });
    // The agent counts a state frame as delivered: a merged spec takes it.
    assert.equal(await h.mirror.refresh(spec, { state: showJson({ stage: "merged", status: "idle" }), turn: null }), true);
    let state = await h.mirror.state(spec);
    assert.equal((state?.state as Record<string, unknown>).status, "idle");
    assert.equal(state?.finished, true);
    assert.equal(state?.line, 2);

    await h.mirror.gone(spec);
    assert.equal(await h.mirror.refresh(spec, { state: showJson({ stage: "build" }), turn: null }), false);
    state = await h.mirror.state(spec);
    assert.equal((state?.state as Record<string, unknown>).stage, "merged");
  } finally {
    await h.close();
  }
});

test("a restored spec sends its old lines again after the reset: its old messages are not added or unread again", async () => {
  const h = await harness(env, { graceMs: 60_000 });
  try {
    const spec = specAddress(REPO, "restored");
    const lines = [
      { kind: "note", text: "start" },
      { kind: "message", id: "old-1", text: "Plan ready.", t: "2026-11-23T12:00:00Z" },
      { kind: "message", id: "old-2", text: "Build done.", t: "2026-11-23T13:00:00Z" },
    ];
    for (const [i, event] of lines.entries()) {
      await h.mirror.sync(spec, { line: i + 1, event, state: showJson(), turn: null });
    }
    const read = await h.mirror.read(spec, undefined, 0, true);
    assert.equal(read.messages.length, 2);
    await h.mirror.gone(spec);

    // The folder came back: the agent sends the same lines from line 1.
    for (const [i, event] of lines.entries()) {
      assert.equal((await h.mirror.sync(spec, { line: i + 1, event, state: showJson(), turn: null })).applied, true);
    }
    // A new message of the restored spec is added.
    await h.mirror.sync(spec, { line: 4, event: { kind: "message", id: "new-1", text: "Back again." }, state: showJson(), turn: null });

    const state = await h.mirror.state(spec);
    const session = state!.coordinator.messages.filter((m) => m.from === "session" && !m.replyTo);
    assert.deepEqual(session.map((m) => m.id), ["old-1", "old-2", "new-1"]);
    const unread = await h.mirror.read(spec, undefined, 0, true);
    assert.deepEqual(unread.messages.map((m) => m.id), ["new-1"]);
  } finally {
    await h.close();
  }
});

test("a state update moves the mirror's syncedAt, and keeps the line cursor and the events", async () => {
  let clock = new Date("2026-11-23T12:00:00.000Z");
  const h = await harness(env);
  try {
    const { Mirror } = await import("../src/mirror.js");
    const mirror = new Mirror({ client: h.clients.data, deployId: h.deployId, taskQueue: h.taskQueue, now: () => clock });
    const spec = specAddress(REPO, "fresh-state");
    await mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson(), turn: null });
    assert.equal((await mirror.state(spec))?.syncedAt, "2026-11-23T12:00:00.000Z");
    clock = new Date("2026-11-23T15:30:00.000Z");
    assert.equal(await mirror.refresh(spec, { state: showJson({ status: "blocked" }), turn: null }), true);
    const state = await mirror.state(spec);
    assert.equal(state?.syncedAt, "2026-11-23T15:30:00.000Z");
    assert.equal(state?.line, 1);
    assert.equal(state?.events.length, 1);
  } finally {
    await h.close();
  }
});

/** The lines `from` to `to` as a sync batch of notes. */
function notes(from: number, to: number) {
  return Array.from({ length: to - from + 1 }, (_, i) => ({ line: from + i, event: { kind: "note", text: `n${from + i}` } }));
}

/** The number of updates in a workflow's history. */
async function updateCount(h: Awaited<ReturnType<typeof harness>>, slug: string, repoKey: string): Promise<number> {
  const history = await h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, repoKey, slug)).fetchHistory();
  return (history.events ?? []).filter((e) => e.workflowExecutionUpdateAcceptedEventAttributes).length;
}

test("R1.S1: a batch of seven lines is one update; R1.S3: lines the workflow has are skipped", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress(REPO, "batch");
    assert.deepEqual(await h.mirror.sync(spec, { lines: notes(1, 40), state: showJson(), turn: null }), { line: 40, applied: true });
    const before = await updateCount(h, "batch", spec.repoKey);
    assert.deepEqual(await h.mirror.sync(spec, { lines: notes(41, 47) }), { line: 47, applied: true });
    assert.equal(await updateCount(h, "batch", spec.repoKey), before + 1);
    let state = await h.mirror.state(spec);
    assert.equal(state?.line, 47);
    assert.equal(state?.events.length, 20);
    assert.equal(state?.events.at(-1)?.event.text, "n47");
    // A frame with no state keeps the mirror's state.
    assert.equal((state?.state as Record<string, unknown>).title, "Dark mode");

    // The workflow has 47; a batch of 41 to 50 applies 48 to 50 only.
    assert.deepEqual(await h.mirror.sync(spec, { lines: notes(41, 50) }), { line: 50, applied: true });
    state = await h.mirror.state(spec);
    assert.deepEqual(state?.events.slice(-4).map((e) => e.line), [47, 48, 49, 50]);
    // The same batch again changes nothing.
    assert.deepEqual(await h.mirror.sync(spec, { lines: notes(41, 50) }), { line: 50, applied: false });
  } finally {
    await h.close();
  }
});

test("R3.S1, R3.S4: a batch without a state keeps the state and the turn; a batch with them replaces both", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress(REPO, "state-once");
    await h.mirror.sync(spec, { lines: notes(1, 2), state: showJson(), turn: { turn: "claude", needsYou: false } });
    await h.mirror.sync(spec, { lines: notes(3, 4) });
    let state = await h.mirror.state(spec);
    assert.deepEqual(state?.turn, { turn: "claude", needsYou: false });
    assert.equal((state?.state as Record<string, unknown>).stage, "plan");
    await h.mirror.sync(spec, { lines: notes(5, 5), state: showJson({ stage: "build" }), turn: null });
    state = await h.mirror.state(spec);
    assert.equal(state?.turn, null);
    assert.equal((state?.state as Record<string, unknown>).stage, "build");
  } finally {
    await h.close();
  }
});

test("R2.S1, R2.S3: a short first sync is one update, and its earlier messages reach the coordinator inbox", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress(REPO, "long-spec");
    const message = (line: number, id: string) => ({ line, event: { kind: "message", id, stage: "plan", text: `m${line}` } });
    const lines = [message(12, "a"), message(150, "b"), ...notes(216, 235)];
    assert.deepEqual(await h.mirror.sync(spec, { first: true, lines, state: showJson(), turn: null }), { line: 235, applied: true });
    assert.equal(await updateCount(h, "long-spec", spec.repoKey), 1);
    const state = await h.mirror.state(spec);
    assert.equal(state?.line, 235);
    assert.equal(state?.events.length, 20);
    assert.deepEqual(state?.coordinator.messages.map((m) => m.text), ["m12", "m150"]);
  } finally {
    await h.close();
  }
});

test("R2.S5: a first sync to a running workflow does not reset it", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress(REPO, "lost-cursor");
    await h.mirror.sync(spec, { lines: notes(1, 235), state: showJson(), turn: null });
    assert.deepEqual(await h.mirror.sync(spec, { first: true, lines: notes(216, 235), state: showJson(), turn: null }), {
      line: 235,
      applied: false,
    });
    const state = await h.mirror.state(spec);
    assert.equal(state?.line, 235);
    assert.equal(state?.events.at(-1)?.line, 235);
  } finally {
    await h.close();
  }
});

test("R2.S4: a first sync that starts above line 1 starts a finished spec again, in its grace period and after it completed", async () => {
  const h = await harness(env, { graceMs: 300 });
  try {
    // In the grace period: the run resets.
    const spec = specAddress(REPO, "made-again");
    await h.mirror.sync(spec, { lines: notes(1, 3), state: showJson({ title: "Old" }), turn: null });
    await h.mirror.gone(spec);
    assert.equal((await h.mirror.state(spec))?.finished, true);
    assert.deepEqual(await h.mirror.sync(spec, { first: true, lines: notes(31, 50), state: showJson({ title: "New" }), turn: null }), {
      line: 50,
      applied: true,
    });
    let state = await h.mirror.state(spec);
    assert.equal(state?.finished, false);
    assert.equal(state?.dropped, false);
    assert.deepEqual(state?.events.map((e) => e.line), notes(31, 50).map((l) => l.line));
    assert.equal((state?.state as Record<string, unknown>).title, "New");

    // After the run completed: a new run.
    const done = specAddress(REPO, "made-again-later");
    await h.mirror.sync(done, { lines: notes(1, 2), state: showJson(), turn: null });
    await h.mirror.sync(done, { lines: [{ line: 3, event: { kind: "stage", to: "merged" } }], state: showJson({ stage: "merged" }), turn: null });
    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, done.repoKey, "made-again-later"));
    await handle.result();
    const oldRun = (await handle.describe()).runId;
    // A later batch with no first mark stays a no-op.
    assert.deepEqual(await h.mirror.sync(done, { lines: notes(4, 5) }), { line: 5, applied: false });
    assert.equal((await handle.describe()).runId, oldRun);
    assert.deepEqual(await h.mirror.sync(done, { first: true, lines: notes(31, 50), state: showJson({ title: "New" }), turn: null }), {
      line: 50,
      applied: true,
    });
    assert.notEqual((await handle.describe()).runId, oldRun);
    state = await h.mirror.state(done);
    assert.equal(state?.line, 50);
  } finally {
    await h.close();
  }
});

test("a batch with no new line still applies its state", async () => {
  const h = await harness(env);
  try {
    const spec = specAddress(REPO, "old-lines");
    await h.mirror.sync(spec, { lines: notes(1, 47), state: showJson(), turn: null });
    // The synced answer was lost; the stage changed; the agent sends 41 to 47 again with the new state.
    assert.deepEqual(await h.mirror.sync(spec, { lines: notes(41, 47), state: showJson({ stage: "build" }), turn: { turn: "you", needsYou: true } }), {
      line: 47,
      applied: false,
    });
    const state = await h.mirror.state(spec);
    assert.equal((state?.state as Record<string, unknown>).stage, "build");
    assert.deepEqual(state?.turn, { turn: "you", needsYou: true });
    assert.equal(state?.line, 47);
  } finally {
    await h.close();
  }
});

test("a first sync of the same merged spec sent again does not reset it; a dropped spec that comes back without a state keeps its state", async () => {
  const h = await harness(env, { graceMs: 60_000 });
  try {
    const spec = specAddress(REPO, "same-merged");
    const merged = showJson({ stage: "merged", createdAt: "2026-11-01T00:00:00Z" });
    const lines = [...notes(1, 9), { line: 10, event: { kind: "stage", to: "merged" } }];
    await h.mirror.sync(spec, { first: true, lines, state: merged, turn: null });
    assert.equal((await h.mirror.state(spec))?.finished, true);
    // The same first sync again (a lost answer): no reset.
    assert.deepEqual(await h.mirror.sync(spec, { first: true, lines, state: merged, turn: null }), { line: 10, applied: false });
    let state = await h.mirror.state(spec);
    assert.equal(state?.finished, true);
    assert.equal(state?.events.length, 10);
    assert.equal((state?.state as Record<string, unknown>).stage, "merged");

    // A first sync with a null state (studio show failed) is no proof of a new spec: no reset.
    assert.deepEqual(await h.mirror.sync(spec, { first: true, lines: notes(1, 3), state: null, turn: null }), { line: 10, applied: false });
    assert.equal((await h.mirror.state(spec))?.events.length, 10);

    // A new spec with the same slug has another createdAt: the run resets.
    const fresh = showJson({ stage: "spec", title: "Other", createdAt: "2026-11-20T00:00:00Z" });
    assert.deepEqual(await h.mirror.sync(spec, { first: true, lines: notes(1, 3), state: fresh, turn: null }), { line: 3, applied: true });
    state = await h.mirror.state(spec);
    assert.equal(state?.finished, false);
    assert.equal((state?.state as Record<string, unknown>).title, "Other");

    // Dropped, then the folder comes back with a first sync and no state: the run resets and keeps its state.
    await h.mirror.gone(spec);
    assert.deepEqual(await h.mirror.sync(spec, { first: true, lines: notes(1, 3) }), { line: 3, applied: true });
    state = await h.mirror.state(spec);
    assert.equal(state?.dropped, false);
    assert.equal((state?.state as Record<string, unknown>).title, "Other");
  } finally {
    await h.close();
  }
});

test("a first sync with no state on a merged then dropped run starts a new spec that is not finished", async () => {
  const h = await harness(env, { graceMs: 60_000 });
  try {
    const spec = specAddress(REPO, "merged-dropped");
    await h.mirror.sync(spec, { first: true, lines: [{ line: 1, event: { kind: "stage", to: "merged" } }], state: showJson({ stage: "merged" }), turn: null });
    await h.mirror.gone(spec);
    assert.deepEqual(await h.mirror.sync(spec, { first: true, lines: notes(1, 2), state: null, turn: null }), { line: 2, applied: true });
    const state = await h.mirror.state(spec);
    assert.equal(state?.finished, false);
    assert.equal(state?.state, null);
  } finally {
    await h.close();
  }
});
