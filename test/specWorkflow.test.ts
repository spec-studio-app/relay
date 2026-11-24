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

test("a state update on a finished run changes nothing", async () => {
  const h = await harness(env, { graceMs: 60_000 });
  try {
    const spec = specAddress(REPO, "finished-state");
    await h.mirror.sync(spec, { line: 1, event: { kind: "note" }, state: showJson(), turn: null });
    assert.equal(await h.mirror.refresh(spec, { state: showJson({ status: "blocked" }), turn: { turn: "you", needsYou: true } }), true);
    await h.mirror.sync(spec, { line: 2, event: { kind: "stage", to: "merged" }, state: showJson({ stage: "merged" }), turn: null });
    assert.equal(await h.mirror.refresh(spec, { state: showJson({ stage: "build" }), turn: null }), false);
    const state = await h.mirror.state(spec);
    assert.equal((state?.state as Record<string, unknown>).stage, "merged");
    assert.equal(state?.line, 2);
  } finally {
    await h.close();
  }
});
