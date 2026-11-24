import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { eventually, harness, startTestServer } from "./temporal.js";
import { specAddress } from "../src/mirror.js";
import { SA, specWorkflowId } from "../src/workflows/types.js";
import { attachmentPrefix } from "../src/activities.js";

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
    await h.mirror.sync(spec, { line: 2, event: { kind: "dropped" }, state: null, turn: null });
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
