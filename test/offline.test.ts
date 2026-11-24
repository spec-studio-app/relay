import { after, before, beforeEach, test } from "node:test";
import { randomBytes } from "node:crypto";
import assert from "node:assert/strict";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { eventually, harness, startTestServer } from "./temporal.js";
import { AGENT_SECRET, callTool, connectAgent, signIn } from "./helpers.js";
import { specAddress } from "../src/mirror.js";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

// Spec workflow IDs do not hold the deploy ID, so each test uses its own repos.
let REPO = "";
let OTHER = "";
beforeEach(() => {
  const id = randomBytes(4).toString("hex");
  REPO = `/Users/me/code/app-${id}`;
  OTHER = `/Users/me/code/site-${id}`;
});

function show(slug: string, over: Record<string, unknown> = {}) {
  return { slug, title: `Title of ${slug}`, stage: "build", status: "working", statusNote: `Note of ${slug}`, mode: "guided", ...over };
}

async function seed(h: Awaited<ReturnType<typeof harness>>) {
  await h.mirror.sync(specAddress(REPO, "alpha"), {
    line: 3,
    event: { kind: "note", text: "a" },
    state: show("alpha"),
    turn: { turn: "you", needsYou: true },
  });
  await h.mirror.sync(specAddress(OTHER, "beta"), {
    line: 1,
    event: { kind: "note", text: "b" },
    state: show("beta", { stage: "plan", status: "awaiting-approval" }),
    turn: { turn: "claude", needsYou: false },
  });
  await h.mirror.sync(specAddress(REPO, "gone"), { line: 1, event: { kind: "note" }, state: show("gone"), turn: null });
  await h.mirror.sync(specAddress(REPO, "gone"), {
    line: 2,
    event: { kind: "stage", to: "merged" },
    state: show("gone", { stage: "merged", status: "idle" }),
    turn: null,
  });
}

test("R5.S1: with no agent, list_specs lists the synced specs with their sync times", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await seed(h);
    // Another deploy on the same namespace: its specs do not show.
    const other = await harness(env);
    await other.mirror.sync(specAddress(REPO, "foreign"), { line: 1, event: {}, state: show("foreign"), turn: null });
    await other.close();

    const { accessToken } = await signIn(t.base);
    const result = await eventually(async () => {
      const r = await callTool(t.base, accessToken, "list_specs", {});
      return r.content[0].text.includes('"beta"') && r.content[0].text.includes('"alpha"') ? r : null;
    });
    assert.equal(result.isError, undefined);
    const [head, ...body] = result.content[0].text.split("\n");
    assert.match(head, /^The Mac is offline\. .* Each spec shows the time of its last sync/);
    const list = JSON.parse(body.join("\n"));
    const bySlug = Object.fromEntries(list.specs.map((s: any) => [s.slug, s]));
    assert.deepEqual(Object.keys(bySlug).sort(), ["alpha", "beta"]);
    assert.equal(bySlug.alpha.stage, "build");
    assert.equal(bySlug.alpha.status, "working");
    assert.equal(bySlug.alpha.note, "Note of alpha");
    assert.equal(bySlug.alpha.turn, "you");
    assert.equal(bySlug.alpha.needsYou, true);
    assert.equal(bySlug.alpha.repo, REPO);
    assert.ok(Date.parse(bySlug.alpha.syncedAt) > 0);
    assert.equal(bySlug.beta.needsYou, false);
    assert.ok(list.asOf);

    const scoped = await callTool(t.base, accessToken, "list_specs", { repo: OTHER });
    const scopedList = JSON.parse(scoped.content[0].text.split("\n").slice(1).join("\n"));
    assert.deepEqual(scopedList.specs.map((s: any) => s.slug), ["beta"]);

    const all = await eventually(async () => {
      const r = await callTool(t.base, accessToken, "list_specs", { all: true });
      return r.content[0].text.includes('"gone"') ? r : null;
    });
    assert.ok(all);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R5.S2: read_spec with no agent gives the state, the events, the sync time and the note about the text", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    const spec = specAddress(REPO, "many");
    for (let line = 1; line <= 25; line++) {
      await h.mirror.sync(spec, { line, event: { kind: "note", text: `event ${line}` }, state: show("many"), turn: null });
    }
    await h.mirror.send(spec, { text: "Use the old icon." });
    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "read_spec", { repo: REPO, slug: "many" });
    assert.equal(result.isError, undefined);
    const text = result.content[0].text;
    assert.match(text, /This copy of the spec is from its last sync at \d{4}-\d\d-\d\dT/);
    assert.match(text, /spec\.md and plan\.md text are not available while the Mac is offline/);
    assert.match(text, /"title": "Title of many"/);
    assert.match(text, /1 message waits for the Mac/);
    assert.ok(text.includes('{"kind":"note","text":"event 25"}'));
    assert.ok(text.includes('{"kind":"note","text":"event 6"}'));
    assert.ok(!text.includes('"event 5"'), "only the last 20 events");

    const missing = await callTool(t.base, accessToken, "read_spec", { repo: REPO, slug: "nope" });
    assert.equal(missing.isError, true);
    assert.match(missing.content[0].text, /no copy of the spec nope/);
  } finally {
    await t.close();
    await h.close();
  }
});

test("list_repos offline gives the repos of the last hello with its time", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 2, repos: [REPO, OTHER] });
    await agent.next("welcome");
    await eventually(() => h.configStore.repos());
    await agent.close();
    const { accessToken } = await signIn(t.base);
    const result = await eventually(async () => {
      const r = await callTool(t.base, accessToken, "list_repos");
      return r.isError ? null : r;
    });
    const [head, json] = result.content[0].text.split("\n");
    assert.match(head, /^The Mac is offline\. .*This list of repos is from \d{4}-/);
    assert.deepEqual(JSON.parse(json), [REPO, OTHER]);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R5.S3: start_spec and make_spec with no agent fail at once with the Mac is offline", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await seed(h);
    const { accessToken } = await signIn(t.base);
    for (const [tool, args] of [
      ["start_spec", { repo: REPO, slug: "alpha" }],
      ["make_spec", { repo: REPO, title: "T", brief: "B" }],
    ] as const) {
      const started = Date.now();
      const result = await callTool(t.base, accessToken, tool, args);
      assert.ok(Date.now() - started < 5000);
      assert.equal(result.isError, true);
      assert.match(result.content[0].text, /^The Mac is offline\. The relay did not see the Mac since it started at /);
    }
  } finally {
    await t.close();
    await h.close();
  }
});

test("online, the read tools still go to the agent", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await seed(h);
    const agent = await connectAgent(t.base);
    agent.answer = (call) => ({ ok: true, text: `from the Mac: ${call.tool}` });
    const { accessToken } = await signIn(t.base);
    for (const tool of ["list_repos", "list_specs", "read_spec"]) {
      const result = await callTool(t.base, accessToken, tool, tool === "read_spec" ? { repo: REPO, slug: "alpha" } : {});
      assert.equal(result.content[0].text, `from the Mac: ${tool}`);
    }
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});
