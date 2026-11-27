import { after, before, beforeEach, test } from "node:test";
import { randomBytes } from "node:crypto";
import assert from "node:assert/strict";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { eventually, harness, startTestServer } from "./temporal.js";
import { AGENT_SECRET, callTool, connectAgent, signIn } from "./helpers.js";
import { repoKeyOf, specAddress } from "../src/mirror.js";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

// Each test uses its own repos.
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
    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 3, repos: [REPO, OTHER] });
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
    assert.ok(!/filter needs the Mac online/.test(head));
    assert.deepEqual(JSON.parse(json), [REPO, OTHER]);
    // R17.S4: the relay does not know areas, so an area filter shows all repos and says so.
    const filtered = await callTool(t.base, accessToken, "list_repos", { area: "Vendgogh" });
    const [fhead, fjson] = filtered.content[0].text.split("\n");
    assert.match(fhead, /The area filter needs the Mac online\./);
    assert.deepEqual(JSON.parse(fjson), [REPO, OTHER]);
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

test("R17.S4: offline list_specs with an area or a project shows all specs and says the filter needs the Mac", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await seed(h);
    const { accessToken } = await signIn(t.base);
    const plain = await callTool(t.base, accessToken, "list_specs", {});
    assert.ok(!/filter needs the Mac online/.test(plain.content[0].text));
    for (const [args, word] of [
      [{ area: "Vendgogh" }, "area"],
      [{ project: "Backend plugins" }, "project"],
    ] as const) {
      const r = await callTool(t.base, accessToken, "list_specs", args);
      assert.equal(r.isError, undefined);
      const text = r.content[0].text as string;
      assert.match(text, new RegExp(`The ${word} filter needs the Mac online\\.`));
      assert.match(text, /"slug": "alpha"/);
      assert.match(text, /"slug": "beta"/);
    }
    const projects = await callTool(t.base, accessToken, "list_projects", {});
    assert.equal(projects.isError, true);
    assert.match(projects.content[0].text, /^The Mac is offline\. The relay did not see the Mac since it started at /);
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

test("with no turn from the app, the offline answers derive the turn from the status like studio specs", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.mirror.sync(specAddress(REPO, "waiting"), {
      line: 1,
      event: { kind: "status", to: "awaiting-approval" },
      state: show("waiting", { status: "awaiting-approval" }),
      turn: null,
    });
    await h.mirror.sync(specAddress(REPO, "stuck"), { line: 1, event: { kind: "note" }, state: show("stuck", { status: "blocked" }), turn: null });
    await h.mirror.sync(specAddress(REPO, "busy"), { line: 1, event: { kind: "note" }, state: show("busy"), turn: null });
    const { accessToken } = await signIn(t.base);
    const list = await eventually(async () => {
      const r = await callTool(t.base, accessToken, "list_specs", { repo: REPO });
      const l = JSON.parse(r.content[0].text.split("\n").slice(1).join("\n"));
      return l.specs.length === 3 ? l : null;
    });
    const bySlug = Object.fromEntries(list.specs.map((s: any) => [s.slug, s]));
    assert.deepEqual([bySlug.waiting.turn, bySlug.waiting.needsYou], ["yours", true]);
    assert.deepEqual([bySlug.stuck.turn, bySlug.stuck.needsYou], ["blocked", true]);
    assert.deepEqual([bySlug.busy.turn, bySlug.busy.needsYou], ["working", false]);
    // The NeedsYou search attribute agrees.
    const needing = await eventually(async () => {
      const found: string[] = [];
      for await (const wf of h.clients.data.workflow.list({ query: `RelayDeployId = '${h.deployId}' AND NeedsYou = true` })) found.push(wf.workflowId);
      return found.length === 2 ? found : null;
    });
    assert.equal(needing.length, 2);

    const read = await callTool(t.base, accessToken, "read_spec", { repo: REPO, slug: "waiting" });
    assert.match(read.content[0].text, /## Turn\n\nyours\. Needs the user: yes\./);
  } finally {
    await t.close();
    await h.close();
  }
});

test("offline list_specs without all leaves out a merged spec whose workflow still runs", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    const merged = specAddress(REPO, "shipped");
    await h.mirror.sync(merged, { line: 1, event: { kind: "note" }, state: show("shipped"), turn: null });
    // A message waits for the Mac, so the workflow stays open after the merge.
    await h.mirror.send(merged, { text: "Thanks." });
    await h.mirror.sync(merged, {
      line: 2,
      event: { kind: "stage", to: "merged" },
      state: show("shipped", { stage: "merged", status: "idle" }),
      turn: null,
    });
    await h.mirror.sync(specAddress(REPO, "open"), { line: 1, event: { kind: "note" }, state: show("open"), turn: null });
    const { accessToken } = await signIn(t.base);
    const slugs = async (args: Record<string, unknown>) => {
      const r = await callTool(t.base, accessToken, "list_specs", args);
      return JSON.parse(r.content[0].text.split("\n").slice(1).join("\n")).specs.map((s: any) => s.slug).sort();
    };
    // Wait until visibility shows both with all, then check the list without all.
    await eventually(async () => {
      const all = await slugs({ repo: REPO, all: true });
      return all.length === 2 && all[1] === "shipped" ? all : null;
    });
    await eventually(async () => {
      const open = await slugs({ repo: REPO });
      return open.length === 1 && open[0] === "open" ? open : null;
    });
    assert.deepEqual(await slugs({ repo: REPO }), ["open"]);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R5.S3: offline list_repos also lists each desk with the time of its last state", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 3, repos: [REPO, OTHER] });
    await agent.next("welcome");
    agent.send({
      type: "desks",
      desks: [
        { repoKey: repoKeyOf(REPO), repo: REPO, open: true, turn: { turn: "idle", needsYou: false }, canMessage: true, reason: null },
        { repoKey: repoKeyOf(OTHER), repo: OTHER, open: false, turn: null, canMessage: false, reason: "Claude AI cannot message this desk. The user can turn it on in Spec Studio, Settings, Remote Relay." },
      ],
    });
    await eventually(async () => (await h.desks.list()).filter((d) => d.memo.stateAt).length === 2);
    await agent.close();
    const { accessToken } = await signIn(t.base);
    const result = await eventually(async () => {
      const r = await callTool(t.base, accessToken, "list_repos");
      return r.isError || !r.content[0].text.includes(OTHER) || !r.content[0].text.split("\n")[3]?.includes(OTHER) ? null : r;
    });
    const [head, repos, deskHead, desks] = result.content[0].text.split("\n");
    assert.match(head, /^The Mac is offline\./);
    assert.deepEqual(JSON.parse(repos), [REPO, OTHER]);
    assert.match(deskHead, /stateAt/);
    const parsed = JSON.parse(desks) as Array<Record<string, unknown>>;
    const byRepo = new Map(parsed.map((d) => [d.repo, d]));
    assert.deepEqual({ ...byRepo.get(REPO), stateAt: undefined }, {
      repo: REPO,
      open: true,
      turn: { turn: "idle", needsYou: false },
      canMessage: true,
      reason: null,
      stateAt: undefined,
    });
    assert.equal(byRepo.get(OTHER)?.open, false);
    assert.equal(byRepo.get(OTHER)?.canMessage, false);
    assert.match(String(byRepo.get(OTHER)?.reason), /cannot message this desk/);
    for (const d of parsed) assert.ok(!Number.isNaN(Date.parse(String(d.stateAt))));
    // R5.S4: list_specs shows no desk.
    const specs = await callTool(t.base, accessToken, "list_specs");
    assert.deepEqual(JSON.parse(specs.content[0].text.split("\n").slice(1).join("\n")).specs, []);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R11.S2: offline list_specs leaves out the specs of a repo that the Mac's list does not have, unless archived is true", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await seed(h);
    await h.startConfig();
    // The Mac's list has REPO only: OTHER (with beta) is archived.
    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 3, repos: [REPO] });
    await agent.next("welcome");
    await eventually(() => h.configStore.repos());
    await agent.close();
    const { accessToken } = await signIn(t.base);
    const slugs = async (args: Record<string, unknown>) => {
      const r = await eventually(async () => {
        const x = await callTool(t.base, accessToken, "list_specs", args);
        return x.isError || !x.content[0].text.includes('"alpha"') ? null : x;
      });
      const [, ...body] = r.content[0].text.split("\n");
      return JSON.parse(body.join("\n")).specs as Array<Record<string, unknown>>;
    };
    const plain = await slugs({});
    assert.deepEqual(plain.map((s) => s.slug), ["alpha"]);
    assert.equal(plain[0].archived, undefined);

    const withArchived = await slugs({ archived: true });
    const bySlug = Object.fromEntries(withArchived.map((s) => [s.slug, s]));
    assert.deepEqual(Object.keys(bySlug).sort(), ["alpha", "beta"]);
    assert.equal(bySlug.beta.archived, true);
    assert.equal(bySlug.alpha.archived, undefined);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R11.S2: offline list_specs filters nothing when the relay has no repo list", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await seed(h);
    const { accessToken } = await signIn(t.base);
    const r = await eventually(async () => {
      const x = await callTool(t.base, accessToken, "list_specs", {});
      return x.content[0].text.includes('"beta"') && x.content[0].text.includes('"alpha"') ? x : null;
    });
    const [, ...body] = r.content[0].text.split("\n");
    const specs = JSON.parse(body.join("\n")).specs as Array<Record<string, unknown>>;
    assert.deepEqual(specs.map((s) => s.slug).sort(), ["alpha", "beta"]);
    assert.ok(specs.every((s) => s.archived === undefined));
  } finally {
    await t.close();
    await h.close();
  }
});

test("R7.S2: offline read_spec of a paused spec says that the spec is paused, with the note and the time", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.mirror.sync(specAddress(REPO, "held"), {
      line: 1,
      event: { kind: "note" },
      state: show("held", { paused: { at: "2026-11-01T10:00:00Z", by: "ross", note: "Waiting for the API" } }),
      turn: null,
    });
    await h.mirror.sync(specAddress(REPO, "free"), { line: 1, event: { kind: "note" }, state: show("free"), turn: null });
    const { accessToken } = await signIn(t.base);
    const paused = await callTool(t.base, accessToken, "read_spec", { repo: REPO, slug: "held" });
    assert.equal(paused.isError, undefined);
    assert.match(paused.content[0].text, /The spec is paused: Waiting for the API\. Paused at 2026-11-01T10:00:00Z\./);
    const free = await callTool(t.base, accessToken, "read_spec", { repo: REPO, slug: "free" });
    assert.ok(!/The spec is paused/.test(free.content[0].text));
  } finally {
    await t.close();
    await h.close();
  }
});
