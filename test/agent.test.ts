import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { eventually, harness, startTestServer } from "./temporal.js";
import { AGENT_SECRET, callTool, connectAgent, signIn, sleep } from "./helpers.js";
import { repoKeyOf } from "../src/mirror.js";
import { RELAY_VERSION } from "../src/version.js";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

const REPO = "/Users/me/code/app";
const HELLO2 = { type: "hello", version: 2, agentVersion: "0.9.0", repos: [REPO, "/Users/me/code/site"] };
const spec = (slug: string) => ({ repoKey: repoKeyOf(REPO), slug });

test("R1.S1: a version 1 agent keeps working and gets no welcome, sync or message frame", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    // A message waits for the Mac: a version 1 agent must not get it.
    await h.mirror.sync({ ...spec("waiting"), repo: REPO }, { line: 1, event: { kind: "note" }, state: null, turn: null });
    await h.mirror.send(spec("waiting"), { text: "Use the old icon." });

    const agent = await connectAgent(t.base);
    agent.answer = () => ({ ok: true, text: '{"specs":[]}' });
    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "list_specs", {});
    assert.equal(result.isError, undefined);
    assert.equal(result.content[0].text, '{"specs":[]}');
    await sleep(300);
    assert.deepEqual(agent.frames, [], "the relay sent only call frames");
    assert.equal(t.relay.hub.wireVersion, 1);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("a version 2 hello gets a welcome, and the relay keeps the repos with the time", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO2);
    const welcome = await agent.next("welcome");
    assert.deepEqual(welcome, { type: "welcome", version: 2, relayVersion: RELAY_VERSION });
    assert.equal(t.relay.hub.wireVersion, 2);
    const repos = await eventually(() => h.configStore.repos());
    assert.deepEqual(repos.repos, HELLO2.repos);
    assert.ok(Date.parse(repos.at) > Date.now() - 60_000);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R4.S1, R4.S2, R4.S3: sync lines go to the workflow in order; a copy answers synced and changes nothing", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO2);
    await agent.next("welcome");
    const state = { slug: "sync-me", title: "Sync me", stage: "plan", status: "working", statusNote: "Planning." };
    // Three lines sent at once, with no wait: the relay applies them in order.
    for (const line of [1, 2, 3]) {
      agent.send({
        type: "sync",
        spec: { ...spec("sync-me"), repo: REPO },
        line,
        event: { t: "2026-11-23T12:00:00Z", kind: "note", text: `line ${line}` },
        state,
        turn: { turn: "claude", needsYou: false },
      });
    }
    for (const line of [1, 2, 3]) {
      const synced = await agent.next("synced", (f) => f.line === line);
      assert.deepEqual(synced, { type: "synced", spec: spec("sync-me"), line });
    }
    const mirrored = await h.mirror.state(spec("sync-me"));
    assert.equal(mirrored?.line, 3);
    assert.deepEqual(mirrored?.events.map((e) => e.event.text), ["line 1", "line 2", "line 3"]);
    assert.equal(mirrored?.spec.repo, REPO);

    // The agent stopped before synced came back and sends line 3 again.
    agent.send({ type: "sync", spec: spec("sync-me"), line: 3, event: { kind: "note", text: "line 3" }, state, turn: null });
    assert.equal((await agent.next("synced", (f) => f.line === 3)).error, undefined);
    assert.equal((await h.mirror.state(spec("sync-me")))?.events.length, 3);

    // A frame that is not valid gets an error.
    agent.send({ type: "sync", spec: spec("sync-me"), line: 4, event: "not an object" });
    assert.match((await agent.next("synced", (f) => f.line === 4)).error, /not valid/);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("a version 1 agent cannot send sync frames", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    const agent = await connectAgent(t.base);
    agent.send({ type: "sync", spec: spec("v1"), line: 1, event: {}, state: null, turn: null });
    await sleep(300);
    assert.deepEqual(agent.frames, []);
    assert.equal(await h.mirror.state(spec("v1")), null);
    assert.ok(t.logs.some((e) => e.event === "agent_bad_message"));
  } finally {
    await t.close();
    await h.close();
  }
});
