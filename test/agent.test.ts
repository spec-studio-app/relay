import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { eventually, harness, startTestServer } from "./temporal.js";
import { AGENT_SECRET, callTool, connectAgent, signIn, sleep } from "./helpers.js";
import { repoKeyOf } from "../src/mirror.js";
import { specWorkflowId } from "../src/workflows/types.js";
import { SA } from "../src/workflows/searchAttributes.js";
import { RELAY_VERSION } from "../src/version.js";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

const REPO = "/Users/me/code/app";
const HELLO = { type: "hello", version: 3, agentVersion: "0.9.0", repos: [REPO, "/Users/me/code/site"] };
const spec = (slug: string) => ({ repoKey: repoKeyOf(REPO), slug });

test("R1.S1: a paired version 1 agent gets tool calls only: no welcome, sync or message frame", async () => {
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

test("R1.S2: a version 3 hello with no wires gets a welcome, and the relay keeps the repos with the time", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    const welcome = await agent.next("welcome");
    assert.deepEqual(welcome, { type: "welcome", version: 3, relayVersion: RELAY_VERSION, wires: [3] });
    assert.equal(t.relay.hub.wireVersion, 3);
    const repos = await eventually(() => h.configStore.repos());
    assert.deepEqual(repos.repos, HELLO.repos);
    assert.ok(Date.parse(repos.at) > Date.now() - 60_000);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R4.S1, R4.S2, R4.S3: sync frames go to the workflow in order; a copy answers synced and changes nothing", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    await agent.next("welcome");
    const state = { slug: "sync-me", title: "Sync me", stage: "plan", status: "working", statusNote: "Planning." };
    // Three frames sent at once, with no wait: the relay applies them in order.
    for (const line of [1, 2, 3]) {
      agent.send({
        type: "sync",
        spec: { ...spec("sync-me"), repo: REPO },
        lines: [{ line, event: { t: "2026-11-23T12:00:00Z", kind: "note", text: `line ${line}` } }],
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
    agent.send({ type: "sync", spec: spec("sync-me"), lines: [{ line: 3, event: { kind: "note", text: "line 3" } }], state, turn: null });
    assert.equal((await agent.next("synced", (f) => f.line === 3)).error, undefined);
    assert.equal((await h.mirror.state(spec("sync-me")))?.events.length, 3);

    // A frame that is not valid gets an error.
    agent.send({ type: "sync", spec: spec("sync-me"), lines: [{ line: 4, event: "not an object" }] });
    assert.match((await agent.next("synced", (f) => f.line === 4)).error, /not valid/);
    // Lines out of order, and a line below 1, are not valid either; nothing applies.
    agent.send({ type: "sync", spec: spec("sync-me"), lines: [{ line: 6, event: {} }, { line: 5, event: {} }] });
    assert.match((await agent.next("synced", (f) => f.line === 5)).error, /not valid/);
    agent.send({ type: "sync", spec: spec("sync-me"), lines: [{ line: 0, event: {} }] });
    assert.match((await agent.next("synced", (f) => f.line === 0)).error, /not valid/);
    assert.equal((await h.mirror.state(spec("sync-me")))?.line, 3);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("a gone frame marks the spec dropped and gets gone-ok; offline list_specs shows it only with all", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    await agent.next("welcome");
    const state = { slug: "deleted-on-mac", title: "Deleted", stage: "build", status: "working" };
    agent.send({ type: "sync", spec: { ...spec("deleted-on-mac"), repo: REPO }, lines: [{ line: 1, event: { kind: "note" } }], state, turn: null });
    agent.send({ type: "gone", spec: spec("deleted-on-mac") });
    await agent.next("synced", (f) => f.line === 1);
    assert.deepEqual(await agent.next("gone-ok"), { type: "gone-ok", spec: spec("deleted-on-mac") });
    assert.equal((await h.mirror.state(spec("deleted-on-mac")))?.dropped, true);

    // A gone for a spec that the relay never saw also gets gone-ok.
    agent.send({ type: "gone", spec: spec("unknown") });
    assert.deepEqual(await agent.next("gone-ok", (f) => f.spec.slug === "unknown"), { type: "gone-ok", spec: spec("unknown") });
    await agent.close();
    await sleep(100);

    const { accessToken } = await signIn(t.base);
    const slugs = async (args: Record<string, unknown>) => {
      const r = await callTool(t.base, accessToken, "list_specs", args);
      return JSON.parse(r.content[0].text.split("\n").slice(1).join("\n")).specs;
    };
    const all = await eventually(async () => {
      const l = await slugs({ repo: REPO, all: true });
      return l.some((s: any) => s.slug === "deleted-on-mac" && s.dropped === true) ? l : null;
    });
    assert.ok(all);
    assert.ok(!(await slugs({ repo: REPO })).some((s: any) => s.slug === "deleted-on-mac"));
    const sent = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "deleted-on-mac", text: "Hi" });
    assert.equal(sent.isError, true);
    assert.match(sent.content[0].text, /no longer exists on the Mac/);
  } finally {
    await t.close();
    await h.close();
  }
});

test("a state frame replaces the state and the turn after the earlier lines, and keeps the cursor and the events", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    await agent.next("welcome");
    const base = { slug: "turns", title: "Turns", stage: "plan", status: "working", mode: "guided" };
    agent.send({ type: "sync", spec: { ...spec("turns"), repo: REPO }, lines: [{ line: 1, event: { kind: "note" } }], state: base, turn: { turn: "claude", needsYou: false } });
    // Sent at once: it applies after line 1.
    agent.send({
      type: "state",
      spec: spec("turns"),
      state: { ...base, title: "Turns now", stage: "build", status: "awaiting-approval", mode: "autopilot" },
      turn: { turn: "you", needsYou: true },
    });
    await agent.next("synced", (f) => f.line === 1);
    const mirrored = await eventually(async () => {
      const s = await h.mirror.state(spec("turns"));
      return s?.turn?.needsYou ? s : null;
    });
    assert.equal(mirrored.line, 1);
    assert.equal(mirrored.events.length, 1);
    assert.equal((mirrored.state as Record<string, unknown>).status, "awaiting-approval");
    const handle = h.clients.data.workflow.getHandle(specWorkflowId(h.deployId, spec("turns").repoKey, "turns"));
    const sa = await eventually(async () => {
      const d = (await handle.describe()).typedSearchAttributes;
      return d.get(SA.NeedsYou) === true ? d : null;
    });
    assert.equal(sa.get(SA.Status), "awaiting-approval");
    assert.equal(sa.get(SA.Stage), "build");
    assert.equal(sa.get(SA.Mode), "autopilot");
    assert.equal(sa.get(SA.SpecTitle), "Turns now");
    // The next line still applies after it.
    agent.send({ type: "sync", spec: spec("turns"), lines: [{ line: 2, event: { kind: "note" } }], state: base, turn: null });
    assert.equal((await agent.next("synced", (f) => f.line === 2)).error, undefined);

    // A state frame for a spec with no workflow starts no run, and gets no answer.
    agent.send({ type: "state", spec: spec("no-run"), state: base, turn: null });
    await sleep(500);
    assert.equal(await h.mirror.state(spec("no-run")), null);
    assert.equal(agent.frames.length, 0);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R6.S1: a sync frame before the welcome gets synced with an error, and the relay logs it", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    const agent = await connectAgent(t.base);
    agent.send({ type: "sync", spec: spec("v1"), lines: [{ line: 1, event: {} }], state: null, turn: null });
    const synced = await agent.next("synced");
    assert.equal(synced.line, 1);
    assert.match(synced.error, /did not welcome this agent/);
    assert.equal(await h.mirror.state(spec("v1")), null);
    assert.ok(t.logs.some((e) => e.event === "agent_frame_refused" && e.frame === "sync"));
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R6.S2: a state frame with no spec gets an error frame; a frame of an unknown type and text that is not JSON do too", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    await agent.next("welcome");
    agent.send({ type: "state", state: { stage: "build" }, turn: null });
    const error = await agent.next("error");
    assert.equal(error.frame, "state");
    assert.match(error.text, /needs a spec/);
    assert.ok(t.logs.some((e) => e.event === "agent_frame_refused" && e.frame === "state"));

    agent.send({ type: "teleport" });
    assert.match((await agent.next("error", (f) => f.frame === "teleport")).text, /does not know the frame type/);
    agent.ws.send("not json");
    assert.match((await agent.next("error", (f) => f.frame === "unknown")).text, /not JSON/);
    agent.send({ type: "download-url", id: "d1" });
    const download = await agent.next("download-url", (f) => f.id === "d1");
    assert.equal(download.ok, false);
    assert.ok(t.logs.some((e) => e.event === "agent_frame_refused" && e.frame === "download-url"));
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R1.S1: a hello with wires and an app version gets a welcome with the common version and the relay's wires", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, { ...HELLO, agentVersion: "0.1.2", wires: [3] });
    assert.deepEqual(await agent.next("welcome"), { type: "welcome", version: 3, relayVersion: RELAY_VERSION, wires: [3] });
    assert.deepEqual(t.relay.hub.lastAgent?.wires, [3]);
    assert.equal(t.relay.hub.lastAgent?.appVersion, "0.1.2");
    assert.equal(t.relay.hub.lastAgent?.compatible, true);
    assert.equal(t.relay.hub.mismatchNotice(null), null);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R2.S1, R5.S1: a version 2 agent gets incompatible (app too old) and close 4002; the tools then start with the notice", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, { ...HELLO, version: 2, agentVersion: "0.1.0" });
    const frame = await agent.next("incompatible");
    assert.equal(frame.tooOld, "app");
    assert.equal(frame.minAppVersion, "0.1.1");
    assert.equal(frame.relayVersion, RELAY_VERSION);
    assert.deepEqual(frame.wires, [3]);
    assert.match(frame.text, /The relay needs app v0\.1\.1 or later\. Update the app\./);
    const closed = await agent.closed;
    assert.equal(closed.code, 4002);
    assert.equal(closed.reason, "incompatible: update the app to v0.1.1 or later");
    const log = t.logs.find((e) => e.event === "agent_incompatible");
    assert.equal(log?.appVersion, "0.1.0");
    assert.equal(log?.wires, "2");
    assert.equal(await h.mirror.state(spec("old-app")), null);

    // R5.S1: the MCP tools tell Claude AI.
    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "list_specs", {});
    assert.match(
      result.content[0].text,
      new RegExp(
        `^The Mac app \\(v0\\.1\\.0, wire 2\\) is not compatible with this relay \\(${RELAY_VERSION.replace(/\./g, "\\.")}, wire 3\\)\\. ` +
          `Nothing has synced since .+\\. Ask the user to update the app\\.`,
      ),
    );
    const read = await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "old-app" });
    assert.match(read.content[0].text, /^The Mac app \(v0\.1\.0, wire 2\) is not compatible/);

    // R5.S2: a good hello clears the notice.
    const current = await connectAgent(t.base, AGENT_SECRET, HELLO);
    await current.next("welcome");
    current.answer = () => ({ ok: true, text: '{"specs":[]}' });
    const after = await callTool(t.base, accessToken, "list_specs", {});
    assert.equal(after.content[0].text, '{"specs":[]}');
    await current.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R2.S2: a hello with only a newer wire version gets incompatible (relay too old) and close 4002", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, { ...HELLO, version: 4, wires: [4] });
    const frame = await agent.next("incompatible");
    assert.equal(frame.tooOld, "relay");
    assert.match(frame.text, /Update the relay/);
    const closed = await agent.closed;
    assert.equal(closed.code, 4002);
    assert.equal(closed.reason, "incompatible: update the relay to wire version 4");
    assert.match(t.relay.hub.mismatchNotice(null) ?? "", /since the relay started at .+ Ask the user to update the relay\./);
    // A failed read of the last sync time leaves the time out.
    assert.doesNotMatch(t.relay.hub.mismatchNotice(undefined) ?? "", /Nothing has synced/);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R1.S1: one frame with seven lines answers synced with the last line", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO);
    await agent.next("welcome");
    const lines = Array.from({ length: 7 }, (_, i) => ({ line: 41 + i, event: { kind: "note", text: `n${41 + i}` } }));
    agent.send({ type: "sync", spec: { ...spec("seven"), repo: REPO }, first: true, lines, state: { title: "Seven", stage: "plan" }, turn: null });
    assert.deepEqual(await agent.next("synced"), { type: "synced", spec: spec("seven"), line: 47 });
    const mirrored = await h.mirror.state(spec("seven"));
    assert.equal(mirrored?.line, 47);
    assert.equal(mirrored?.events.length, 7);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("an old app keeps the socket until the close delay, and its tool calls still work with the notice", async () => {
  const h = await harness(env);
  const t = await h.relay({ incompatibleCloseMs: 1500 });
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, { ...HELLO, version: 2, agentVersion: "0.1.0" });
    agent.answer = () => ({ ok: true, text: '{"specs":[]}' });
    await agent.next("incompatible");
    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "list_specs", {});
    assert.match(result.content[0].text, /^The Mac app \(v0\.1\.0, wire 2\) is not compatible/);
    assert.match(result.content[0].text, /\{"specs":\[\]\}$/);
    const closed = await agent.closed;
    assert.equal(closed.code, 4002);
  } finally {
    await t.close();
    await h.close();
  }
});

test("a hello with a huge wires list does not crash the relay", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const wires = Array.from({ length: 300_000 }, (_, i) => i + 10);
    const agent = await connectAgent(t.base, AGENT_SECRET, { ...HELLO, version: 4, wires });
    const frame = await agent.next("incompatible");
    assert.equal(frame.tooOld, "relay");
    assert.ok((t.relay.hub.lastAgent?.wires.length ?? 0) <= 16);
    assert.equal((await agent.closed).code, 4002);
  } finally {
    await t.close();
    await h.close();
  }
});
