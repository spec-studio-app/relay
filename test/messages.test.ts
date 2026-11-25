import { after, before, beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { eventually, harness, startTestServer } from "./temporal.js";
import { AGENT_SECRET, callTool, connectAgent, mcpRequest, signIn } from "./helpers.js";
import { repoKeyOf, specAddress } from "../src/mirror.js";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

let REPO = "";
beforeEach(() => {
  REPO = `/Users/me/code/app-${randomBytes(4).toString("hex")}`;
});

const HELLO = () => ({ type: "hello", version: 3, repos: [REPO] });

async function synced(h: Awaited<ReturnType<typeof harness>>, slug: string, stage = "build") {
  await h.mirror.sync(specAddress(REPO, slug), {
    line: 1,
    event: { kind: "note" },
    state: { slug, title: slug, stage, status: "working" },
    turn: null,
  });
}

function json(result: { content: Array<{ text: string }> }) {
  return JSON.parse(result.content[0].text);
}

test("R6.S1: send_message with no agent returns seq 1, and read_spec shows one message for the Mac", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await synced(h, "icon");
    const { accessToken } = await signIn(t.base);
    const sent = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "icon", text: "Use the old icon." });
    assert.equal(sent.isError, undefined, sent.content[0].text);
    assert.equal(json(sent).seq, 1);
    const read = await callTool(t.base, accessToken, "read_spec", { repo: REPO, slug: "icon" });
    assert.match(read.content[0].text, /1 message waits for the Mac/);

    const missing = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "nope", text: "x" });
    assert.equal(missing.isError, true);
    assert.match(missing.content[0].text, /no synced spec/);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R6.S2: after welcome the relay pushes the waiting messages; delivered drops them", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    await synced(h, "push");
    await h.mirror.send(specAddress(REPO, "push"), { text: "First." });
    await h.mirror.send(specAddress(REPO, "push"), { text: "Second." });
    await eventually(async () => (await h.mirror.list({ pendingForMac: true })).length === 1);

    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO());
    await agent.next("welcome");
    const first = await agent.next("message", (f) => f.seq === 1);
    const second = await agent.next("message", (f) => f.seq === 2);
    assert.deepEqual(first.spec, { repoKey: repoKeyOf(REPO), slug: "push", repo: REPO });
    assert.equal(first.message.text, "First.");
    assert.equal(first.message.from, "coordinator");
    assert.equal(first.message.stage, "");
    assert.match(first.message.id, /^[0-9a-f-]{36}$/);
    assert.equal(second.message.text, "Second.");

    agent.send({ type: "delivered", spec: { repoKey: repoKeyOf(REPO), slug: "push" }, seq: 1 });
    await eventually(async () => (await h.mirror.state(specAddress(REPO, "push")))?.mac.messages.length === 1);

    // A new message goes out at once while the agent is connected.
    const { accessToken } = await signIn(t.base);
    await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "push", text: "Third.", replyTo: "abc" });
    const third = await agent.next("message", (f) => f.seq === 3);
    assert.equal(third.message.replyTo, "abc");

    // A refused message becomes a bounce note for Claude AI.
    agent.send({ type: "delivered", spec: { repoKey: repoKeyOf(REPO), slug: "push" }, seq: 3, error: "sync is off" });
    const state = await eventually(async () => {
      const s = await h.mirror.state(specAddress(REPO, "push"));
      return s && s.coordinator.messages.length === 1 ? s : null;
    });
    assert.equal(state.coordinator.messages[0].text, "The Mac did not take the message: sync is off");
    assert.equal(state.coordinator.messages[0].from, "session");
    assert.deepEqual(state.mac.messages.map((m) => m.seq), [2]);

    // A new connection gets the undelivered message again.
    await agent.close();
    const again = await connectAgent(t.base, AGENT_SECRET, HELLO());
    await again.next("welcome");
    assert.equal((await again.next("message")).seq, 2);
    await again.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R7.S1, R7.S2: a synced message event reaches read_messages; two cursors read the same messages", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await synced(h, "answer", "plan");
    await h.mirror.sync(specAddress(REPO, "answer"), {
      line: 2,
      event: {
        t: "2026-11-23T12:00:00Z",
        kind: "message",
        stage: "plan",
        id: "m-1",
        text: "The plan is ready.",
        attachments: [{ filename: "gone.png", contentType: "image/png", size: 3, sha256: "ab", error: "The file is gone." }],
      },
      state: { slug: "answer", stage: "plan", status: "awaiting-approval" },
      turn: { turn: "yours", needsYou: true },
    });
    const { accessToken } = await signIn(t.base);
    const first = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "answer" }));
    assert.equal(first.stage, "plan");
    assert.equal(first.cursor, 1);
    assert.equal(first.messages.length, 1);
    const m = first.messages[0];
    assert.equal(m.seq, 1);
    assert.equal(m.id, "m-1");
    assert.equal(m.text, "The plan is ready.");
    assert.equal(m.stage, "plan");
    assert.equal(m.createdAt, "2026-11-23T12:00:00Z");
    assert.deepEqual(m.attachments, [
      { filename: "gone.png", contentType: "image/png", size: 3, sha256: "ab", error: "The file is gone." },
    ]);

    // The second chat has its own cursor and still gets the message.
    const second = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "answer", after: 0 }));
    assert.deepEqual(second.messages.map((x: any) => x.seq), [1]);

    // After the cursor, the call waits, then returns no message.
    const started = Date.now();
    const none = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "answer", after: 1 }));
    assert.deepEqual(none.messages, []);
    assert.equal(none.cursor, 1);
    assert.ok(Date.now() - started >= 1000);

    // A message that arrives during the wait returns at once.
    const waiting = callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "answer", after: 1 });
    await h.mirror.sync(specAddress(REPO, "answer"), {
      line: 3,
      event: { kind: "message", id: "m-2", text: "Done." },
      state: null,
      turn: null,
    });
    assert.deepEqual(json(await waiting).messages.map((x: any) => x.text), ["Done."]);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R7.S3: read_messages with no slug returns the unread messages of two specs", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    for (const slug of ["one", "two", "quiet"]) await synced(h, slug);
    for (const slug of ["one", "two"]) {
      await h.mirror.sync(specAddress(REPO, slug), {
        line: 2,
        event: { kind: "message", text: `news from ${slug}` },
        state: null,
        turn: null,
      });
    }
    await eventually(async () => (await h.mirror.list({ pendingForCoordinator: true })).length === 2);
    const { accessToken } = await signIn(t.base);
    const all = json(await callTool(t.base, accessToken, "read_messages", {}));
    assert.deepEqual(all.messages.map((m: any) => m.text).sort(), ["news from one", "news from two"]);
    assert.deepEqual(all.specs.map((s: any) => s.slug).sort(), ["one", "two"]);
    assert.equal(all.messages[0].spec.repo, REPO);

    // They are read now: the next call has none.
    await eventually(async () => (await h.mirror.list({ pendingForCoordinator: true })).length === 0);
    const again = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO }));
    assert.deepEqual(again.messages, []);
  } finally {
    await t.close();
    await h.close();
  }
});

test("the relay lists the message tools when it has Temporal", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    const { accessToken } = await signIn(t.base);
    const list = await mcpRequest(t.base, accessToken, "tools/list");
    const names = list.message?.result.tools.map((tool: { name: string }) => tool.name);
    assert.ok(names.includes("send_message") && names.includes("read_messages"));
  } finally {
    await t.close();
    await h.close();
  }
});
