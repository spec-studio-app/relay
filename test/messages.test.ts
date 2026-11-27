import { after, before, beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { eventually, harness, startTestServer } from "./temporal.js";
import { AGENT_SECRET, callTool, connectAgent, mcpRequest, signIn } from "./helpers.js";
import { repoKeyOf, specAddress } from "../src/mirror.js";
import { DESK_ARCHIVED_REFUSAL, DESK_ARCHIVED_TEXT, DESK_GONE_TEXT, DESK_OFF_TEXT } from "../src/workflows/types.js";

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
    // R5.S3: the error gives the time of the last sync of any spec (visibility is eventually consistent).
    await eventually(async () => {
      const again = await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "nope" });
      return /The last sync of any spec was at \d{4}-\d\d-\d\dT/.test(again.content[0].text) ? again : null;
    });
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
    const again = json(await callTool(t.base, accessToken, "read_messages", {}));
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

// ---- desks (relay-desk-messages) ----

function deskOf(path: string, over: Record<string, unknown> = {}) {
  return {
    repoKey: repoKeyOf(path),
    repo: path,
    open: true,
    turn: { turn: "idle", needsYou: false },
    canMessage: true,
    reason: null as string | null,
    ...over,
  };
}

test("R1.S1, R6.S1: send_message with a repo and no slug queues the message for the desk", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.desks.report(deskOf(REPO));
    const { accessToken } = await signIn(t.base);
    // No agent: the message waits for the Mac.
    const offline = await callTool(t.base, accessToken, "send_message", { repo: REPO, text: "Is CI green on main?" });
    assert.equal(offline.isError, undefined, offline.content[0].text);
    const queued = json(offline);
    assert.equal(queued.seq, 1);
    assert.deepEqual(queued.desk, { repo: REPO });
    assert.match(queued.note, /queued for the desk of the repo .* It waits for the Mac/);
    const state = await h.desks.get({ repoKey: repoKeyOf(REPO) });
    assert.deepEqual(state?.mac.messages.map((m) => m.text), ["Is CI green on main?"]);

    // A desk-capable agent gets the next message at once.
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET, HELLO());
    await agent.next("welcome");
    agent.send({ type: "desks", desks: [deskOf(REPO)] });
    assert.equal((await agent.next("desk-message")).seq, 1);
    // The repo's name works too, when one desk has it.
    const name = REPO.split("/").pop()!;
    const online = json(await callTool(t.base, accessToken, "send_message", { repo: name, text: "And the release?" }));
    assert.equal(online.seq, 2);
    assert.equal(online.note, `The message is queued for the desk of the repo ${REPO}.`);
    const pushed = await agent.next("desk-message", (f) => f.seq === 2);
    assert.deepEqual(pushed.desk, { repoKey: repoKeyOf(REPO), repo: REPO });
    assert.equal(pushed.message.text, "And the release?");
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R1.S2: a slug still means the spec, and not the desk", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await synced(h, "themes");
    await h.desks.report(deskOf(REPO));
    const { accessToken } = await signIn(t.base);
    const sent = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "themes", text: "For the spec." });
    assert.equal(sent.isError, undefined, sent.content[0].text);
    assert.deepEqual(json(sent).spec, { repo: REPO, slug: "themes" });
    assert.equal((await h.mirror.state(specAddress(REPO, "themes")))?.mac.messages.length, 1);
    assert.equal((await h.desks.get({ repoKey: repoKeyOf(REPO) }))?.mac.messages.length, 0);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R1.S3: send_message to a repo with no desk fails with a text that names the repo and list_repos", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "send_message", { repo: "nope", text: "Hello" });
    assert.equal(result.isError, true);
    assert.equal(result.content[0].text, 'The relay has no desk for the repo "nope". Call list_repos for the repo paths.');
    const read = await callTool(t.base, accessToken, "read_messages", { repo: "nope" });
    assert.equal(read.isError, true);
    assert.match(read.content[0].text, /no desk for the repo "nope"/);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R2.S2, R2.S5: send_message to a desk that Claude AI cannot message fails with the desk's reason", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.desks.report(deskOf(REPO, { canMessage: false, reason: DESK_OFF_TEXT }));
    const { accessToken } = await signIn(t.base);
    const off = await callTool(t.base, accessToken, "send_message", { repo: REPO, text: "Hello" });
    assert.equal(off.isError, true);
    assert.equal(off.content[0].text, DESK_OFF_TEXT);
    await h.desks.report(deskOf(REPO, { canMessage: false, reason: "Sync is off for this repo on the Mac." }));
    const sync = await callTool(t.base, accessToken, "send_message", { repo: REPO, text: "Hello" });
    assert.equal(sync.isError, true);
    assert.equal(sync.content[0].text, "Sync is off for this repo on the Mac.");
    assert.equal((await h.desks.get({ repoKey: repoKeyOf(REPO) }))?.mac.messages.length, 0);
  } finally {
    await t.close();
    await h.close();
  }
});

test("a desks frame that leaves out an archived repo marks its desk archived, and send_message says how to message it again", async () => {
  const h = await harness(env);
  const t = await h.relay();
  const gone = `${REPO}-gone`;
  try {
    await h.startConfig();
    const { accessToken } = await signIn(t.base);
    let agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 3, repos: [REPO, gone], archivedRepos: [] });
    await agent.next("welcome");
    agent.send({ type: "desks", desks: [deskOf(REPO), deskOf(gone)], archivedRepos: [] });
    await eventually(async () => (await h.desks.get({ repoKey: repoKeyOf(gone) }))?.canMessage === true);

    // The user archives the repo: the Mac leaves it out of the desks and sends it in archivedRepos.
    // The other repo left Spec Studio: it is gone.
    agent.send({ type: "desks", desks: [], archivedRepos: [REPO] });
    const archived = await eventually(async () => {
      const state = await h.desks.get({ repoKey: repoKeyOf(REPO) });
      return state?.reason === DESK_ARCHIVED_TEXT ? state : null;
    });
    assert.equal(archived.canMessage, false);
    assert.equal(archived.open, false);
    await eventually(async () => (await h.desks.get({ repoKey: repoKeyOf(gone) }))?.reason === DESK_GONE_TEXT);

    // Online, by the path and by the name: the archived refusal, not the gone text.
    const byPath = await callTool(t.base, accessToken, "send_message", { repo: REPO, text: "Hello" });
    assert.equal(byPath.isError, true);
    assert.equal(byPath.content[0].text, DESK_ARCHIVED_REFUSAL);
    const name = REPO.split("/").pop()!;
    const byName = await callTool(t.base, accessToken, "send_message", { repo: name, text: "Hello" });
    assert.equal(byName.isError, true);
    assert.equal(byName.content[0].text, DESK_ARCHIVED_REFUSAL);
    // A gone desk keeps its own text.
    const toGone = await callTool(t.base, accessToken, "send_message", { repo: gone, text: "Hello" });
    assert.equal(toGone.content[0].text, DESK_GONE_TEXT);

    // Offline: the same refusal.
    await agent.close();
    await eventually(async () => !t.relay.hub.connected);
    const offline = await callTool(t.base, accessToken, "send_message", { repo: REPO, text: "Hello" });
    assert.equal(offline.isError, true);
    assert.equal(offline.content[0].text, DESK_ARCHIVED_REFUSAL);
    assert.equal((await h.desks.get({ repoKey: repoKeyOf(REPO) }))?.mac.messages.length, 0);

    // The user unarchives the repo: its desk takes messages again.
    agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 3, repos: [REPO], archivedRepos: [] });
    await agent.next("welcome");
    agent.send({ type: "desks", desks: [deskOf(REPO)], archivedRepos: [] });
    await eventually(async () => (await h.desks.get({ repoKey: repoKeyOf(REPO) }))?.canMessage === true);
    const again = await callTool(t.base, accessToken, "send_message", { repo: REPO, text: "Back again." });
    assert.equal(again.isError, undefined, again.content[0].text);
    assert.equal(json(again).seq, 1);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R4.S1, R4.S3: read_messages gives the desk's replies by repo, and with the replies of specs when no repo is given", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.desks.report(deskOf(REPO));
    await h.desks.sync({ repoKey: repoKeyOf(REPO), repo: REPO }, [
      { line: 1, event: { t: "2026-11-24T10:00:00Z", kind: "message", id: "d1", text: "CI is green on main.", replyTo: "abc" } },
    ]);
    await synced(h, "themes");
    await h.mirror.sync(specAddress(REPO, "themes"), {
      line: 2,
      event: { t: "2026-11-24T10:01:00Z", kind: "message", text: "The themes are done." },
      state: null,
      turn: null,
    });
    await eventually(async () => (await h.desks.list({ pendingForCoordinator: true })).length === 1);
    await eventually(async () => (await h.mirror.list({ pendingForCoordinator: true })).length === 1);
    const { accessToken } = await signIn(t.base);
    const all = json(await callTool(t.base, accessToken, "read_messages", {}));
    assert.deepEqual(
      all.messages.map((m: any) => [m.text, m.desk ?? false, m.repo ?? m.spec?.repo]),
      [
        ["CI is green on main.", true, REPO],
        ["The themes are done.", false, REPO],
      ],
    );
    assert.equal(all.messages[0].spec, undefined);
    assert.equal(all.messages[0].replyTo, "abc");
    assert.deepEqual(all.desks, [{ repo: REPO, cursor: 1 }]);
    assert.deepEqual(all.specs.map((s: any) => s.slug), ["themes"]);

    // By repo and no slug: the desk's messages after the cursor, read or not.
    const desk = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO }));
    assert.deepEqual(desk.desk, { repo: REPO });
    assert.equal(desk.cursor, 1);
    assert.deepEqual(desk.messages.map((m: any) => [m.seq, m.id, m.text, m.replyTo, m.desk, m.repo]), [
      [1, "d1", "CI is green on main.", "abc", true, REPO],
    ]);
    // After the cursor, the read waits for a new reply.
    const waiting = callTool(t.base, accessToken, "read_messages", { repo: REPO, after: 1 });
    await new Promise((resolve) => setTimeout(resolve, 300));
    await h.desks.sync({ repoKey: repoKeyOf(REPO) }, [{ line: 2, event: { kind: "message", id: "d2", text: "Done." } }]);
    assert.deepEqual(json(await waiting).messages.map((m: any) => m.text), ["Done."]);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R4.S4, R4.S5: upload_attachment with no slug stores the file in the desk's own prefix, for a desk message only", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.desks.report(deskOf(REPO));
    await synced(h, "files");
    const { accessToken } = await signIn(t.base);
    const base64 = Buffer.from("log text").toString("base64");
    const upload = await callTool(t.base, accessToken, "upload_attachment", { repo: REPO, filename: "build.log", contentType: "text/plain", base64 });
    assert.equal(upload.isError, undefined, upload.content[0].text);
    const file = json(upload);
    assert.ok(file.objectKey.startsWith(`${h.deployId}/${repoKeyOf(REPO)}/_desk/`), file.objectKey);
    // A spec of the same repo cannot use the desk's file.
    const toSpec = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "files", text: "x", attachments: [file.objectKey] });
    assert.equal(toSpec.isError, true);
    const sent = await callTool(t.base, accessToken, "send_message", { repo: REPO, text: "The log.", attachments: [file.objectKey] });
    assert.equal(sent.isError, undefined, sent.content[0].text);
    const state = await h.desks.get({ repoKey: repoKeyOf(REPO) });
    assert.deepEqual(state?.mac.messages[0].attachments?.map((a) => a.filename), ["build.log"]);
    // A spec's file cannot go to the desk.
    const specFile = json(await callTool(t.base, accessToken, "upload_attachment", { repo: REPO, slug: "files", filename: "a.txt", contentType: "text/plain", base64 }));
    const toDesk = await callTool(t.base, accessToken, "send_message", { repo: REPO, text: "x", attachments: [specFile.objectKey] });
    assert.equal(toDesk.isError, true);
    assert.match(toDesk.content[0].text, /for this desk/);
    // No desk: no upload.
    const none = await callTool(t.base, accessToken, "upload_attachment", { repo: "/Users/me/none", filename: "a.txt", contentType: "text/plain", base64 });
    assert.equal(none.isError, true);
    assert.match(none.content[0].text, /no desk for the repo/);
  } finally {
    await t.close();
    await h.close();
  }
});

const showState = (slug: string, stage: string, status: string, note: string) => ({
  slug,
  title: slug,
  stage,
  status,
  statusNote: note,
});

test("R1.S1, R3.S1: a local answer shows as from the user, with its replyTo, and is unread without a slug", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await synced(h, "local", "build");
    const spec = specAddress(REPO, "local");
    await h.mirror.sync(spec, {
      lines: [
        { line: 2, event: { t: "2026-11-24T14:27:00Z", kind: "message", stage: "build", id: "s-1", text: "If Ross agrees, I reopen Build." } },
        {
          line: 3,
          event: {
            t: "2026-11-24T14:30:00Z",
            kind: "message",
            from: "user",
            stage: "build",
            id: "u-1",
            text: "Yes, reopen Build and add the flag.",
            replyTo: "s-1",
          },
        },
      ],
      state: null,
      turn: null,
    });
    const { accessToken } = await signIn(t.base);
    await eventually(async () => (await h.mirror.list({ pendingForCoordinator: true })).length === 1);
    const all = json(await callTool(t.base, accessToken, "read_messages", {}));
    const user = all.messages.find((m: any) => m.id === "u-1");
    assert.equal(user.from, "user");
    assert.equal(user.replyTo, "s-1");
    assert.equal(user.text, "Yes, reopen Build and add the flag.");
    assert.equal(all.messages.find((m: any) => m.id === "s-1").from, "session");

    const one = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "local" }));
    assert.deepEqual(one.messages.map((m: any) => m.from), ["session", "user"]);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R4.S1, R4.S2, R4.S5: read_messages gives the status change and the turn, and stale when the spec changed later", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    const spec = specAddress(REPO, "truck-load");
    // R4.S5: only message lines: no status change, the turn shows, no stale.
    await h.mirror.sync(spec, {
      lines: [
        { line: 1, event: { t: "2026-11-24T14:00:00Z", kind: "note", text: "start" } },
        { line: 2, event: { t: "2026-11-24T14:27:00Z", kind: "message", stage: "merge", id: "s-1", text: "If Ross agrees, I reopen Build." } },
      ],
      first: true,
      state: showState("truck-load", "merge", "awaiting-approval", "Ready."),
      turn: { turn: "yours", needsYou: true },
    });
    const { accessToken } = await signIn(t.base);
    const quiet = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "truck-load" }));
    assert.equal(quiet.changed, null);
    assert.deepEqual(quiet.turn, { turn: "yours", needsYou: true });
    assert.equal(quiet.stale, undefined);

    // R4.S1: the spec went back to Build and then to Merge after the message.
    await h.mirror.sync(spec, {
      lines: [
        { line: 3, event: { t: "2026-11-24T14:40:00Z", kind: "went-back", from: "merge", to: "build", stage: "build" } },
        { line: 4, event: { t: "2026-11-24T16:00:00Z", kind: "review-finished", text: "self round 6" } },
        { line: 5, event: { t: "2026-11-24T17:11:00Z", kind: "status", from: "working", to: "awaiting-approval", stage: "merge", text: "Merge when you are ready." } },
      ],
      state: showState("truck-load", "merge", "awaiting-approval", "Merge when you are ready."),
      turn: { turn: "yours", needsYou: true },
    });
    const stale = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "truck-load", after: 0 }));
    assert.deepEqual(stale.changed, {
      stage: "merge",
      status: "awaiting-approval",
      note: "Merge when you are ready.",
      at: "2026-11-24T17:11:00Z",
    });
    assert.deepEqual(stale.turn, { turn: "yours", needsYou: true });
    assert.equal(stale.stale, "The spec changed after the last message. Use read_spec for the current state.");

    // R4.S2: a newer message makes the thread current again.
    await h.mirror.sync(spec, {
      lines: [{ line: 6, event: { t: "2026-11-24T17:12:00Z", kind: "message", stage: "merge", id: "s-2", text: "Merged the flag." } }],
      state: null,
      turn: null,
    });
    const current = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "truck-load", after: 0 }));
    assert.equal(current.changed.at, "2026-11-24T17:11:00Z");
    assert.equal(current.stale, undefined);

    // The session answers and changes the status in the same second: stale.
    await h.mirror.sync(spec, {
      lines: [
        { line: 7, event: { t: "2026-11-24T17:20:00Z", kind: "message", stage: "merge", id: "s-3", text: "Done." } },
        { line: 8, event: { t: "2026-11-24T17:20:00Z", kind: "status", from: "awaiting-approval", to: "working", stage: "merge", text: "Merging." } },
      ],
      state: null,
      turn: null,
    });
    assert.ok(json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "truck-load", after: 0 })).stale);

    // A message from the coordinator after the change makes the thread current.
    await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "truck-load", text: "Thanks, I see it." });
    const answered = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "truck-load", after: 0 }));
    assert.equal(answered.stale, undefined);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R4.S3: without a slug, each spec has its status change, and only the spec that changed is stale", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    const lines = (stale: boolean) => [
      { line: 1, event: { t: "2026-11-24T10:00:00Z", kind: "status", from: "draft", to: "working", stage: "build" } },
      { line: 2, event: { t: "2026-11-24T11:00:00Z", kind: "message", stage: "build", text: "A question." } },
      ...(stale ? [{ line: 3, event: { t: "2026-11-24T12:00:00Z", kind: "approved", stage: "build" } }] : []),
    ];
    for (const [slug, stale] of [["moved", true], ["still", false]] as const) {
      await h.mirror.sync(specAddress(REPO, slug), {
        lines: lines(stale),
        first: true,
        state: showState(slug, stale ? "check" : "build", "working", "Working."),
        turn: null,
      });
    }
    await eventually(async () => (await h.mirror.list({ pendingForCoordinator: true, repoKey: repoKeyOf(REPO) })).length === 2);
    const { accessToken } = await signIn(t.base);
    const all = json(await callTool(t.base, accessToken, "read_messages", {}));
    const bySlug = Object.fromEntries(all.specs.map((s: any) => [s.slug, s]));
    assert.equal(bySlug.moved.changed.at, "2026-11-24T12:00:00Z");
    assert.equal(bySlug.moved.changed.stage, "check");
    assert.ok(bySlug.moved.stale);
    assert.equal(bySlug.still.changed.at, "2026-11-24T10:00:00Z");
    assert.equal(bySlug.still.stale, undefined);
    assert.deepEqual(bySlug.still.turn, { turn: "working", needsYou: false });
  } finally {
    await t.close();
    await h.close();
  }
});

test("R7.S1: send_message to a paused spec stores the message and says that the spec is paused", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.mirror.sync(specAddress(REPO, "held"), {
      line: 1,
      event: { kind: "note" },
      state: {
        slug: "held",
        title: "held",
        stage: "build",
        status: "working",
        paused: { at: "2026-11-01T10:00:00Z", by: "ross", note: "Waiting for the API" },
      },
      turn: null,
    });
    await synced(h, "free");
    await h.mirror.sync(specAddress(REPO, "nonote"), {
      line: 1,
      event: { kind: "note" },
      state: { slug: "nonote", title: "nonote", stage: "build", status: "working", paused: { at: "2026-11-01T10:00:00Z", by: "ross" } },
      turn: null,
    });
    const { accessToken } = await signIn(t.base);

    const sent = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "held", text: "Hello." });
    assert.equal(sent.isError, undefined, sent.content[0].text);
    const [first, ...rest] = sent.content[0].text.split("\n");
    const body = JSON.parse(first);
    assert.equal(body.seq, 1);
    assert.equal(body.paused, true);
    assert.equal(rest.join("\n"), "The spec is paused: Waiting for the API");
    // The message is in the inbox.
    const read = await callTool(t.base, accessToken, "read_spec", { repo: REPO, slug: "held" });
    assert.match(read.content[0].text, /1 message waits for the Mac/);

    const noNote = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "nonote", text: "Hello." });
    assert.match(noNote.content[0].text, /\nThe spec is paused\.$/);

    // A spec with no paused field: the answer is the JSON alone, as before.
    const free = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "free", text: "Hello." });
    assert.ok(!free.content[0].text.includes("\n"));
    assert.equal(json(free).paused, undefined);
    assert.equal(json(free).seq, 1);
  } finally {
    await t.close();
    await h.close();
  }
});

test("read_messages of a paused spec with no turn from the app gives the turn paused, which needs no user", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.mirror.sync(specAddress(REPO, "held"), {
      line: 1,
      event: { t: "2026-11-24T14:27:00Z", kind: "message", stage: "build", id: "s-1", text: "Paused for now." },
      state: { slug: "held", title: "held", stage: "build", status: "awaiting-approval", paused: { at: "2026-11-24T14:00:00Z", by: "ross" } },
      turn: null,
    });
    const { accessToken } = await signIn(t.base);
    const read = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "held" }));
    assert.deepEqual(read.turn, { turn: "paused", needsYou: false });
  } finally {
    await t.close();
    await h.close();
  }
});

test("send_message to a spec of an archived repo says that the repo is archived, and the message waits in the inbox", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    await h.mirror.sync(specAddress(REPO, "held"), {
      line: 1,
      event: { kind: "note" },
      state: { slug: "held", title: "held", stage: "build", status: "working", paused: { at: "2026-11-01T10:00:00Z", by: "ross", note: "Later" } },
      turn: null,
    });
    await synced(h, "plain");
    await h.configStore.setArchivedRepos([REPO], new Date());
    const { accessToken } = await signIn(t.base);

    const plain = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "plain", text: "Hello." });
    assert.equal(plain.isError, undefined, plain.content[0].text);
    const [first, ...rest] = plain.content[0].text.split("\n");
    assert.equal(JSON.parse(first).archived, true);
    assert.equal(JSON.parse(first).paused, undefined);
    assert.deepEqual(rest, ["The repo is archived."]);
    const read = await callTool(t.base, accessToken, "read_spec", { repo: REPO, slug: "plain" });
    assert.match(read.content[0].text, /1 message waits for the Mac/);

    // A paused spec of an archived repo: both lines.
    const held = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "held", text: "Hello." });
    const [hfirst, ...hrest] = held.content[0].text.split("\n");
    assert.equal(JSON.parse(hfirst).paused, true);
    assert.equal(JSON.parse(hfirst).archived, true);
    assert.deepEqual(hrest, ["The spec is paused: Later", "The repo is archived."]);

    // The repo is unarchived: no archived line.
    await h.configStore.setArchivedRepos([], new Date());
    const after = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "plain", text: "Again." });
    assert.ok(!after.content[0].text.includes("\n"));
    assert.equal(json(after).archived, undefined);
  } finally {
    await t.close();
    await h.close();
  }
});
