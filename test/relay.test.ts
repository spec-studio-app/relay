import { test } from "node:test";
import assert from "node:assert/strict";
import {
  AGENT_SECRET,
  PASSWORD,
  callTool,
  connectAgent,
  mcpRequest,
  openLogin,
  postLogin,
  register,
  rejectedStatus,
  signIn,
  sleep,
  startRelay,
} from "./helpers.js";
import { LOCAL_TOOLS, TOOLS } from "../src/tools.js";
import { redirectAllowed } from "../src/oauth.js";
import type { AgentBackend } from "../src/backend.js";
import { connect, type Socket } from "node:net";

test("R5.S1: an MCP call without a token gets 401 and nothing goes to the agent", async () => {
  const t = await startRelay();
  try {
    const agent = await connectAgent(t.base);
    agent.answer = () => ({ ok: true, text: "should not happen" });

    const res = await fetch(`${t.base}/mcp`, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json, text/event-stream" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "tools/call",
        params: { name: "list_specs", arguments: {} },
      }),
    });
    assert.equal(res.status, 401);
    assert.match(res.headers.get("www-authenticate") ?? "", /Bearer/);
    assert.match(res.headers.get("www-authenticate") ?? "", /oauth-protected-resource/);

    const badToken = await mcpRequest(t.base, "not-a-token", "tools/call", { name: "list_specs", arguments: {} });
    assert.equal(badToken.status, 401);

    const get = await fetch(`${t.base}/mcp`, { headers: { accept: "text/event-stream" } });
    assert.equal(get.status, 401);

    await sleep(100);
    assert.equal(agent.received.length, 0);
    await agent.close();
  } finally {
    await t.close();
  }
});

test("R5.S2: a second agent with a wrong secret gets 401 and the first agent stays", async () => {
  const t = await startRelay();
  try {
    const first = await connectAgent(t.base);
    first.answer = () => ({ ok: true, text: "first agent" });

    assert.equal(await rejectedStatus(t.base, "wrong-secret"), 401);
    assert.equal(await rejectedStatus(t.base), 401);
    assert.equal(await rejectedStatus(t.base, ""), 401);

    await sleep(50);
    assert.equal(first.ws.readyState, first.ws.OPEN);
    assert.equal(t.relay.hub.connected, true);

    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "list_repos");
    assert.equal(result.isError, undefined);
    assert.equal(result.content[0].text, "first agent");
    await first.close();
  } finally {
    await t.close();
  }
});

test("a new valid agent connection replaces the old one", async () => {
  const t = await startRelay();
  try {
    const first = await connectAgent(t.base);
    const closed = new Promise<number>((resolve) => first.ws.once("close", (code) => resolve(code)));
    const second = await connectAgent(t.base);
    second.answer = () => ({ ok: true, text: "second agent" });
    assert.equal(await closed, 4000);

    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "list_repos");
    assert.equal(result.content[0].text, "second agent");
    await second.close();
  } finally {
    await t.close();
  }
});

test("R6.S1: with no agent a call returns the offline error at once, with the last-seen time", async () => {
  let clock = new Date("2026-11-22T10:00:00.000Z");
  const t = await startRelay({ now: () => clock });
  try {
    const { accessToken } = await signIn(t.base);

    let started = Date.now();
    let result = await callTool(t.base, accessToken, "list_specs", {});
    assert.ok(Date.now() - started < 5000);
    assert.equal(result.isError, true);
    assert.equal(
      result.content[0].text,
      "The Mac is offline. The relay did not see the Mac since it started at 2026-11-22T10:00:00.000Z.",
    );

    clock = new Date("2026-11-22T11:30:00.000Z");
    const agent = await connectAgent(t.base);
    await sleep(50);
    clock = new Date("2026-11-22T12:00:00.000Z");
    await agent.close();
    await sleep(50);

    started = Date.now();
    result = await callTool(t.base, accessToken, "list_specs", {});
    assert.ok(Date.now() - started < 5000);
    assert.equal(result.isError, true);
    assert.equal(result.content[0].text, "The Mac is offline. The relay last saw it at 2026-11-22T11:30:00.000Z.");
  } finally {
    await t.close();
  }
});

test("R6.S2: an agent that never answers gives 'did not answer in time'", async () => {
  const t = await startRelay({ callTimeoutMs: 200 });
  try {
    const agent = await connectAgent(t.base);
    agent.answer = () => undefined;
    const { accessToken } = await signIn(t.base);
    const started = Date.now();
    const result = await callTool(t.base, accessToken, "start_spec", { repo: "/r", slug: "s" });
    assert.ok(Date.now() - started < 2000);
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /^The Mac did not answer in time \(0\.2 seconds\)\.$/);
    assert.equal(agent.received.length, 1);
    await agent.close();
  } finally {
    await t.close();
  }
});

test("an agent that disconnects in a call fails the call with the offline text", async () => {
  const t = await startRelay({ callTimeoutMs: 5000 });
  try {
    const agent = await connectAgent(t.base);
    agent.answer = () => {
      agent.ws.close();
      return undefined;
    };
    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "list_specs", {});
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /^The Mac is offline\. The relay last saw it at /);
  } finally {
    await t.close();
  }
});

test("R1-R4 happy path: a real OAuth flow, then a tool call routed to the agent", async () => {
  const t = await startRelay();
  try {
    const meta = await (await fetch(`${t.base}/.well-known/oauth-authorization-server`)).json();
    assert.equal(meta.issuer, `${t.base}/`);
    assert.ok(meta.registration_endpoint);
    assert.deepEqual(meta.code_challenge_methods_supported, ["S256"]);
    const resource = await (await fetch(`${t.base}/.well-known/oauth-protected-resource/mcp`)).json();
    assert.equal(resource.resource, `${t.base}/mcp`);

    const agent = await connectAgent(t.base);
    agent.answer = (call) => ({ ok: true, text: `made ${String(call.arguments.title)}` });

    const { accessToken } = await signIn(t.base);

    const list = await mcpRequest(t.base, accessToken, "tools/list");
    assert.deepEqual(
      list.message?.result.tools.map((tool: { name: string }) => tool.name),
      ["list_repos", "list_specs", "read_spec", "make_spec", "start_spec"],
    );

    const args = { repo: "/Users/me/code/app", title: "Dark mode", brief: "Add a dark mode.", start: true };
    const result = await callTool(t.base, accessToken, "make_spec", args);
    assert.equal(result.isError, undefined);
    assert.equal(result.content[0].text, "made Dark mode");

    assert.equal(agent.received.length, 1);
    const call = agent.received[0];
    assert.equal(call.type, "call");
    assert.match(call.id, /^[0-9a-f-]{36}$/);
    assert.equal(call.tool, "make_spec");
    // The relay passes the arguments as given: no defaults added.
    assert.deepEqual(call.arguments, args);

    agent.answer = () => ({ ok: false, text: "No spec has the slug nothing-here in this repo." });
    const failed = await callTool(t.base, accessToken, "read_spec", { repo: "/r", slug: "nothing-here" });
    assert.equal(failed.isError, true);
    assert.equal(failed.content[0].text, "No spec has the slug nothing-here in this repo.");

    await agent.close();
  } finally {
    await t.close();
  }
});

test("a wrong password does not sign in", async () => {
  const t = await startRelay();
  try {
    await assert.rejects(signIn(t.base, "wrong password"), /login: 401/);
  } finally {
    await t.close();
  }
});

test("a refresh token gives a new access token and works only once", async () => {
  const t = await startRelay();
  try {
    const { refreshToken, clientId } = await signIn(t.base);
    const refresh = () =>
      fetch(`${t.base}/token`, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ grant_type: "refresh_token", refresh_token: refreshToken, client_id: clientId }),
      });
    const first = await refresh();
    assert.equal(first.status, 200);
    const tokens = await first.json();
    assert.ok(tokens.access_token);
    const listed = await mcpRequest(t.base, tokens.access_token, "tools/list");
    assert.equal(listed.status, 200);
    assert.equal((await refresh()).status, 400);
  } finally {
    await t.close();
  }
});

test("R5.S3: the logs hold no agent secret, password, code or token", async () => {
  const t = await startRelay();
  try {
    const agent = await connectAgent(t.base);
    agent.answer = () => ({ ok: true, text: "ok" });
    await rejectedStatus(t.base, "wrong-secret");
    await signIn(t.base, "wrong password").catch(() => {});
    const { accessToken, refreshToken, code } = await signIn(t.base);
    await callTool(t.base, accessToken, "list_specs", { all: true });
    await agent.close();
    await sleep(50);

    const text = JSON.stringify(t.logs);
    assert.ok(t.logs.some((entry) => entry.tool === "list_specs"), "the tool call is in the logs");
    for (const secret of [AGENT_SECRET, PASSWORD, "wrong password", "wrong-secret", accessToken, refreshToken, code]) {
      assert.ok(!text.includes(secret), "a secret is in the logs");
    }
    assert.ok(!/authorization/i.test(text), "a header is in the logs");
  } finally {
    await t.close();
  }
});

test("R7.S1: each tool description is in short STE sentences with the Spec Studio terms", () => {
  const texts: string[] = [];
  for (const tool of [...TOOLS, ...LOCAL_TOOLS]) {
    texts.push(tool.description);
    for (const schema of Object.values(tool.inputSchema)) {
      if (schema.description) texts.push(schema.description);
    }
  }
  for (const text of texts) {
    const sentences = text
      .split(/(?<=[.!?])\s+/)
      .map((s) => s.trim())
      .filter(Boolean);
    for (const sentence of sentences) {
      const words = sentence.split(/\s+/).length;
      assert.ok(words <= 25, `More than 25 words: "${sentence}"`);
      assert.ok(/[.!?]$/.test(sentence), `A sentence must end with a stop: "${sentence}"`);
    }
    // STE: no -ing forms where an alternative is possible.
    const ing = text.match(/\b\w+ing\b/g)?.filter((w) => !["string"].includes(w.toLowerCase()));
    assert.deepEqual(ing ?? [], [], `An -ing word in: "${text}"`);
  }
  const all = texts.join(" ").toLowerCase();
  for (const term of ["spec", "stage", "status", "turn"]) {
    assert.match(all, new RegExp(`\\b${term}\\b`), `The descriptions do not mention "${term}"`);
  }
});

test("GET /healthz answers 200", async () => {
  const t = await startRelay();
  try {
    await connectAgent(t.base);
    const res = await fetch(`${t.base}/healthz`);
    assert.equal(res.status, 200);
    // Public: it does not say whether the Mac is online.
    assert.deepEqual(await res.json(), { ok: true });
  } finally {
    await t.close();
  }
});

test("the relay pings the agent and a pong updates the last-seen time", async () => {
  let clock = new Date("2026-11-22T10:00:00.000Z");
  const t = await startRelay({ now: () => clock, pingIntervalMs: 30 });
  const until = async (check: () => boolean, what: string) => {
    const end = Date.now() + 5000;
    while (!check()) {
      if (Date.now() > end) throw new Error(`${what} did not happen in time.`);
      await sleep(5);
    }
  };
  try {
    const agent = await connectAgent(t.base);
    await until(() => t.relay.hub.connected, "the connection");
    clock = new Date("2026-11-22T10:05:00.000Z");
    // Wait for a ping after the clock moved; the agent's pong updates the last-seen time.
    await new Promise<void>((resolve) => agent.ws.once("ping", () => resolve()));
    await until(() => t.relay.hub.offlineText().includes("10:05:00.000Z"), "the pong");
    agent.ws.terminate();
    await until(() => !t.relay.hub.connected, "the disconnect");
    // After the disconnect, time goes on but the last-seen time stays.
    clock = new Date("2026-11-22T10:10:00.000Z");
    assert.equal(t.relay.hub.offlineText(), "The Mac is offline. The relay last saw it at 2026-11-22T10:05:00.000Z.");
  } finally {
    await t.close();
  }
});

test("R6.S1: a call to a Mac that sleeps (the socket stays open but is silent) answers offline in 5 seconds or less", async () => {
  const t = await startRelay({ callTimeoutMs: 30_000, probeTimeoutMs: 1000 });
  try {
    const agent = await connectAgent(t.base);
    agent.answer = () => ({ ok: true, text: "answer" });
    const { accessToken } = await signIn(t.base);
    await sleep(1100); // The agent is quiet for longer than the probe time.
    agent.ws.pause(); // The Mac sleeps: the socket stays open, but nothing answers.
    const start = Date.now();
    const result = await callTool(t.base, accessToken, "list_specs", {});
    const ms = Date.now() - start;
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /^The Mac is offline\. The relay last saw it at /);
    assert.ok(ms < 5000, `answered in ${ms} ms`);
    assert.equal(agent.received.length, 0, "the call did not go to the sleeping Mac");
    assert.equal(t.relay.hub.connected, false);
  } finally {
    await t.close();
  }
});

test("a call to a quiet but awake Mac still goes through after the probe", async () => {
  const t = await startRelay({ probeTimeoutMs: 300 });
  try {
    const agent = await connectAgent(t.base);
    agent.answer = () => ({ ok: true, text: "answer" });
    const { accessToken } = await signIn(t.base);
    await sleep(400);
    const result = await callTool(t.base, accessToken, "list_specs", {});
    assert.equal(result.isError, undefined);
    assert.equal(result.content[0].text, "answer");
  } finally {
    await t.close();
  }
});

test("a client may register only the claude.ai or claude.com callback or a loopback redirect", async () => {
  const t = await startRelay();
  try {
    assert.equal(await register(t.base, ["https://attacker.example/cb"]), 400);
    assert.equal(await register(t.base, ["http://127.0.0.1:9/callback", "https://attacker.example/cb"]), 400);
    assert.equal(await register(t.base, ["https://claude.ai/api/mcp/auth_callback"]), 201);
    assert.equal(await register(t.base, ["http://localhost:54321/callback"]), 201);
  } finally {
    await t.close();
  }
  assert.equal(redirectAllowed("https://claude.ai.attacker.example/api/mcp/auth_callback"), false);
  assert.equal(redirectAllowed("https://claude.ai/other"), false);
  assert.equal(redirectAllowed("http://claude.ai/api/mcp/auth_callback"), false);
  assert.equal(redirectAllowed("http://user@127.0.0.1/cb"), false);
  assert.equal(redirectAllowed("https://my.example/cb", ["https://my.example/cb"]), true);
});

test("the login page shows where the code goes", async () => {
  const t = await startRelay();
  try {
    const { html } = await openLogin(t.base);
    assert.match(html, /sends you back to <strong>127\.0\.0\.1:9<\/strong>/);
  } finally {
    await t.close();
  }
});

test("wrong passwords sent at the same time all count toward the limit", async () => {
  const t = await startRelay();
  try {
    const { request } = await openLogin(t.base);
    const answers = await Promise.all(Array.from({ length: 15 }, () => postLogin(t.base, request, "wrong")));
    const statuses = answers.map((res) => res.status);
    assert.equal(statuses.filter((s) => s === 401).length, 10);
    assert.equal(statuses.filter((s) => s === 429).length, 5);
    // The limit holds for this source, also for the correct password.
    assert.equal((await postLogin(t.base, request, PASSWORD)).status, 429);
  } finally {
    await t.close();
  }
});

test("registrations push out only clients that never signed in", async () => {
  const { SingleUserProvider } = await import("../src/oauth.js");
  const provider = new SingleUserProvider({ loginPasswordBcrypt: "x", now: () => new Date(), log: () => {} });
  const store = provider.clientsStore;
  const meta = { redirect_uris: ["http://127.0.0.1:9/callback"] } as Parameters<typeof store.registerClient>[0];
  const user = await store.registerClient(meta);
  await store.markSignedIn(user.client_id);
  for (let i = 0; i < 150; i++) await store.registerClient(meta);
  assert.ok(await store.getClient(user.client_id), "the signed-in client stays");
});

test("a stranger who blocks the shared login limit cannot lock out the user's known browser", async () => {
  const t = await startRelay();
  try {
    // The user signed in once in this browser: the relay set a device cookie.
    const first = await openLogin(t.base);
    const ok = await postLogin(t.base, first.request, PASSWORD);
    assert.equal(ok.status, 302);
    const cookie = (ok.headers.get("set-cookie") ?? "").split(";")[0];
    assert.match(cookie, /^relay_device=[\w-]+\.[\w-]+$/);

    // A stranger uses up the shared limit.
    const { request } = await openLogin(t.base);
    await Promise.all(Array.from({ length: 12 }, () => postLogin(t.base, request, "wrong")));
    assert.equal((await postLogin(t.base, request, PASSWORD)).status, 429);
    // A forged cookie counts as a stranger.
    const forged = await postLogin(t.base, request, PASSWORD, "relay_device=abc.def");
    assert.equal(forged.status, 429);

    // The user's browser still signs in.
    const again = await openLogin(t.base);
    assert.equal((await postLogin(t.base, again.request, PASSWORD, cookie)).status, 302);
  } finally {
    await t.close();
  }
});

test("a call made while the woken Mac connects again goes to the new connection", async () => {
  const t = await startRelay({ callTimeoutMs: 10_000, probeTimeoutMs: 1000 });
  try {
    const old = await connectAgent(t.base);
    const { accessToken } = await signIn(t.base);
    await sleep(1100);
    old.ws.pause(); // The Mac sleeps.
    const call = callTool(t.base, accessToken, "list_specs", {});
    await sleep(200);
    const woken = await connectAgent(t.base); // The Mac wakes during the probe.
    woken.answer = () => ({ ok: true, text: "answer from the new connection" });
    const result = await call;
    assert.equal(result.isError, undefined, result.content[0].text);
    assert.equal(result.content[0].text, "answer from the new connection");
  } finally {
    await t.close();
  }
});

/** A backend whose secret check never answers (Temporal is slow or down). */
function hangingBackend(): AgentBackend {
  const never = () => new Promise<never>(() => {});
  return {
    verifyAgent: never,
    hello: never,
    sync: never,
    gone: never,
    state: never,
    ack: never,
    pair: never,
    pairCodeValid: never,
    unpair: never,
    uploadUrl: never,
    downloadUrl: never,
    pendingForMac: never,
  };
}

function rawUpgrade(base: string): Promise<Socket> {
  const { port } = new URL(base);
  return new Promise((resolve, reject) => {
    const socket = connect(Number(port), "127.0.0.1", () => {
      socket.write(
        "GET /agent HTTP/1.1\r\nHost: relay\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
          "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAuthorization: Bearer not-paired\r\n\r\n",
      );
      resolve(socket);
    });
    socket.on("error", reject);
  });
}

test("a reset of the agent socket during the secret check does not crash the relay", async () => {
  const t = await startRelay({ agentBackend: hangingBackend(), testAgentPaired: false });
  try {
    const socket = await rawUpgrade(t.base);
    await sleep(100);
    socket.resetAndDestroy();
    await sleep(200);
    // The relay is still up.
    assert.equal((await fetch(`${t.base}/healthz`)).status, 200);
  } finally {
    await t.close();
  }
});

test("the secret check of an agent upgrade ends with 503 when Temporal does not answer in time", async () => {
  const t = await startRelay({ agentBackend: hangingBackend(), testAgentPaired: false, agentAuthTimeoutMs: 300 });
  try {
    const socket = await rawUpgrade(t.base);
    const answer = await new Promise<string>((resolve) => {
      let text = "";
      socket.on("data", (d) => (text += d.toString()));
      socket.on("close", () => resolve(text));
    });
    assert.match(answer, /^HTTP\/1\.1 503/);
    assert.ok(t.logs.some((l) => l.event === "agent_rejected" && l.status === 503));
  } finally {
    await t.close();
  }
});

test("the codec sign-in uses the device cookie from /login, so a stranger cannot lock out the user's browser", async () => {
  const { newPayloadKeys } = await import("../src/codec.js");
  const { parsePayloadKeys } = await import("../src/config.js");
  const t = await startRelay({ codecDataKeys: parsePayloadKeys(newPayloadKeys()).data });
  const codecSignIn = (password: string, cookie?: string) =>
    fetch(`${t.base}/codec/signin`, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded", ...(cookie ? { cookie } : {}) },
      body: new URLSearchParams({ password }).toString(),
    });
  try {
    const first = await openLogin(t.base);
    const ok = await postLogin(t.base, first.request, PASSWORD);
    const setCookie = ok.headers.get("set-cookie") ?? "";
    assert.match(setCookie, /Path=\/(;|$)/, "the device cookie reaches /codec/signin too");
    const cookie = setCookie.split(";")[0];

    // A stranger uses up the shared limit on the codec sign-in.
    await Promise.all(Array.from({ length: 12 }, () => codecSignIn("wrong")));
    assert.equal((await codecSignIn(PASSWORD)).status, 429);
    // The user's browser still signs in.
    assert.equal((await codecSignIn(PASSWORD, cookie)).status, 200);
  } finally {
    await t.close();
  }
});

test("a codec sign-in from a new browser sets the device cookie", async () => {
  const { newPayloadKeys } = await import("../src/codec.js");
  const { parsePayloadKeys } = await import("../src/config.js");
  const t = await startRelay({ codecDataKeys: parsePayloadKeys(newPayloadKeys()).data });
  try {
    const res = await fetch(`${t.base}/codec/signin`, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ password: PASSWORD }).toString(),
    });
    assert.equal(res.status, 200);
    const cookies = res.headers.getSetCookie();
    assert.ok(cookies.some((c) => /^relay_device=[\w-]+\.[\w-]+;.*Path=\/(;|$)/.test(c)), cookies.join(" | "));
  } finally {
    await t.close();
  }
});

test("while a current agent is connected, the relay looks again for waiting messages that the welcome scan missed", async () => {
  let scans = 0;
  const spec = { repoKey: "0123456789ab", slug: "late", repo: "/Users/me/code/app" };
  const message = {
    id: "m1",
    seq: 1,
    from: "coordinator" as const,
    spec: { repoKey: spec.repoKey, slug: spec.slug },
    stage: "",
    text: "Sent while visibility lagged.",
    createdAt: "2026-11-23T12:00:00Z",
  };
  const backend: AgentBackend = {
    ...hangingBackend(),
    hello: async () => {},
    // The first scan (at the welcome) misses the message; later scans find it.
    pendingForMac: async () => (scans++ === 0 ? [] : [{ spec, message }]),
  };
  const t = await startRelay({ agentBackend: backend, pendingScanMs: 150 });
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 3, repos: [] });
    await agent.next("welcome");
    const pushed = await agent.next("message");
    assert.equal(pushed.message.text, "Sent while visibility lagged.");
    await sleep(400);
    assert.ok(scans >= 3);
    assert.equal(agent.frames.filter((f: any) => f.type === "message").length, 0, "a message goes once per connection");
    await agent.close();
  } finally {
    await t.close();
  }
});

test("a message of a new run with the same seq as a message pushed before on this connection is pushed too", async () => {
  const spec = { repoKey: "0123456789ab", slug: "again", repo: "/Users/me/code/app" };
  const message = (id: string, text: string) => ({
    id,
    seq: 1,
    from: "coordinator" as const,
    spec: { repoKey: spec.repoKey, slug: spec.slug },
    stage: "",
    text,
    createdAt: "2026-11-23T12:00:00Z",
  });
  const backend: AgentBackend = { ...hangingBackend(), hello: async () => {}, pendingForMac: async () => [] };
  const t = await startRelay({ agentBackend: backend });
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 3, repos: [] });
    await agent.next("welcome");
    assert.equal(t.relay.hub.pushMessage(spec, message("old-run-1", "Old run.")), true);
    assert.equal((await agent.next("message")).message.text, "Old run.");
    // The same message again: not pushed twice.
    t.relay.hub.pushMessage(spec, message("old-run-1", "Old run."));
    // A new run of the slug: seq 1 again, a new ID.
    t.relay.hub.pushMessage(spec, message("new-run-1", "New run."));
    const next = await agent.next("message");
    assert.equal(next.message.text, "New run.");
    await sleep(100);
    assert.equal(agent.frames.filter((f: any) => f.type === "message").length, 0);
    await agent.close();
  } finally {
    await t.close();
  }
});

test("a scan pushes again a message that still waits long after its push on the same connection", async () => {
  const spec = { repoKey: "0123456789ab", slug: "unacked", repo: "/Users/me/code/app" };
  const message = {
    id: "m-unacked",
    seq: 1,
    from: "coordinator" as const,
    spec: { repoKey: spec.repoKey, slug: spec.slug },
    stage: "",
    text: "The ack of this one failed.",
    createdAt: "2026-11-23T12:00:00Z",
  };
  // The message keeps waiting (its ack never lands).
  const backend: AgentBackend = { ...hangingBackend(), hello: async () => {}, pendingForMac: async () => [{ spec, message }] };
  const t = await startRelay({ agentBackend: backend, pendingScanMs: 50, resendAfterMs: 400 });
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 3, repos: [] });
    await agent.next("welcome");
    const start = Date.now();
    await agent.next("message");
    await sleep(250);
    assert.equal(agent.frames.filter((f: any) => f.type === "message").length, 0, "no push again before resendAfterMs");
    await agent.next("message", undefined, 3000);
    assert.ok(Date.now() - start >= 350);
    await agent.close();
  } finally {
    await t.close();
  }
});

test("a state frame that fails on a Temporal error is tried again until it applies; a newer frame or a disconnect stops it", async () => {
  const spec = { repoKey: "0123456789ab", slug: "flaky" };
  const calls: string[] = [];
  let failures = 2;
  const backend: AgentBackend = {
    ...hangingBackend(),
    hello: async () => {},
    pendingForMac: async () => [],
    sync: async (_spec, input) => {
      const last = input.lines?.at(-1)?.line ?? 0;
      calls.push(`sync ${last}`);
      return { line: last, applied: true };
    },
    state: async (_spec, state) => {
      calls.push(`state ${state.status}`);
      if (failures-- > 0) throw new Error("Temporal is down");
      return true;
    },
  };
  const t = await startRelay({ agentBackend: backend, stateRetryMs: 100 });
  try {
    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 3, repos: [] });
    await agent.next("welcome");
    agent.send({ type: "state", spec, state: { status: "blocked" }, turn: null });
    await sleep(600);
    // Two failures, then it applies, and no more tries.
    assert.deepEqual(calls, ["state blocked", "state blocked", "state blocked"]);

    // A newer sync frame stops the retry of an older failed state.
    calls.length = 0;
    failures = 100;
    agent.send({ type: "state", spec, state: { status: "working" }, turn: null });
    await sleep(30);
    agent.send({ type: "sync", spec, lines: [{ line: 1, event: { kind: "note" } }], state: { status: "idle" }, turn: null });
    await agent.next("synced");
    await sleep(400);
    assert.deepEqual(calls, ["state working", "sync 1"]);

    // A newer state frame replaces the failed one; a disconnect stops its retry.
    calls.length = 0;
    agent.send({ type: "state", spec, state: { status: "old" }, turn: null });
    await sleep(30);
    agent.send({ type: "state", spec, state: { status: "new" }, turn: null });
    await sleep(250);
    assert.ok(!calls.slice(2).includes("state old"), calls.join(", "));
    assert.ok(calls.filter((c) => c === "state new").length >= 2, calls.join(", "));
    await agent.close();
    await sleep(50);
    const after = calls.length;
    await sleep(400);
    assert.equal(calls.length, after, "no retry after the disconnect");
  } finally {
    await t.close();
  }
});

test("a repeated post of a sign-in that worked gets the same redirect, not an error", async () => {
  const t = await startRelay();
  try {
    const { request } = await openLogin(t.base);
    const first = await postLogin(t.base, request, PASSWORD);
    assert.equal(first.status, 302);
    const again = await postLogin(t.base, request, PASSWORD);
    assert.equal(again.status, 302);
    assert.equal(again.headers.get("location"), first.headers.get("location"));
  } finally {
    await t.close();
  }
});

test("two posts of one sign-in sent at once both get the one redirect", async () => {
  const t = await startRelay();
  try {
    const { request } = await openLogin(t.base);
    const [a, b] = await Promise.all([postLogin(t.base, request, PASSWORD), postLogin(t.base, request, PASSWORD)]);
    assert.deepEqual([a.status, b.status], [302, 302]);
    assert.equal(a.headers.get("location"), b.headers.get("location"));
  } finally {
    await t.close();
  }
});

test("the sign-in pages turn off the button after the first submit, with a nonce in the CSP", async () => {
  const { newPayloadKeys } = await import("../src/codec.js");
  const { parsePayloadKeys } = await import("../src/config.js");
  const t = await startRelay({ codecDataKeys: parsePayloadKeys(newPayloadKeys()).data });
  try {
    for (const res of [await fetchLoginPage(t.base), await fetch(`${t.base}/codec/signin`)]) {
      const csp = res.headers.get("content-security-policy") ?? "";
      const html = await res.text();
      const nonce = /script-src 'nonce-([^']+)'/.exec(csp)?.[1];
      assert.ok(nonce, `no script nonce in: ${csp}`);
      assert.match(csp, /default-src 'none'/);
      assert.ok(html.includes(`<script nonce="${nonce}">`));
      assert.match(html, /Signing in…/);
    }
  } finally {
    await t.close();
  }
});

async function fetchLoginPage(base: string): Promise<Response> {
  const { request } = await openLogin(base);
  // openLogin consumed the page; post a wrong password to get the page again with its headers.
  return postLogin(base, request, "wrong password");
}
