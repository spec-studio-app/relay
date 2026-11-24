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
  try {
    const agent = await connectAgent(t.base);
    await sleep(20);
    clock = new Date("2026-11-22T10:05:00.000Z");
    await sleep(120);
    clock = new Date("2026-11-22T10:10:00.000Z");
    agent.ws.terminate();
    await sleep(50);
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
