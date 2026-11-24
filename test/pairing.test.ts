import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { harness, startTestServer } from "./temporal.js";
import { AGENT_SECRET, callTool, connectAgent, rejectedStatus, signIn, sleep } from "./helpers.js";
import { makePairCode } from "../src/startup.js";
import { RELAY_VERSION } from "../src/version.js";
import { PairLimit } from "../src/server.js";
import { relayConfigWorkflowId } from "../src/workflows/types.js";

let env: TestWorkflowEnvironment;
before(async () => {
  env = await startTestServer();
});
after(async () => {
  await env?.teardown();
});

const CODE = "ABCD-EFGH-JKLM";

function pair(base: string, code: string, ip?: string) {
  return fetch(`${base}/agent/pair`, {
    method: "POST",
    headers: { "content-type": "application/json", ...(ip ? { "x-forwarded-for": ip } : {}) },
    body: JSON.stringify({ code }),
  });
}

test("R9.S1, R9.S2: a pairing code mints a secret once; the agent connects with it; a used code fails", async () => {
  const h = await harness(env);
  const t = await h.relay({ legacySecretSha256: null });
  try {
    await h.startConfig(CODE);
    const res = await pair(t.base, "abcd efgh jklm");
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.match(body.secret, /^[A-Za-z0-9_-]{43}$/);
    assert.equal(body.relayVersion, RELAY_VERSION);
    assert.equal(body.mcpURL, `${t.base}/mcp`);

    const agent = await connectAgent(t.base, body.secret, { type: "hello", version: 2, repos: [] });
    await agent.next("welcome");

    const again = await pair(t.base, CODE);
    assert.equal(again.status, 403);
    assert.deepEqual(await again.json(), { error: "The relay refused the pairing code." });
    const wrong = await pair(t.base, "WXYZ-WXYZ-WXYZ");
    assert.equal(wrong.status, 403);

    assert.ok(!JSON.stringify(t.logs).includes(body.secret), "the secret is not in the log");
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("wrong pairing codes are limited per source IP, and a correct code always passes", async () => {
  const h = await harness(env);
  const t = await h.relay({ legacySecretSha256: null });
  try {
    await h.startConfig(CODE);
    const stranger = "203.0.113.7";
    const statuses: number[] = [];
    for (let i = 0; i < 12; i++) statuses.push((await pair(t.base, `WRNG-WRNG-WR${i}`, stranger)).status);
    assert.deepEqual(statuses, [...Array(10).fill(403), 429, 429]);
    // Another source is not limited.
    assert.equal((await pair(t.base, "WRNG-WRNG-WRNG", "198.51.100.2")).status, 403);
    // The owner behind the same IP still pairs with the correct code.
    const ok = await pair(t.base, CODE, stranger);
    assert.equal(ok.status, 200);
    assert.match((await ok.json()).secret, /^[A-Za-z0-9_-]{43}$/);
    // The used code is now wrong, and the limit holds for it.
    assert.equal((await pair(t.base, CODE, stranger)).status, 429);
  } finally {
    await t.close();
    await h.close();
  }
});

test("PairLimit: a ceiling for all sources, and the window ends", () => {
  let now = 0;
  const limit = new PairLimit({ perSource: 3, total: 5, windowMs: 1000, maxSources: 100 }, () => now);
  for (let i = 0; i < 3; i++) limit.fail("a");
  assert.equal(limit.limited("a"), true);
  assert.equal(limit.limited("b"), false);
  limit.fail("b");
  limit.fail("c");
  assert.equal(limit.limited("d"), true, "the ceiling for all sources");
  now = 1001;
  assert.equal(limit.limited("a"), false);
  assert.equal(limit.limited("d"), false);
});

test("relay pair-code: a new code works and the old code stops", async () => {
  const h = await harness(env);
  const t = await h.relay({ legacySecretSha256: null });
  try {
    await h.startConfig(CODE);
    const code = await makePairCode(h.clients.config, h.deployId);
    assert.notEqual(code, CODE);
    assert.equal((await pair(t.base, CODE)).status, 403);
    assert.equal((await pair(t.base, code)).status, 200);
  } finally {
    await t.close();
    await h.close();
  }
});

test("R9.S3: after unpair the relay refuses the old secret with 401 and closes its connection", async () => {
  const h = await harness(env);
  const t = await h.relay({ legacySecretSha256: null });
  try {
    await h.startConfig(CODE);
    const { secret } = await (await pair(t.base, CODE)).json();
    const agent = await connectAgent(t.base, secret);
    const closed = new Promise<number>((resolve) => agent.ws.once("close", (code) => resolve(code)));

    const unpair = (s: string) => fetch(`${t.base}/agent/unpair`, { method: "POST", headers: { authorization: `Bearer ${s}` } });
    assert.equal((await unpair(secret)).status, 204);
    assert.equal(await closed, 4001);
    assert.equal(await rejectedStatus(t.base, secret), 401);
    assert.equal((await unpair(secret)).status, 401);
    assert.equal((await fetch(`${t.base}/agent/unpair`, { method: "POST" })).status, 401);
  } finally {
    await t.close();
    await h.close();
  }
});

test("unpair with a bearer that is not paired adds no update to the relay-config history", async () => {
  const h = await harness(env);
  const t = await h.relay({ legacySecretSha256: null });
  try {
    await h.startConfig(CODE);
    const { secret } = await (await pair(t.base, CODE)).json();
    const handle = h.clients.config.workflow.getHandle(relayConfigWorkflowId(h.deployId));
    const events = async () => (await handle.fetchHistory()).events?.length ?? 0;
    const before = await events();
    for (let i = 0; i < 5; i++) {
      const res = await fetch(`${t.base}/agent/unpair`, { method: "POST", headers: { authorization: `Bearer stranger-${i}` } });
      assert.equal(res.status, 401);
    }
    assert.equal(await events(), before);
    // The workflow's validator also refuses an unpair that removes nothing.
    assert.equal(await h.configStore.unpair("0".repeat(64)), false);
    assert.equal(await events(), before);
    // The paired secret still unpairs.
    const res = await fetch(`${t.base}/agent/unpair`, { method: "POST", headers: { authorization: `Bearer ${secret}` } });
    assert.equal(res.status, 204);
    assert.equal(await h.configStore.paired(), 0);
  } finally {
    await t.close();
    await h.close();
  }
});

test("the legacy AGENT_SECRET_SHA256 still connects beside paired secrets", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await h.startConfig();
    const agent = await connectAgent(t.base, AGENT_SECRET);
    await sleep(50);
    assert.equal(t.relay.hub.connected, true);
    await agent.close();
    assert.equal(await rejectedStatus(t.base, "not-a-secret"), 401);
  } finally {
    await t.close();
    await h.close();
  }
});

test("GET /agent/info gives the relay version, the wire version and the MCP URL", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    const info = await (await fetch(`${t.base}/agent/info`)).json();
    assert.deepEqual(info, { relayVersion: RELAY_VERSION, wireVersion: 2, mcpURL: `${t.base}/mcp` });
  } finally {
    await t.close();
    await h.close();
  }
});

test("R9.S4: a token from one relay instance works on a second instance with the same Temporal", async () => {
  const h = await harness(env);
  const first = await h.relay();
  const second = await h.relay();
  try {
    await h.startConfig();
    const { accessToken, refreshToken, clientId } = await signIn(first.base);
    await first.close(); // The relay restarts: a new instance with no state of its own.

    const agent = await connectAgent(second.base);
    agent.answer = () => ({ ok: true, text: "from the Mac" });
    const result = await callTool(second.base, accessToken, "list_repos");
    assert.equal(result.content[0].text, "from the Mac");

    const refresh = () =>
      fetch(`${second.base}/token`, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ grant_type: "refresh_token", refresh_token: refreshToken, client_id: clientId }),
      });
    const refreshed = await refresh();
    assert.equal(refreshed.status, 200);
    const tokens = await refreshed.json();
    assert.equal((await callTool(second.base, tokens.access_token, "list_repos")).content[0].text, "from the Mac");
    assert.equal((await refresh()).status, 400, "a refresh token works once");
    await agent.close();
  } finally {
    await second.close();
    await h.close();
  }
});
