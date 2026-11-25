// A relay on the JSON store, end to end, with no Temporal (pluggable-relay-store
// R1.S1, R4.S5), and the check that only the Temporal store uses Temporal.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { openJsonStore } from "../src/store/json/index.js";
import { storeAgentBackend, storeOAuthStore } from "../src/store/adapters.js";
import { OfflineTools } from "../src/offline.js";
import { MessageTools } from "../src/messages.js";
import { Attachments } from "../src/attachments.js";
import { MemoryBucket } from "../src/bucket.js";
import { repoKeyOf } from "../src/store/address.js";
import { callTool, connectAgent, signIn, startRelay } from "./helpers.js";

const REPO = "/Users/me/code/app";
const root = fileURLToPath(new URL("../../", import.meta.url));

test("R1.S1: a relay on the JSON store pairs, syncs, reads offline and carries messages with no Temporal", async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-json-"));
  const bucket = new MemoryBucket();
  const store = openJsonStore({ dataDir: dir, deployId: "default", publicUrl: "https://relay.example.com" }, { bucket, log: () => {} });
  const printed: string[] = [];
  const { pairCode } = await store.start({ log: () => {}, print: (t) => printed.push(t) });
  assert.ok(pairCode);
  const attachments = new Attachments({ bucket, deployId: "default", maxBytes: 1024 * 1024 });
  const t = await startRelay({
    testAgentPaired: false,
    agentBackend: storeAgentBackend({ specs: store.specs, desks: store.desks, config: store.config, attachments }),
    offline: new OfflineTools({ specs: store.specs, config: store.config }),
    oauthStore: storeOAuthStore(store.config),
    localTools: (hub) =>
      new MessageTools({ specs: store.specs, attachments, push: (spec, message) => hub.pushMessage(spec, message), waitMs: 1000 }),
  });
  try {
    // The Mac pairs with the code from the first start.
    const res = await fetch(`${t.base}/agent/pair`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ code: pairCode }),
    });
    assert.equal(res.status, 200);
    const { secret } = (await res.json()) as { secret: string };

    // It syncs a spec with a message line for Claude AI.
    const agent = await connectAgent(t.base, secret, { type: "hello", version: 3, repos: [REPO] });
    await agent.next("welcome");
    const spec = { repoKey: repoKeyOf(REPO), slug: "dark-mode", repo: REPO };
    const state = { slug: "dark-mode", title: "Dark mode", stage: "plan", status: "working", statusNote: "Planning.", mode: "autopilot" };
    agent.send({
      type: "sync",
      spec,
      first: true,
      lines: [
        { line: 1, event: { kind: "status", to: "working" } },
        { line: 2, event: { kind: "message", id: "m-1", text: "The plan is ready.", stage: "plan" } },
      ],
      state,
      turn: { turn: "claude", needsYou: false },
    });
    assert.deepEqual(await agent.next("synced", (f) => f.line === 2), { type: "synced", spec: { repoKey: spec.repoKey, slug: spec.slug }, line: 2 });
    await agent.close();

    // Claude AI signs in; the Mac is offline, so the read tools answer from the store.
    const { accessToken } = await signIn(t.base);
    const list = await callTool(t.base, accessToken, "list_specs", {});
    assert.equal(list.isError, undefined, list.content[0].text);
    assert.match(list.content[0].text, /"slug": "dark-mode"/);
    assert.match(list.content[0].text, /"note": "Planning\."/);
    const read = await callTool(t.base, accessToken, "read_spec", { repo: REPO, slug: "dark-mode" });
    assert.match(read.content[0].text, /Stage: plan/);
    const repos = await callTool(t.base, accessToken, "list_repos", {});
    assert.match(repos.content[0].text, new RegExp(REPO));

    // It reads the session's message and sends one to the Mac.
    const messages = await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "dark-mode" });
    assert.match(messages.content[0].text, /The plan is ready\./);
    const sent = await callTool(t.base, accessToken, "send_message", { repo: REPO, slug: "dark-mode", text: "Use the old icon." });
    assert.equal(sent.isError, undefined, sent.content[0].text);

    // The Mac connects again: the relay pushes the message, and delivered removes it.
    const again = await connectAgent(t.base, secret, { type: "hello", version: 3, repos: [REPO] });
    const pushed = await again.next("message");
    assert.equal(pushed.message.text, "Use the old icon.");
    again.send({ type: "delivered", spec: { repoKey: spec.repoKey, slug: spec.slug }, seq: pushed.seq });
    for (let i = 0; i < 50 && (await store.specs.state(spec))?.mac.messages.length; i++) await new Promise((r) => setTimeout(r, 20));
    assert.equal((await store.specs.state(spec))?.mac.messages.length, 0);
    await again.close();

    // R4.S5: no codec endpoints without Temporal.
    assert.equal((await fetch(`${t.base}/codec/signin`)).status, 404);
    assert.equal((await fetch(`${t.base}/codec/decode`, { method: "POST" })).status, 404);
  } finally {
    await t.close();
    await store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

/** The modules that a module loads at start: static imports and re-exports, not `import type` or `import()`. */
function staticImports(file: string): string[] {
  const text = readFileSync(file, "utf8");
  const out: string[] = [];
  for (const m of text.matchAll(/^(?:import|export)\s+(type\s+)?[^;]*?from\s+"([^"]+)";/gms)) {
    if (m[1]) continue;
    out.push(m[2]);
  }
  for (const m of text.matchAll(/^import\s+"([^"]+)";/gm)) out.push(m[1]);
  return out;
}

/** Every module that the entry files load at start, as "<file> -> <module>" for the Temporal ones. */
function temporalLoads(entries: string[]): string[] {
  const src = join(root, "src");
  const seen = new Set<string>();
  const found: string[] = [];
  const queue = entries.map((e) => join(src, e));
  while (queue.length > 0) {
    const file = queue.shift()!;
    if (seen.has(file)) continue;
    seen.add(file);
    for (const spec of staticImports(file)) {
      if (spec.startsWith("@temporalio/")) found.push(`${relative(src, file)} -> ${spec}`);
      else if (spec.startsWith(".")) queue.push(join(file, "..", spec.replace(/\.js$/, ".ts")));
    }
  }
  return found;
}

test("R1: a relay on the JSON store, and its relay command, load no Temporal module", () => {
  assert.deepEqual(temporalLoads(["cli.ts", "serve.ts", "store/index.ts", "store/json/index.ts"]), []);
  // The check finds a load when there is one.
  assert.ok(temporalLoads(["store/temporal.ts"]).length > 0);
});
