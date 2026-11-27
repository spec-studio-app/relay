// The conformance suite of the relay's stores (pluggable-relay-store R3): the
// same scenarios run against the JSON store (conformance.json.test.ts) and
// the Temporal store (conformance.temporal.test.ts). Each scenario uses only
// the store interfaces, and checks fixed expected values, so both stores must
// give the same results.
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import type { ListedSpec, RelayConfigStore, SpecStore } from "../src/store/types.js";
import type { ListFilter } from "../src/workflows/specRules.js";
import type { Message } from "../src/workflows/types.js";
import { SpecDroppedError, SpecNotFoundError } from "../src/store/errors.js";
import { specAddress } from "../src/store/address.js";
import { pairCodeHash } from "../src/pairing.js";
import { DROPPED_TEXT, RECREATED_TEXT } from "../src/workflows/types.js";

export interface StoreUnderTest {
  specs: SpecStore;
  config: RelayConfigStore;
  /** Make the relay's config with this pairing code. */
  startConfig(code: string): Promise<void>;
  close(): Promise<void>;
}

/** Wait until the check passes: Temporal lists are eventually consistent. */
async function eventually<T>(check: () => Promise<T | null | undefined | false>, ms = 10_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > end) throw new Error("The condition did not pass in time.");
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

function show(slug: string, over: Record<string, unknown> = {}) {
  return { slug, title: `Title of ${slug}`, stage: "build", status: "working", mode: "autopilot", createdAt: "2026-11-01T00:00:00Z", ...over };
}

function lines(from: number, to: number, event: (n: number) => Record<string, unknown> = () => ({ kind: "status" })) {
  const out = [];
  for (let n = from; n <= to; n++) out.push({ line: n, event: event(n) });
  return out;
}

function repo(): string {
  return `/Users/me/code/app-${randomBytes(4).toString("hex")}`;
}

/** The type check of R1.S2: one message type for both stores. */
const sharedMessage: (m: Message) => Pick<Message, "from" | "text" | "replyTo" | "attachments"> = (m) => m;
void sharedMessage;

export function runConformance(label: string, open: () => Promise<StoreUnderTest>, skip: string | false = false): void {
  const scenario = (name: string, fn: (s: StoreUnderTest) => Promise<void>) =>
    test(`${label}: ${name}`, { skip }, async () => {
      const store = await open();
      try {
        await fn(store);
      } finally {
        await store.close();
      }
    });

  scenario("R3.S1: lines it already has are skipped", async ({ specs }) => {
    const spec = specAddress(repo(), "skip");
    assert.deepEqual(await specs.sync(spec, { lines: lines(1, 10), first: true, state: show("skip"), turn: null }), { line: 10, applied: true });
    assert.deepEqual(await specs.sync(spec, { lines: lines(8, 10) }), { line: 10, applied: false });
    assert.deepEqual(await specs.sync(spec, { lines: lines(8, 12) }), { line: 12, applied: true });
    const state = await specs.state(spec);
    assert.equal(state?.line, 12);
    assert.deepEqual(state?.events.map((e) => e.line), lines(1, 12).map((l) => l.line));
    assert.equal(state?.spec.repo, spec.repo);
  });

  scenario("R3.S2: the first-sync rules hold", async ({ specs }) => {
    const r = repo();
    // A merged spec with a message that waits for the Mac.
    const merged = specAddress(r, "again");
    await specs.sync(merged, { lines: lines(1, 2), first: true, state: show("again"), turn: null });
    const waiting = await specs.send(merged, { text: "For the Mac" });
    await specs.sync(merged, { lines: [{ line: 3, event: { kind: "merged" } }], state: show("again", { stage: "merged" }) });
    // The same merged spec's first sync again (same createdAt): no reset.
    assert.deepEqual(
      await specs.sync(merged, { lines: lines(1, 3), first: true, state: show("again", { stage: "merged" }) }),
      { line: 3, applied: false },
    );
    assert.equal((await specs.state(merged))?.finished, true);
    // A new spec with the same slug (another createdAt): it starts again.
    const fresh = show("again", { stage: "spec", createdAt: "2026-11-02T00:00:00Z" });
    assert.deepEqual(await specs.sync(merged, { lines: lines(1, 2), first: true, state: fresh }), { line: 2, applied: true });
    const state = await specs.state(merged);
    assert.equal(state?.finished, false);
    assert.equal(state?.line, 2);
    assert.equal(state?.mac.messages.length, 0);
    const bounce = state?.coordinator.messages.find((m) => m.replyTo === waiting.message.id);
    assert.equal(bounce?.text, `The Mac did not take the message: ${RECREATED_TEXT}`);

    // A first sync to a running spec (a lost cursor on the Mac) does not reset it.
    const running = specAddress(r, "running");
    await specs.sync(running, { lines: lines(1, 5), first: true, state: show("running"), turn: null });
    assert.deepEqual(await specs.sync(running, { lines: lines(1, 2), first: true, state: show("running") }), { line: 5, applied: false });
    assert.equal((await specs.state(running))?.line, 5);
  });

  scenario("R3.S3: messages keep their order and stay until delivered", async ({ specs }) => {
    const spec = specAddress(repo(), "inbox");
    await specs.sync(spec, { lines: lines(1, 1), first: true, state: show("inbox"), turn: null });
    const sent = [];
    for (const text of ["one", "two", "three"]) sent.push(await specs.send(spec, { text, replyTo: text === "two" ? "m-1" : undefined }));
    assert.deepEqual(sent.map((s) => s.seq), [1, 2, 3]);
    let state = await specs.state(spec);
    assert.deepEqual(state?.mac.messages.map((m) => [m.seq, m.text, m.from]), [
      [1, "one", "coordinator"],
      [2, "two", "coordinator"],
      [3, "three", "coordinator"],
    ]);
    assert.equal(state?.mac.messages[1].replyTo, "m-1");
    assert.deepEqual(await specs.ack(spec, 2), { removed: true });
    assert.deepEqual(await specs.ack(spec, 2), { removed: false });
    state = await specs.state(spec);
    assert.deepEqual(state?.mac.messages.map((m) => m.seq), [1, 3]);
    assert.deepEqual(await specs.ack(spec, 1, "The disk is full."), { removed: true });
    state = await specs.state(spec);
    assert.deepEqual(state?.mac.messages.map((m) => m.seq), [3]);
    const bounce = state?.coordinator.messages.at(-1);
    assert.equal(bounce?.text, "The Mac did not take the message: The disk is full.");
    assert.equal(bounce?.replyTo, sent[0].message.id);
    assert.equal(bounce?.from, "session");
  });

  scenario("R3.S4: the coordinator inbox and its unread state", async ({ specs }) => {
    const spec = specAddress(repo(), "coordinator");
    const message = (n: number) => ({ kind: "message", id: `m-${n}`, text: `Message ${n}`, stage: "build", t: "2026-11-01T00:00:00Z" });
    await specs.sync(spec, { lines: lines(1, 3, message), first: true, state: show("coordinator"), turn: null });
    assert.deepEqual((await specs.read(spec, 0, 0)).messages.map((m) => m.id), ["m-1", "m-2", "m-3"]);
    assert.deepEqual((await specs.read(spec, 2, 0)).messages.map((m) => m.id), ["m-3"]);
    assert.deepEqual((await specs.read(spec, undefined, 0, true)).messages, []);
    await specs.sync(spec, { lines: lines(4, 4, message) });
    const unread = await specs.read(spec, undefined, 0, true);
    assert.deepEqual(unread.messages.map((m) => m.id), ["m-4"]);
    assert.equal(unread.stage, "build");
    assert.deepEqual((await specs.read(spec, undefined, 0, true)).messages, []);
    // The inbox keeps the newest 200 messages.
    await specs.sync(spec, { lines: lines(5, 210, message) });
    const all = await specs.read(spec, 0, 0);
    assert.equal(all.messages.length, 200);
    assert.equal(all.messages[0].id, "m-11");
    assert.equal(all.messages.at(-1)?.id, "m-210");
  });

  scenario("R2.S4: a read waits for a new message", async ({ specs }) => {
    const spec = specAddress(repo(), "wait");
    await specs.sync(spec, { lines: lines(1, 1), first: true, state: show("wait"), turn: null });
    const start = Date.now();
    const read = specs.read(spec, 0, 5000);
    setTimeout(() => {
      void specs.sync(spec, { lines: [{ line: 2, event: { kind: "message", id: "late", text: "Late" } }] });
    }, 200);
    const result = await read;
    assert.deepEqual(result.messages.map((m) => m.id), ["late"]);
    assert.ok(Date.now() - start < 2000, "the read returned before its wait ended");
  });

  scenario("R3.S5: a dropped spec refuses messages", async ({ specs }) => {
    const spec = specAddress(repo(), "dropped");
    await specs.sync(spec, { lines: lines(1, 1), first: true, state: show("dropped"), turn: null });
    const waiting = await specs.send(spec, { text: "For the Mac" });
    await specs.gone(spec);
    const state = await specs.state(spec);
    assert.equal(state?.dropped, true);
    assert.equal(state?.mac.messages.length, 0);
    const bounce = state?.coordinator.messages.find((m) => m.replyTo === waiting.message.id);
    assert.equal(bounce?.text, `The Mac did not take the message: ${DROPPED_TEXT}`);
    await assert.rejects(specs.send(spec, { text: "Too late" }), (error) => error instanceof SpecDroppedError);
    // A state frame on a dropped spec changes nothing.
    assert.equal(await specs.refresh(spec, { state: show("dropped", { stage: "plan" }), turn: null }), false);
  });

  scenario("an unknown spec: no state, and a send fails as not found", async ({ specs }) => {
    const spec = specAddress(repo(), "unknown");
    assert.equal(await specs.state(spec), null);
    await assert.rejects(specs.send(spec, { text: "Hello" }), (error) => error instanceof SpecNotFoundError);
    assert.equal(await specs.refresh(spec, { state: show("unknown"), turn: null }), false);
    await specs.gone(spec);
  });

  scenario("R3.S6: lists and filters agree", async ({ specs }) => {
    const a = repo();
    const b = repo();
    await specs.sync(specAddress(a, "one"), { lines: lines(1, 1), first: true, state: show("one", { statusNote: "Note one" }), turn: { turn: "yours", needsYou: true } });
    await specs.sync(specAddress(a, "two"), { lines: lines(1, 2), first: true, state: show("two", { stage: "merged" }), turn: null });
    await specs.send(specAddress(a, "two"), { text: "Waits, so the merged spec stays" });
    await specs.sync(specAddress(b, "three"), {
      lines: [{ line: 1, event: { kind: "message", id: "u", text: "Unread" } }],
      first: true,
      state: show("three", { stage: "plan" }),
      turn: null,
    });
    const keyA = specAddress(a, "x").repoKey;
    const keyB = specAddress(b, "x").repoKey;
    const ours = (l: ListedSpec[]) => l.filter((s) => s.repoKey === keyA || s.repoKey === keyB);
    const slugs = async (filter: ListFilter, expected: string[]) =>
      eventually(async () => {
        const got = ours(await specs.list(filter)).map((s) => s.slug).sort();
        return JSON.stringify(got) === JSON.stringify([...expected].sort()) ? got : null;
      });
    await slugs({}, ["one", "two", "three"]);
    await slugs({ repoKey: keyA }, ["one", "two"]);
    await slugs({ openOnly: true }, ["one", "three"]);
    await slugs({ pendingForMac: true }, ["two"]);
    await slugs({ pendingForCoordinator: true }, ["three"]);
    const listed = await eventually(async () => {
      const l = ours(await specs.list({ repoKey: keyA }));
      return l.find((s) => s.slug === "one" && s.memo?.note === "Note one") ? l : null;
    });
    const one = listed.find((s) => s.slug === "one")!;
    assert.equal(one.running, true);
    assert.equal(one.memo?.title, "Title of one");
    assert.equal(one.memo?.stage, "build");
    assert.equal(one.memo?.repo, a);
    assert.deepEqual(one.memo?.turn, { turn: "yours", needsYou: true });
    assert.equal(one.memo?.line, 1);
    const two = listed.find((s) => s.slug === "two")!;
    assert.equal(two.memo?.waitingForMac, 1);
    assert.equal(two.memo?.stage, "merged");
  });

  scenario("R3.S7: pairing and OAuth agree", async ({ config, startConfig }) => {
    await startConfig("ABCD-EFGH");
    const code = pairCodeHash("ABCD-EFGH");
    const secret = "s".repeat(64);
    assert.equal(await config.pairCodeValid(code), true);
    assert.equal(await config.pairCodeValid(pairCodeHash("WRONG-CODE")), false);
    assert.equal(await config.pair(code, secret, new Date()), true);
    assert.equal(await config.pair(code, "t".repeat(64), new Date()), false, "the code works once");
    assert.equal(await config.verifyAgent(secret), true);
    assert.equal(await config.paired(), 1);

    await config.setRepos(["/a", "/b"], new Date("2026-11-01T00:00:00Z"));
    assert.deepEqual(await config.repos(), { repos: ["/a", "/b"], at: "2026-11-01T00:00:00.000Z" });

    assert.equal(await config.registerClient({ client_id: "c1", redirect_uris: ["https://claude.ai/cb"] }), true);
    assert.equal((await config.getClient("c1"))?.signedIn, false);
    await config.markSignedIn("c1");
    assert.equal((await config.getClient("c1"))?.signedIn, true);
    assert.equal(await config.getClient("c2"), null);

    const hour = Date.now() + 3_600_000;
    const tok = (hash: string, expiresAt = hour) => ({ hash, token: { clientId: "c1", scopes: ["mcp"], resource: "https://r", expiresAt } });
    await config.issueTokens({ access: tok("a1"), refresh: tok("r1") });
    await config.issueTokens({ access: tok("a0", Date.now() - 1000), refresh: tok("r0", Date.now() - 1000) });
    assert.equal((await config.token("access", "a1"))?.clientId, "c1");
    assert.equal(await config.token("access", "a0"), null, "an expired token is not valid");
    const old = await config.rotateRefresh("r1", "c1", { access: tok("a2"), refresh: { hash: "r2", token: { clientId: "c1", scopes: [], expiresAt: hour } }, inherit: { scopes: true, resource: true } });
    assert.equal(old?.clientId, "c1");
    assert.equal(await config.rotateRefresh("r1", "c1", { access: tok("a3"), refresh: tok("r3") }), null, "the old refresh token stops working");
    assert.equal(await config.token("refresh", "r1"), null);
    const r2 = await config.token("refresh", "r2");
    assert.deepEqual(r2?.scopes, ["mcp"], "the rotation keeps the scopes");
    assert.equal(r2?.resource, "https://r");
    await config.revoke("a2", "c1");
    assert.equal(await config.token("access", "a2"), null);

    assert.equal(await config.unpair(secret), true);
    assert.equal(await config.verifyAgent(secret), false);
    assert.equal(await config.unpair(secret), false);

    await config.setPairCode({ hash: pairCodeHash("NEW1-CODE"), expiresAt: hour });
    assert.equal(await config.pairCodeValid(pairCodeHash("NEW1-CODE")), true);
  });

  scenario("relay-threads R3.S2, R2.S3: a local answer is from the user, and it goes only to the coordinator inbox", async ({ specs }) => {
    const spec = specAddress(repo(), "senders");
    await specs.sync(spec, {
      first: true,
      lines: [
        { line: 1, event: { t: "2026-11-24T14:27:00Z", kind: "message", stage: "build", id: "s-1", text: "A question." } },
        { line: 2, event: { t: "2026-11-24T14:30:00Z", kind: "message", from: "user", stage: "build", id: "u-1", text: "Yes.", replyTo: "s-1" } },
        { line: 3, event: { t: "2026-11-24T14:31:00Z", kind: "message", from: "other", stage: "build", id: "x-1", text: "Odd." } },
      ],
      state: show("senders"),
      turn: null,
    });
    const state = await specs.state(spec);
    assert.deepEqual(state?.coordinator.messages.map((m) => [m.id, m.from]), [["s-1", "session"], ["u-1", "user"], ["x-1", "session"]]);
    assert.equal(state?.coordinator.messages[1].replyTo, "s-1");
    assert.deepEqual(state?.mac.messages, []);
  });

  scenario("relay-threads R4.S4: the status change survives the 20-event window; a send is the newest message", async ({ specs }) => {
    const spec = specAddress(repo(), "window");
    await specs.sync(spec, {
      first: true,
      lines: [{ line: 1, event: { t: "2026-11-24T10:00:00Z", kind: "status", from: "draft", to: "working", stage: "build" } }],
      state: show("window", { statusNote: "Building." }),
      turn: null,
    });
    await specs.sync(spec, { lines: lines(2, 26, (n) => ({ t: "2026-11-24T11:00:00Z", kind: "triage-decided", text: `T${n}: fix` })) });
    const read = await specs.read(spec, 0, 0);
    assert.deepEqual(read.changed, { stage: "build", status: "working", note: "Building.", at: "2026-11-24T10:00:00Z" });
    assert.deepEqual(read.turn, { turn: "working", needsYou: false });
    assert.equal(read.lastMessageAt, null);
    const { message } = await specs.send(spec, { text: "How is it going?" });
    assert.equal((await specs.read(spec, 0, 0)).lastMessageAt, message.createdAt);
  });

  scenario("relay-threads R4.S1: the stale rule: same-second order, loop-ended, and a send", async ({ specs }) => {
    const spec = specAddress(repo(), "stale");
    const at = "2026-11-24T14:27:00Z";
    await specs.sync(spec, {
      first: true,
      lines: [
        { line: 1, event: { t: "2026-11-24T14:00:00Z", kind: "status", from: "draft", to: "working", stage: "build" } },
        { line: 2, event: { t: at, kind: "message", stage: "build", id: "s-1", text: "A question." } },
      ],
      state: show("stale"),
      turn: null,
    });
    // A status change before the message, in an earlier second: current.
    assert.equal((await specs.read(spec, 0, 0)).stale, false);
    // A status change after the message in the same second: stale.
    await specs.sync(spec, { lines: [{ line: 3, event: { t: at, kind: "status", from: "working", to: "idle", stage: "build" } }] });
    assert.equal((await specs.read(spec, 0, 0)).stale, true);
    // A message after it in the same second: current again.
    await specs.sync(spec, { lines: [{ line: 4, event: { t: at, kind: "message", stage: "build", id: "s-2", text: "Done." } }] });
    assert.equal((await specs.read(spec, 0, 0)).stale, false);
    // studio loop end moved the stage: it counts as a status change.
    await specs.sync(spec, {
      lines: [{ line: 5, event: { t: "2026-11-24T15:00:00Z", kind: "loop-ended", stage: "build", text: "Back to Build." } }],
      state: show("stale", { stage: "build", status: "working" }),
    });
    const read = await specs.read(spec, 0, 0);
    assert.equal(read.stale, true);
    assert.equal(read.changed?.at, "2026-11-24T15:00:00Z");
    // The coordinator's message after the change: current.
    await specs.send(spec, { text: "How is it going?" });
    assert.equal((await specs.read(spec, 0, 0)).stale, false);
  });
}
