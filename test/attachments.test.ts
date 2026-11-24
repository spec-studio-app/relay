import { after, before, beforeEach, test } from "node:test";
import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import type { TestWorkflowEnvironment } from "@temporalio/testing";
import { harness, startTestServer } from "./temporal.js";
import { AGENT_SECRET, callTool, connectAgent, signIn } from "./helpers.js";
import { repoKeyOf, specAddress } from "../src/mirror.js";
import { attachmentPrefix } from "../src/activities.js";

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

async function synced(h: Awaited<ReturnType<typeof harness>>, slug: string) {
  await h.mirror.sync(specAddress(REPO, slug), { line: 1, event: { kind: "note" }, state: { slug, stage: "build" }, turn: null });
}

function json(result: { content: Array<{ text: string }> }) {
  return JSON.parse(result.content[0].text);
}

test("R8.S1: Claude AI uploads a 200 KB PNG and sends it; the message carries the reference", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await synced(h, "pic");
    const png = randomBytes(200 * 1024);
    const { accessToken } = await signIn(t.base);
    const uploaded = await callTool(t.base, accessToken, "upload_attachment", {
      repo: REPO,
      slug: "pic",
      filename: "../screen shot.png",
      contentType: "image/png",
      base64: png.toString("base64"),
    });
    assert.equal(uploaded.isError, undefined, uploaded.content[0].text);
    const ref = json(uploaded);
    assert.ok(ref.objectKey.startsWith(attachmentPrefix(h.deployId, repoKeyOf(REPO), "pic")));
    assert.equal(ref.filename, "screen shot.png");
    assert.equal(ref.size, png.length);
    assert.equal(ref.sha256, createHash("sha256").update(png).digest("hex"));
    assert.deepEqual(h.bucket.objects.get(ref.objectKey)?.body, png);

    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 2, repos: [REPO] });
    await agent.next("welcome");
    const sent = await callTool(t.base, accessToken, "send_message", {
      repo: REPO,
      slug: "pic",
      text: "See the screenshot.",
      attachments: [ref.objectKey],
    });
    assert.equal(sent.isError, undefined, sent.content[0].text);
    const pushed = await agent.next("message");
    assert.deepEqual(pushed.message.attachments, [ref]);

    // The agent asks for a download URL.
    agent.send({ type: "download-url", id: "d1", objectKey: ref.objectKey });
    const download = await agent.next("download-url");
    assert.deepEqual(download, { type: "download-url", id: "d1", ok: true, url: `memory://get/${encodeURIComponent(ref.objectKey)}` });
    agent.send({ type: "download-url", id: "d2", objectKey: "other-deploy/x/y/z" });
    assert.equal((await agent.next("download-url")).ok, false);

    // An object key of another spec is refused.
    await synced(h, "other");
    const wrong = await callTool(t.base, accessToken, "send_message", {
      repo: REPO,
      slug: "other",
      text: "x",
      attachments: [ref.objectKey],
    });
    assert.equal(wrong.isError, true);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R8.S2: the session attaches a file; read_messages shows name, type, size, SHA-256 and a link", async () => {
  const h = await harness(env);
  const t = await h.relay();
  try {
    await synced(h, "proof");
    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 2, repos: [REPO] });
    await agent.next("welcome");
    const spec = { repoKey: repoKeyOf(REPO), slug: "proof" };
    agent.send({ type: "upload-url", id: "u1", spec, filename: "proof.png", contentType: "image/png", size: 1234, sha256: "ab" });
    const answer = await agent.next("upload-url");
    assert.equal(answer.ok, true);
    assert.equal(answer.id, "u1");
    assert.ok(answer.objectKey.startsWith(attachmentPrefix(h.deployId, spec.repoKey, "proof")));
    assert.match(answer.url, /^memory:\/\/put\//);
    assert.deepEqual(answer.headers, { "Content-Type": "image/png" });

    const attachment = { objectKey: answer.objectKey, filename: "proof.png", contentType: "image/png", size: 1234, sha256: "ab" };
    agent.send({
      type: "sync",
      spec,
      line: 2,
      event: { t: "2026-11-23T12:00:00Z", kind: "message", stage: "check", id: "s1", text: "Proof", attachments: [attachment] },
      state: null,
      turn: null,
    });
    await agent.next("synced");
    const { accessToken } = await signIn(t.base);
    const read = json(await callTool(t.base, accessToken, "read_messages", { repo: REPO, slug: "proof" }));
    assert.deepEqual(read.messages[0].attachments, [
      { ...attachment, url: `memory://get/${encodeURIComponent(answer.objectKey)}` },
    ]);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("R8.S3: 6 MB over a 5 MB limit fails with the limit in the text", async () => {
  const h = await harness(env, { attachmentMaxBytes: 5 * 1024 * 1024 });
  const t = await h.relay();
  try {
    await synced(h, "big");
    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "upload_attachment", {
      repo: REPO,
      slug: "big",
      filename: "big.bin",
      contentType: "application/octet-stream",
      base64: randomBytes(6 * 1024 * 1024).toString("base64"),
    });
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /The limit for an attachment is 5 MB \(5242880 bytes\)\./);
    assert.equal(h.bucket.objects.size, 0);

    const agent = await connectAgent(t.base, AGENT_SECRET, { type: "hello", version: 2, repos: [REPO] });
    await agent.next("welcome");
    agent.send({
      type: "upload-url",
      id: "u2",
      spec: { repoKey: repoKeyOf(REPO), slug: "big" },
      filename: "big.bin",
      contentType: "application/octet-stream",
      size: 6 * 1024 * 1024,
      sha256: "x",
    });
    const answer = await agent.next("upload-url");
    assert.equal(answer.ok, false);
    assert.match(answer.text, /5 MB/);
    await agent.close();
  } finally {
    await t.close();
    await h.close();
  }
});

test("upload_attachment accepts wrapped base64 of a file just under the limit", async () => {
  const max = 3000;
  const h = await harness(env, { attachmentMaxBytes: max });
  const t = await h.relay();
  try {
    await synced(h, "wrapped");
    const body = randomBytes(max - 1);
    // 76-character lines, and a newline after the padding.
    const wrapped = (body.toString("base64").match(/.{1,76}/g) ?? []).join("\r\n") + "\n";
    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "upload_attachment", {
      repo: REPO,
      slug: "wrapped",
      filename: "file.bin",
      contentType: "application/octet-stream",
      base64: wrapped,
    });
    assert.equal(result.isError, undefined, result.content[0].text);
    const ref = json(result);
    assert.equal(ref.size, max - 1);
    assert.deepEqual(h.bucket.objects.get(ref.objectKey)?.body, body);
  } finally {
    await t.close();
    await h.close();
  }
});

test("without a bucket, upload_attachment and upload-url say that no bucket is set", async () => {
  const h = await harness(env, { noBucket: true });
  const t = await h.relay();
  try {
    await synced(h, "nobucket");
    const { accessToken } = await signIn(t.base);
    const result = await callTool(t.base, accessToken, "upload_attachment", {
      repo: REPO,
      slug: "nobucket",
      filename: "a.txt",
      contentType: "text/plain",
      base64: Buffer.from("hi").toString("base64"),
    });
    assert.equal(result.isError, true);
    assert.match(result.content[0].text, /no bucket/);
  } finally {
    await t.close();
    await h.close();
  }
});

test("the S3 bucket signs PUT and GET URLs for 15 minutes without a network call", async () => {
  const { S3Bucket } = await import("../src/bucket.js");
  const bucket = new S3Bucket({
    endpoint: "https://acct.r2.cloudflarestorage.com",
    name: "studio",
    accessKey: "AKIDEXAMPLE",
    secretKey: "secret-example",
    region: "auto",
  });
  const put = new URL(await bucket.signPut("d/k/s/a.png", "image/png"));
  assert.equal(put.host, "acct.r2.cloudflarestorage.com");
  assert.equal(put.pathname, "/studio/d/k/s/a.png");
  assert.equal(put.searchParams.get("X-Amz-Expires"), "900");
  assert.ok(put.searchParams.get("X-Amz-Signature"));
  const get = new URL(await bucket.signGet("d/k/s/a.png"));
  assert.equal(get.searchParams.get("X-Amz-Expires"), "900");
});
