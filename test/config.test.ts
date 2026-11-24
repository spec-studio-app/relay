import { test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { ConfigError, loadConfig, parsePayloadKeys } from "../src/config.js";

const BCRYPT = "$2b$12$" + "a".repeat(53);
const DATA_KEY = randomBytes(32).toString("base64");
const CONFIG_KEY = randomBytes(32).toString("base64");

export function validEnv(): Record<string, string> {
  return {
    PUBLIC_URL: "https://relay.example.com",
    OWNER_PASSWORD_HASH: BCRYPT,
    TEMPORAL_ADDRESS: "temporal.example.com:7233",
    PAYLOAD_KEYS: `data:d1:${DATA_KEY},config:c1:${CONFIG_KEY}`,
  };
}

function errorOf(env: Record<string, string>): string {
  try {
    loadConfig(env);
  } catch (error) {
    assert.ok(error instanceof ConfigError);
    return error.message;
  }
  assert.fail("loadConfig did not throw");
}

test("R1.S2: without TEMPORAL_ADDRESS the start fails with a message that names it and holds no value", () => {
  const env = validEnv();
  delete env.TEMPORAL_ADDRESS;
  const message = errorOf(env);
  assert.match(message, /TEMPORAL_ADDRESS/);
  for (const secret of Object.values(env)) {
    assert.ok(!message.includes(secret), "the message holds a setting value");
  }
  for (const part of [DATA_KEY, CONFIG_KEY, BCRYPT]) assert.ok(!message.includes(part));
});

test("no settings: the message names each required setting", () => {
  const message = errorOf({});
  for (const name of ["PUBLIC_URL", "OWNER_PASSWORD_HASH", "TEMPORAL_ADDRESS", "PAYLOAD_KEYS"]) {
    assert.match(message, new RegExp(name));
  }
  assert.match(message, /relay keys/);
});

test("the defaults: namespace, deploy ID, task queue, plain auth, no bucket", () => {
  const config = loadConfig(validEnv());
  assert.equal(config.port, 8760);
  assert.equal(config.callTimeoutMs, 90_000);
  assert.equal(config.deployId, "default");
  assert.equal(config.temporal.namespace, "default");
  assert.equal(config.temporal.taskQueue, "relay-default");
  assert.equal(config.temporal.auth, "plain");
  assert.equal(config.bucket, null);
  assert.equal(config.attachmentMaxBytes, 10 * 1024 * 1024);
  assert.equal(config.payloadKeys.data[0].id, "d1");
  assert.equal(config.payloadKeys.config[0].id, "c1");
  assert.equal(config.agentSecretSha256, undefined);
});

test("LOGIN_PASSWORD_BCRYPT is an alias of OWNER_PASSWORD_HASH", () => {
  const env = validEnv();
  delete env.OWNER_PASSWORD_HASH;
  env.LOGIN_PASSWORD_BCRYPT = BCRYPT;
  assert.equal(loadConfig(env).ownerPasswordHash, BCRYPT);
});

test("the task queue follows DEPLOY_ID, and TASK_QUEUE overrides it", () => {
  assert.equal(loadConfig({ ...validEnv(), DEPLOY_ID: "loomis" }).temporal.taskQueue, "relay-loomis");
  assert.equal(loadConfig({ ...validEnv(), DEPLOY_ID: "loomis", TASK_QUEUE: "q" }).temporal.taskQueue, "q");
  assert.match(errorOf({ ...validEnv(), DEPLOY_ID: "Bad Id" }), /DEPLOY_ID/);
});

test("Temporal auth: plain, tls, api-key and mtls", () => {
  assert.equal(loadConfig({ ...validEnv(), TEMPORAL_AUTH: "plain" }).temporal.auth, "plain");
  assert.equal(loadConfig({ ...validEnv(), TEMPORAL_AUTH: "tls" }).temporal.auth, "tls");
  const cloud = loadConfig({ ...validEnv(), TEMPORAL_API_KEY: "key-123" });
  assert.equal(cloud.temporal.auth, "api-key");
  const pem = "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----";
  const mtls = loadConfig({ ...validEnv(), TEMPORAL_TLS_CERT: pem, TEMPORAL_TLS_KEY: pem });
  assert.equal(mtls.temporal.auth, "mtls");
  assert.match(errorOf({ ...validEnv(), TEMPORAL_TLS_CERT: pem }), /TEMPORAL_TLS_KEY/);
  assert.match(errorOf({ ...validEnv(), TEMPORAL_AUTH: "api-key" }), /TEMPORAL_API_KEY/);
  const both = errorOf({ ...validEnv(), TEMPORAL_API_KEY: "key-123", TEMPORAL_TLS_CERT: pem, TEMPORAL_TLS_KEY: pem });
  assert.match(both, /not both/);
  assert.ok(!both.includes("key-123"));
  assert.match(errorOf({ ...validEnv(), TEMPORAL_AUTH: "magic" }), /TEMPORAL_AUTH/);
  assert.match(errorOf({ ...validEnv(), TEMPORAL_TLS_CERT: "/no/such/file", TEMPORAL_TLS_KEY: "/no/such/file" }), /TEMPORAL_TLS_CERT/);
});

test("the bucket: all four settings or none", () => {
  const bucket = {
    BUCKET_ENDPOINT: "https://acct.r2.cloudflarestorage.com",
    BUCKET_NAME: "studio",
    BUCKET_ACCESS_KEY: "AKIA-secret-access",
    BUCKET_SECRET_KEY: "very-secret-key",
  };
  const config = loadConfig({ ...validEnv(), ...bucket });
  assert.equal(config.bucket?.name, "studio");
  assert.equal(config.bucket?.region, "auto");
  const partial: Record<string, string> = { ...validEnv(), ...bucket };
  delete partial.BUCKET_SECRET_KEY;
  const message = errorOf(partial);
  assert.match(message, /BUCKET_SECRET_KEY/);
  assert.ok(!message.includes("AKIA-secret-access"));
});

test("PAYLOAD_KEYS: errors name the setting and never the key", () => {
  const short = randomBytes(16).toString("base64");
  for (const bad of [
    `data:d1:${short},config:c1:${CONFIG_KEY}`,
    `data:d1:${DATA_KEY}`,
    `config:c1:${CONFIG_KEY}`,
    `other:x:${DATA_KEY},config:c1:${CONFIG_KEY}`,
    `data:d1:${DATA_KEY},config:d1:${CONFIG_KEY}`,
  ]) {
    const message = errorOf({ ...validEnv(), PAYLOAD_KEYS: bad });
    assert.match(message, /PAYLOAD_KEYS/);
    for (const key of [short, DATA_KEY, CONFIG_KEY]) assert.ok(!message.includes(key));
  }
  const keys = parsePayloadKeys(`data:d2:${DATA_KEY}, data:d1:${CONFIG_KEY}, config:c1:${CONFIG_KEY}`);
  assert.deepEqual(keys.data.map((k) => k.id), ["d2", "d1"]);
});

test("other settings: bad values name the setting", () => {
  assert.match(errorOf({ ...validEnv(), PUBLIC_URL: "not a url" }), /PUBLIC_URL/);
  assert.match(errorOf({ ...validEnv(), OWNER_PASSWORD_HASH: "plain" }), /OWNER_PASSWORD_HASH/);
  assert.match(errorOf({ ...validEnv(), AGENT_SECRET_SHA256: "not-hex" }), /AGENT_SECRET_SHA256/);
  assert.match(errorOf({ ...validEnv(), ATTACHMENT_MAX_BYTES: "-1" }), /ATTACHMENT_MAX_BYTES/);
  assert.match(errorOf({ ...validEnv(), PORT: "x" }), /PORT/);
  assert.match(errorOf({ ...validEnv(), CALL_TIMEOUT_MS: "0" }), /CALL_TIMEOUT_MS/);
  assert.equal(loadConfig({ ...validEnv(), UI_ORIGIN: "https://ui.example.com/path" }).uiOrigin, "https://ui.example.com");
  assert.equal(loadConfig({ ...validEnv(), AGENT_SECRET_SHA256: "A".repeat(64) }).agentSecretSha256, "a".repeat(64));
});
