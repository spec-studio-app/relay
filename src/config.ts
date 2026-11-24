// Every setting of the relay comes from the environment. A setting that is
// missing or not valid stops the start with an error that names the setting.
// An error never holds the value of a setting, because many values are secrets.
import { readFileSync } from "node:fs";

export const DEFAULT_PORT = 8760;
export const DEFAULT_CALL_TIMEOUT_MS = 90_000;
export const DEFAULT_NAMESPACE = "default";
export const DEFAULT_DEPLOY_ID = "default";
export const DEFAULT_ATTACHMENT_MAX_BYTES = 10 * 1024 * 1024;
export const DEFAULT_BUCKET_REGION = "auto";

/** How the relay connects to Temporal. */
export type TemporalAuth = "plain" | "tls" | "mtls" | "api-key";

export interface TemporalSettings {
  address: string;
  namespace: string;
  auth: TemporalAuth;
  apiKey?: string;
  /** PEM text of the client certificate and key (mtls). */
  tlsCert?: string;
  tlsKey?: string;
  /** The task queue of the spec workflows. The relay-config workflow uses `<taskQueue>-config`. */
  taskQueue: string;
}

export interface BucketSettings {
  endpoint: string;
  name: string;
  accessKey: string;
  secretKey: string;
  region: string;
}

export interface PayloadKey {
  id: string;
  key: Buffer;
}

/**
 * The AES-256 keys of the payload codec. The first key of each scope encrypts;
 * every key of a scope decrypts (so an old key can stay for old payloads).
 */
export interface PayloadKeys {
  data: PayloadKey[];
  config: PayloadKey[];
}

export interface Config {
  port: number;
  publicUrl: string;
  ownerPasswordHash: string;
  callTimeoutMs: number;
  extraRedirectUris: string[];
  deployId: string;
  temporal: TemporalSettings;
  bucket: BucketSettings | null;
  attachmentMaxBytes: number;
  payloadKeys: PayloadKeys;
  /** The origin of the Temporal Web UI that may call the codec endpoints. */
  uiOrigin?: string;
}

export class ConfigError extends Error {}

type Env = Record<string, string | undefined>;

function value(env: Env, name: string): string | undefined {
  const v = env[name]?.trim();
  return v ? v : undefined;
}

function validUrl(text: string, name: string): string {
  try {
    new URL(text);
  } catch {
    throw new ConfigError(`${name} is not a valid URL.`);
  }
  return text;
}

function positiveInt(env: Env, name: string, fallback: number): number {
  const text = value(env, name);
  if (text === undefined) return fallback;
  const n = Number(text);
  if (!Number.isInteger(n) || n <= 0) throw new ConfigError(`${name} is not a positive whole number.`);
  return n;
}

/** PEM text, or the path of a file that holds it. */
function pem(env: Env, name: string): string | undefined {
  const text = value(env, name);
  if (text === undefined) return undefined;
  if (text.startsWith("-----BEGIN")) return text;
  try {
    return readFileSync(text, "utf8");
  } catch {
    throw new ConfigError(`${name} is not PEM text or the path of a readable file.`);
  }
}

/**
 * PAYLOAD_KEYS is a comma-separated list of `<scope>:<key id>:<base64 key>`.
 * The scope is `data` (spec workflows) or `config` (the relay-config workflow).
 * Each key is 32 random bytes. `relay keys` prints a new value.
 */
export function parsePayloadKeys(text: string): PayloadKeys {
  const keys: PayloadKeys = { data: [], config: [] };
  const ids = new Set<string>();
  for (const part of text.split(",").map((p) => p.trim()).filter(Boolean)) {
    const [scope, id, b64, ...rest] = part.split(":");
    if (rest.length > 0 || (scope !== "data" && scope !== "config") || !id || !b64) {
      throw new ConfigError("PAYLOAD_KEYS is not valid. Each entry must be data:<id>:<base64 key> or config:<id>:<base64 key>.");
    }
    if (!/^[A-Za-z0-9_-]{1,32}$/.test(id)) {
      throw new ConfigError("PAYLOAD_KEYS has a key ID that is not valid. Use 1 to 32 letters, digits, - or _.");
    }
    if (ids.has(id)) throw new ConfigError("PAYLOAD_KEYS has the same key ID two times.");
    ids.add(id);
    const key = Buffer.from(b64, "base64");
    if (key.length !== 32) throw new ConfigError("PAYLOAD_KEYS has a key that is not 32 bytes.");
    keys[scope].push({ id, key });
  }
  if (keys.data.length === 0) throw new ConfigError("PAYLOAD_KEYS has no data key.");
  if (keys.config.length === 0) throw new ConfigError("PAYLOAD_KEYS has no config key.");
  return keys;
}

function temporalSettings(env: Env, deployId: string): TemporalSettings {
  const address = value(env, "TEMPORAL_ADDRESS")!;
  const namespace = value(env, "TEMPORAL_NAMESPACE") ?? DEFAULT_NAMESPACE;
  const apiKey = value(env, "TEMPORAL_API_KEY");
  const tlsCert = pem(env, "TEMPORAL_TLS_CERT");
  const tlsKey = pem(env, "TEMPORAL_TLS_KEY");
  if ((tlsCert === undefined) !== (tlsKey === undefined)) {
    throw new ConfigError(tlsCert ? "TEMPORAL_TLS_KEY is missing. Set it with TEMPORAL_TLS_CERT." : "TEMPORAL_TLS_CERT is missing. Set it with TEMPORAL_TLS_KEY.");
  }
  if (apiKey && tlsCert) throw new ConfigError("Set TEMPORAL_API_KEY or TEMPORAL_TLS_CERT, not both.");

  const inferred: TemporalAuth = apiKey ? "api-key" : tlsCert ? "mtls" : "plain";
  const given = value(env, "TEMPORAL_AUTH");
  let auth: TemporalAuth = inferred;
  if (given !== undefined) {
    if (!["plain", "tls", "mtls", "api-key"].includes(given)) {
      throw new ConfigError("TEMPORAL_AUTH must be plain, tls, mtls or api-key.");
    }
    auth = given as TemporalAuth;
    if (auth === "api-key" && !apiKey) throw new ConfigError("TEMPORAL_API_KEY is missing. TEMPORAL_AUTH is api-key.");
    if (auth === "mtls" && !tlsCert) throw new ConfigError("TEMPORAL_TLS_CERT is missing. TEMPORAL_AUTH is mtls.");
    if ((auth === "plain" || auth === "tls") && (apiKey || tlsCert)) {
      throw new ConfigError(`TEMPORAL_AUTH is ${auth}, but TEMPORAL_API_KEY or TEMPORAL_TLS_CERT is set.`);
    }
  }
  const taskQueue = value(env, "TASK_QUEUE") ?? `relay-${deployId}`;
  return { address, namespace, auth, apiKey, tlsCert, tlsKey, taskQueue };
}

function bucketSettings(env: Env): BucketSettings | null {
  const names = ["BUCKET_ENDPOINT", "BUCKET_NAME", "BUCKET_ACCESS_KEY", "BUCKET_SECRET_KEY"] as const;
  const set = names.filter((name) => value(env, name) !== undefined);
  if (set.length === 0) return null;
  const missing = names.filter((name) => value(env, name) === undefined);
  if (missing.length > 0) {
    throw new ConfigError(`Missing bucket settings: ${missing.join(", ")}. Set all four bucket settings, or none.`);
  }
  return {
    endpoint: validUrl(value(env, "BUCKET_ENDPOINT")!, "BUCKET_ENDPOINT"),
    name: value(env, "BUCKET_NAME")!,
    accessKey: value(env, "BUCKET_ACCESS_KEY")!,
    secretKey: value(env, "BUCKET_SECRET_KEY")!,
    region: value(env, "BUCKET_REGION") ?? DEFAULT_BUCKET_REGION,
  };
}

/** Read the configuration from the environment. Throws a ConfigError that names the setting. */
export function loadConfig(env: Env): Config {
  const ownerPasswordHash = value(env, "OWNER_PASSWORD_HASH");
  const missing: string[] = [];
  if (!value(env, "PUBLIC_URL")) missing.push("PUBLIC_URL");
  if (!ownerPasswordHash) missing.push("OWNER_PASSWORD_HASH");
  if (!value(env, "TEMPORAL_ADDRESS")) missing.push("TEMPORAL_ADDRESS");
  if (!value(env, "PAYLOAD_KEYS")) missing.push("PAYLOAD_KEYS");
  if (missing.length > 0) {
    const hint = missing.includes("PAYLOAD_KEYS") ? " Run `relay keys` to make PAYLOAD_KEYS." : "";
    throw new ConfigError(`Missing required settings: ${missing.join(", ")}.${hint}`);
  }

  const publicUrl = validUrl(value(env, "PUBLIC_URL")!, "PUBLIC_URL");
  if (!/^\$2[aby]\$\d{2}\$.{53}$/.test(ownerPasswordHash!)) {
    throw new ConfigError("OWNER_PASSWORD_HASH is not a bcrypt hash.");
  }


  const deployId = value(env, "DEPLOY_ID") ?? DEFAULT_DEPLOY_ID;
  if (!/^[a-z0-9][a-z0-9-]{0,39}$/.test(deployId)) {
    throw new ConfigError("DEPLOY_ID must be 1 to 40 lowercase letters, digits or -, and start with a letter or digit.");
  }

  const extraRedirectUris = (value(env, "EXTRA_REDIRECT_URIS") ?? "")
    .split(",")
    .map((uri) => uri.trim())
    .filter((uri) => uri.length > 0);
  for (const uri of extraRedirectUris) validUrl(uri, "EXTRA_REDIRECT_URIS");

  let uiOrigin = value(env, "UI_ORIGIN");
  if (uiOrigin !== undefined) {
    validUrl(uiOrigin, "UI_ORIGIN");
    uiOrigin = new URL(uiOrigin).origin;
  }

  const port = positiveInt(env, "PORT", DEFAULT_PORT);
  if (port > 65535) throw new ConfigError("PORT is not a valid port.");

  return {
    port,
    publicUrl,
    ownerPasswordHash: ownerPasswordHash!,
    callTimeoutMs: positiveInt(env, "CALL_TIMEOUT_MS", DEFAULT_CALL_TIMEOUT_MS),
    extraRedirectUris,
    deployId,
    temporal: temporalSettings(env, deployId),
    bucket: bucketSettings(env),
    attachmentMaxBytes: positiveInt(env, "ATTACHMENT_MAX_BYTES", DEFAULT_ATTACHMENT_MAX_BYTES),
    payloadKeys: parsePayloadKeys(value(env, "PAYLOAD_KEYS")!),
    uiOrigin,
  };
}
