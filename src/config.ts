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
export const DEFAULT_DATA_DIR = "/data";

/** Where the relay keeps its state: a built-in store, or a backend plugin. */
export type StoreKind = "json" | "temporal" | "plugin";
/** Why the relay uses its store: RELAY_STORE, TEMPORAL_ADDRESS without RELAY_STORE, a plugin setting, or the default. */
export type StoreReason = "set" | "temporal-address" | "plugin" | "default";

/** A backend plugin: a command that the relay starts (stdio), or a sidecar (HTTP). */
export type PluginTransport =
  | { transport: "stdio"; command: string; args: string[] }
  | { transport: "http"; url: string; token?: string };

export type PluginSettings = PluginTransport & {
  /** The handshake settings, from the RELAY_PLUGIN_<NAME> variables. Some values are secrets. */
  settings: Record<string, string>;
  /** The variable that chose the plugin, for messages. */
  from: "RELAY_PLUGIN" | "RELAY_PLUGIN_URL";
};

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
  store: StoreKind;
  storeReason: StoreReason;
  /** The folder of the JSON store (RELAY_DATA_DIR). */
  dataDir: string;
  /** The Temporal settings: only with the Temporal store. */
  temporal: TemporalSettings | null;
  bucket: BucketSettings | null;
  attachmentMaxBytes: number;
  /** The payload codec keys: only with the Temporal store. */
  payloadKeys: PayloadKeys | null;
  /** The origin of the Temporal Web UI that may call the codec endpoints. */
  uiOrigin?: string;
  /** The backend plugin: only with the plugin store. */
  plugin: PluginSettings | null;
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

/**
 * Split a command into words as a shell does: spaces separate words, and
 * single quotes, double quotes and a backslash keep spaces in a word. There
 * is no other shell feature: no variables, no globs, no pipes.
 */
export function splitCommand(text: string): string[] {
  const words: string[] = [];
  let word = "";
  let inWord = false;
  let quote: "'" | '"' | null = null;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quote === "'") {
      if (c === "'") quote = null;
      else word += c;
      continue;
    }
    if (quote === '"') {
      if (c === '"') quote = null;
      else if (c === "\\" && (text[i + 1] === '"' || text[i + 1] === "\\")) word += text[++i];
      else word += c;
      continue;
    }
    if (c === "'" || c === '"') {
      quote = c;
      inWord = true;
    } else if (c === "\\" && i + 1 < text.length) {
      word += text[++i];
      inWord = true;
    } else if (/\s/.test(c)) {
      if (inWord) words.push(word);
      word = "";
      inWord = false;
    } else {
      word += c;
      inWord = true;
    }
  }
  if (quote) throw new ConfigError("RELAY_PLUGIN has a quote with no end.");
  if (inWord) words.push(word);
  return words;
}

/** The RELAY_PLUGIN_ variables that are relay settings, not plugin settings. */
const PLUGIN_RESERVED = new Set(["RELAY_PLUGIN_URL", "RELAY_PLUGIN_TOKEN"]);
const PLUGIN_PREFIX = "RELAY_PLUGIN_";

/** The handshake settings: each RELAY_PLUGIN_<NAME> as <name>, in lowercase. */
function pluginSettingsOf(env: Env): Record<string, string> {
  const settings: Record<string, string> = {};
  for (const name of Object.keys(env).sort()) {
    if (!name.startsWith(PLUGIN_PREFIX) || PLUGIN_RESERVED.has(name)) continue;
    const v = env[name];
    if (v === undefined) continue;
    const setting = name.slice(PLUGIN_PREFIX.length).toLowerCase();
    if (!/^[a-z][a-z0-9_]{0,63}$/.test(setting)) {
      throw new ConfigError(`${name} is not a valid plugin setting name. After RELAY_PLUGIN_, use a letter, then letters, digits or _ (at most 64).`);
    }
    settings[setting] = v;
  }
  return settings;
}

/** The plugin from RELAY_PLUGIN or RELAY_PLUGIN_URL, or null. */
function pluginOf(env: Env): PluginSettings | null {
  const command = value(env, "RELAY_PLUGIN");
  const url = value(env, "RELAY_PLUGIN_URL");
  if (command !== undefined && url !== undefined) {
    throw new ConfigError("Set RELAY_PLUGIN or RELAY_PLUGIN_URL, not both.");
  }
  if (command === undefined && url === undefined) {
    if (value(env, "RELAY_PLUGIN_TOKEN") !== undefined) throw new ConfigError("RELAY_PLUGIN_TOKEN is set, but RELAY_PLUGIN_URL is not.");
    return null;
  }
  if (value(env, "RELAY_STORE") !== undefined) {
    throw new ConfigError(`Set RELAY_STORE or ${command !== undefined ? "RELAY_PLUGIN" : "RELAY_PLUGIN_URL"}, not both. A plugin is the relay's store.`);
  }
  const settings = pluginSettingsOf(env);
  if (command !== undefined) {
    if (value(env, "RELAY_PLUGIN_TOKEN") !== undefined) throw new ConfigError("RELAY_PLUGIN_TOKEN is for RELAY_PLUGIN_URL only.");
    const words = splitCommand(command);
    if (words.length === 0) throw new ConfigError("RELAY_PLUGIN has no command.");
    return { transport: "stdio", command: words[0], args: words.slice(1), settings, from: "RELAY_PLUGIN" };
  }
  validUrl(url!, "RELAY_PLUGIN_URL");
  const protocol = new URL(url!).protocol;
  if (protocol !== "http:" && protocol !== "https:") throw new ConfigError("RELAY_PLUGIN_URL must be an http or https URL.");
  const token = value(env, "RELAY_PLUGIN_TOKEN");
  return { transport: "http", url: url!, ...(token ? { token } : {}), settings, from: "RELAY_PLUGIN_URL" };
}

/** The store from RELAY_STORE, else Temporal when TEMPORAL_ADDRESS is set, else JSON. */
function storeOf(env: Env): { store: StoreKind; storeReason: StoreReason } {
  const given = value(env, "RELAY_STORE");
  if (given !== undefined) {
    if (given !== "json" && given !== "temporal") throw new ConfigError("RELAY_STORE must be json or temporal.");
    return { store: given, storeReason: "set" };
  }
  if (value(env, "TEMPORAL_ADDRESS")) return { store: "temporal", storeReason: "temporal-address" };
  return { store: "json", storeReason: "default" };
}

/** Read the configuration from the environment. Throws a ConfigError that names the setting. */
export function loadConfig(env: Env): Config {
  const ownerPasswordHash = value(env, "OWNER_PASSWORD_HASH");
  const plugin = pluginOf(env);
  const { store, storeReason } = plugin ? { store: "plugin" as const, storeReason: "plugin" as const } : storeOf(env);
  const missing: string[] = [];
  if (!value(env, "PUBLIC_URL")) missing.push("PUBLIC_URL");
  if (!ownerPasswordHash) missing.push("OWNER_PASSWORD_HASH");
  if (store === "temporal" && !value(env, "TEMPORAL_ADDRESS")) missing.push("TEMPORAL_ADDRESS");
  if (store === "temporal" && !value(env, "PAYLOAD_KEYS")) missing.push("PAYLOAD_KEYS");
  if (missing.length > 0) {
    const hint = missing.includes("PAYLOAD_KEYS") ? " Run `relay keys` to make PAYLOAD_KEYS." : "";
    const temporal = missing.some((m) => m.startsWith("TEMPORAL_") || m === "PAYLOAD_KEYS") ? " The Temporal store needs them." : "";
    throw new ConfigError(`Missing required settings: ${missing.join(", ")}.${temporal}${hint}`);
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
    store,
    storeReason,
    dataDir: value(env, "RELAY_DATA_DIR") ?? DEFAULT_DATA_DIR,
    temporal: store === "temporal" ? temporalSettings(env, deployId) : null,
    bucket: bucketSettings(env),
    attachmentMaxBytes: positiveInt(env, "ATTACHMENT_MAX_BYTES", DEFAULT_ATTACHMENT_MAX_BYTES),
    payloadKeys: store === "temporal" ? parsePayloadKeys(value(env, "PAYLOAD_KEYS")!) : null,
    uiOrigin,
    plugin,
  };
}
