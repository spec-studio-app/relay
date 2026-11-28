// The plugin store: the relay's state in a backend plugin (PROTOCOL.md in
// spec-studio-app/plugin-sdk). The relay starts the plugin as a child process
// (RELAY_PLUGIN) or calls a sidecar over HTTP (RELAY_PLUGIN_URL). The relay
// keeps every rule; the plugin only stores.
import type { Config, PluginSettings } from "../../config.js";
import type { Bucket } from "../../bucket.js";
import type { Logger } from "../../log.js";
import { createActivities } from "../../activities.js";
import { newPairCode, pairCodeHash, PAIR_CODE_TTL_MS } from "../../pairing.js";
import { checkBucket, pairingText, StartupError } from "../../startupCommon.js";
import { RELAY_VERSION } from "../../version.js";
import { PROTOCOL_VERSIONS, PluginClient } from "../../vendor/plugin-sdk/index.js";
import { StoreRefusedError, StoreUnavailableError } from "../errors.js";
import type { RelayStore, StoreIdentity } from "../types.js";
import { Docs } from "./docs.js";
import { PluginConfigStore } from "./pluginConfigStore.js";
import { PluginDeskStore } from "./pluginDeskStore.js";
import { PluginSpecStore } from "./pluginSpecStore.js";

export { PluginConfigStore } from "./pluginConfigStore.js";
export { PluginDeskStore } from "./pluginDeskStore.js";
export { PluginSpecStore } from "./pluginSpecStore.js";

export interface PluginStoreOptions {
  bucket: Bucket | null;
  log: Logger;
  /** How long a finished spec stays after its last call (tests set less). */
  graceMs?: number;
  /** The restart backoff of a stdio plugin (tests set less). */
  restart?: { minMs?: number; maxMs?: number };
}

export interface PluginRelayStore extends RelayStore {
  kind: "plugin";
  specs: PluginSpecStore;
  desks: PluginDeskStore;
  config: PluginConfigStore;
  /** The plugin client (tests). */
  client: PluginClient;
  /** The plugin settings that the store uses. */
  plugin: PluginSettings;
  identity(): StoreIdentity | null;
}

/** The client's log entries in the relay's log: a list becomes one string. */
function pluginLog(log: Logger): (entry: { event: string; [key: string]: unknown }) => void {
  return (entry) => {
    const out: Record<string, string | number | boolean | undefined> = {};
    for (const [name, v] of Object.entries(entry)) {
      if (v === null || v === undefined) continue;
      if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") out[name] = v;
      else if (Array.isArray(v)) out[name] = v.map(String).join(name === "stderr" ? "\n" : ",");
      else out[name] = JSON.stringify(v);
    }
    log(out);
  };
}

/** The environment of a stdio plugin: only what a program needs to run, no relay secret. */
function childEnv(): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const name of ["PATH", "HOME", "TMPDIR", "TZ", "LANG", "LC_ALL"]) {
    if (process.env[name] !== undefined) env[name] = process.env[name];
  }
  return env;
}

/** The text with each secret value replaced: a plugin's message could hold one. */
function redact(text: string, plugin: PluginSettings): string {
  const secrets = [...Object.values(plugin.settings), plugin.transport === "http" ? plugin.token : undefined];
  let out = text;
  for (const secret of secrets) if (secret && secret.length >= 4) out = out.split(secret).join("[secret]");
  return out;
}

/** The owner's message when the relay cannot start or reach the plugin. */
export function pluginStartText(plugin: PluginSettings, error: unknown): string {
  const reason = redact(error instanceof Error ? error.message : String(error), plugin);
  const where =
    plugin.transport === "stdio"
      ? `the plugin from RELAY_PLUGIN (${plugin.command})`
      : "the plugin at RELAY_PLUGIN_URL";
  const check =
    plugin.transport === "stdio"
      ? "Check the command and its arguments in RELAY_PLUGIN, and the RELAY_PLUGIN_ settings."
      : "Check RELAY_PLUGIN_URL, RELAY_PLUGIN_TOKEN and the RELAY_PLUGIN_ settings, and that the plugin runs.";
  return `The relay cannot use ${where}: ${reason} The relay speaks protocol version ${PROTOCOL_VERSIONS.join(", ")}. ${check}`;
}

/** Start or connect to the plugin, with the handshake. A failure is a StartupError for the owner. */
export async function connectPlugin(plugin: PluginSettings, o: Pick<PluginStoreOptions, "log" | "restart">): Promise<PluginClient> {
  const host = { kind: "relay" as const, version: RELAY_VERSION };
  try {
    if (plugin.transport === "stdio") {
      return await PluginClient.startStdio({
        command: plugin.command,
        args: plugin.args,
        env: childEnv(),
        host,
        settings: plugin.settings,
        log: pluginLog(o.log),
        ...(o.restart ? { restart: o.restart } : {}),
      });
    }
    return await PluginClient.connectHttp({ url: plugin.url, ...(plugin.token ? { token: plugin.token } : {}), host, settings: plugin.settings, log: pluginLog(o.log) });
  } catch (error) {
    throw new StartupError(pluginStartText(plugin, error));
  }
}

export async function openPluginStore(
  config: Pick<Config, "deployId" | "publicUrl" | "plugin">,
  o: PluginStoreOptions,
): Promise<PluginRelayStore> {
  const plugin = config.plugin;
  if (!plugin) throw new StartupError("The plugin store needs RELAY_PLUGIN or RELAY_PLUGIN_URL.");
  const client = await connectPlugin(plugin, o);
  const docs = new Docs(client);
  const activities = createActivities({ bucket: o.bucket, log: o.log });
  const specs = new PluginSpecStore(docs, {
    deployId: config.deployId,
    log: o.log,
    removeAttachments: (input) => activities.removeAttachments(input),
    graceMs: o.graceMs,
  });
  const desks = new PluginDeskStore(docs, { deployId: config.deployId, log: o.log });
  const configStore = new PluginConfigStore(docs, { deployId: config.deployId });
  let closed = false;
  return {
    kind: "plugin",
    client,
    plugin,
    specs,
    desks,
    config: configStore,
    identity() {
      // The handshake after a restart can change it, so read it at each call.
      const storeId = client.info.storeId;
      return typeof storeId === "string" && storeId ? { plugin: client.info.plugin.name, storeId } : null;
    },
    async start({ log, print }) {
      await checkBucket({ bucket: o.bucket, log });
      const code = newPairCode();
      let made: boolean;
      try {
        // Another deploy's data stops the start before anything is written.
        await configStore.read();
        // The timers first: a failure there stops the start before a new
        // config makes a pairing code that would never be printed.
        await specs.startTimers();
        made = await configStore.create({ hash: pairCodeHash(code), expiresAt: Date.now() + PAIR_CODE_TTL_MS });
      } catch (error) {
        if (error instanceof StoreUnavailableError || error instanceof StoreRefusedError) throw new StartupError(pluginStartText(plugin, error));
        throw error;
      }
      if (!made) return { pairCode: null };
      log({ event: "relay_config_started" });
      print(pairingText(config, code));
      return { pairCode: code };
    },
    async close() {
      if (closed) return;
      closed = true;
      specs.close();
      desks.close();
      // A stdio plugin is this process's own: stop it. A sidecar is shared: only stop to call it.
      if (plugin.transport === "stdio") await client.shutdown().catch(() => client.close());
      else await client.close();
    },
  };
}
