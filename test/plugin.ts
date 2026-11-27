// Test support for the plugin store: the plugin-sdk memory plugin and a store
// on it, over stdio or over HTTP.
//
// The memory plugin comes from RELAY_TEST_PLUGIN (the path of a built binary),
// else it is built with Go from PLUGIN_SDK_DIR (a checkout of plugin-sdk, see
// src/vendor/plugin-sdk/SOURCE.md for the commit). With neither, the plugin
// tests skip with a note, so `npm test` works without Go. CI sets both.
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { MemoryBucket } from "../src/bucket.js";
import type { PluginSettings } from "../src/config.js";
import type { Logger } from "../src/log.js";
import { openPluginStore, type PluginRelayStore } from "../src/store/plugin/index.js";

let built: string | null | undefined;

/** The path of the memory plugin, or null when the tests cannot get it. */
export function memoryPlugin(): string | null {
  if (built !== undefined) return built;
  const given = process.env.RELAY_TEST_PLUGIN;
  if (given) return (built = existsSync(given) ? given : null);
  const sdk = process.env.PLUGIN_SDK_DIR;
  if (!sdk) return (built = null);
  const out = join(mkdtempSync(join(tmpdir(), "relay-memory-plugin-")), "memory");
  try {
    execFileSync("go", ["build", "-o", out, "."], { cwd: join(sdk, "examples", "memory"), stdio: "ignore" });
    return (built = out);
  } catch {
    return (built = null);
  }
}

/** The reason for a skip, or false when the memory plugin is there. */
export function skipWithoutPlugin(): string | false {
  if (memoryPlugin()) return false;
  // CI sets RELAY_REQUIRE_PLUGIN, so the plugin tests never skip there in silence.
  if (process.env.RELAY_REQUIRE_PLUGIN) throw new Error("RELAY_REQUIRE_PLUGIN is set, but the tests cannot get the memory plugin (RELAY_TEST_PLUGIN or PLUGIN_SDK_DIR).");
  return "Set RELAY_TEST_PLUGIN (a memory plugin binary) or PLUGIN_SDK_DIR (a plugin-sdk checkout, with Go) to run the plugin store tests.";
}

/** The fake plugin script (test/fixtures/fakePlugin.mjs), from dist/test. */
export const FAKE_PLUGIN = fileURLToPath(new URL("../../test/fixtures/fakePlugin.mjs", import.meta.url));

export function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() => resolve(typeof address === "object" && address ? address.port : 0));
    });
  });
}

/** A memory plugin that listens over HTTP, with a token. */
export async function httpMemoryPlugin(dataFile?: string): Promise<{ url: string; token: string; child: ChildProcess; stop(): Promise<void> }> {
  const port = await freePort();
  const token = "test-token-" + Math.random().toString(36).slice(2);
  const args = ["--listen", `127.0.0.1:${port}`, "--token", token, ...(dataFile ? ["--data", dataFile] : [])];
  const child = spawn(memoryPlugin()!, args, { stdio: ["ignore", "ignore", "ignore"] });
  const url = `http://127.0.0.1:${port}/rpc`;
  const end = Date.now() + 10_000;
  for (;;) {
    try {
      await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
      break;
    } catch {
      if (Date.now() > end) throw new Error("The memory plugin did not listen in time.");
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
  return {
    url,
    token,
    child,
    stop: () =>
      new Promise<void>((resolve) => {
        if (child.exitCode !== null) return resolve();
        child.once("exit", () => resolve());
        child.kill("SIGTERM");
      }),
  };
}

export interface PluginStoreUnderTest {
  store: PluginRelayStore;
  logs: Array<Record<string, unknown>>;
  printed: string[];
  close(): Promise<void>;
}

/** A plugin store on a memory plugin over stdio (with a data file) or over HTTP. */
export async function memoryStore(
  transport: "stdio" | "http",
  o: { graceMs?: number; dataFile?: string; bucket?: MemoryBucket; start?: boolean; restart?: { minMs?: number; maxMs?: number } } = {},
): Promise<PluginStoreUnderTest> {
  const dir = mkdtempSync(join(tmpdir(), "relay-plugin-"));
  const dataFile = o.dataFile ?? join(dir, "memory.json");
  const logs: Array<Record<string, unknown>> = [];
  const printed: string[] = [];
  const log: Logger = (entry) => logs.push(entry);
  let sidecar: Awaited<ReturnType<typeof httpMemoryPlugin>> | null = null;
  let plugin: PluginSettings;
  if (transport === "stdio") {
    plugin = { transport: "stdio", command: memoryPlugin()!, args: ["--data", dataFile], settings: {}, from: "RELAY_PLUGIN" };
  } else {
    sidecar = await httpMemoryPlugin(dataFile);
    plugin = { transport: "http", url: sidecar.url, token: sidecar.token, settings: {}, from: "RELAY_PLUGIN_URL" };
  }
  const store = await openPluginStore(
    { deployId: "default", publicUrl: "https://relay.example.com", plugin },
    { bucket: o.bucket ?? new MemoryBucket(), log, graceMs: o.graceMs ?? 60_000, ...(o.restart ? { restart: o.restart } : {}) },
  );
  if (o.start !== false) await store.start({ log, print: (text) => printed.push(text) });
  return {
    store,
    logs,
    printed,
    async close() {
      await store.close();
      await sidecar?.stop();
      if (!o.dataFile) rmSync(dir, { recursive: true, force: true });
    },
  };
}
