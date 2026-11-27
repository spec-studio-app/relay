import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));

test("the dev compose profile publishes the Temporal Web UI on loopback only", () => {
  const compose = readFileSync(`${root}compose.yaml`, "utf8");
  const ports = [...compose.matchAll(/^\s*-\s*"([^"]*8233[^"]*)"/gm)].map((m) => m[1]);
  assert.deepEqual(ports, ["127.0.0.1:8233:8233"]);
});

test("relay-plugin-host R6.S2: the package and the Helm chart say relay version 0.7.0", () => {
  const pkg = JSON.parse(readFileSync(`${root}package.json`, "utf8")) as { version: string };
  const lock = JSON.parse(readFileSync(`${root}package-lock.json`, "utf8")) as { version: string };
  const chart = readFileSync(`${root}chart/relay/Chart.yaml`, "utf8");
  assert.equal(pkg.version, "0.7.0");
  assert.equal(lock.version, "0.7.0");
  assert.match(chart, /^version: 0\.7\.0$/m);
  assert.match(chart, /^appVersion: "0\.7\.0"$/m);
});

test("pluggable-relay-store R5.S3: Compose runs the JSON store with a volume, and Temporal only in the dev profile", () => {
  const compose = readFileSync(`${root}compose.yaml`, "utf8");
  assert.match(compose, /^\s*- relay-data:\/data$/m);
  assert.match(compose, /^\s*RELAY_STORE: \$\{RELAY_STORE:-\}$/m, "an empty store setting: the relay's own rule");
  assert.match(compose, /^\s*TEMPORAL_ADDRESS: \$\{TEMPORAL_ADDRESS:-\}$/m, "no Temporal address unless the owner sets one");
  assert.doesNotMatch(compose, /PAYLOAD_KEYS:\?/, "PAYLOAD_KEYS is not required");
  const temporal = compose.slice(compose.indexOf("\n  temporal:"));
  assert.match(temporal, /profiles: \["dev"\]/);
});

test("pluggable-relay-store R5.S4: the README starts with the JSON store and explains the switch", () => {
  const readme = readFileSync(`${root}README.md`, "utf8");
  const compose = readme.slice(readme.indexOf("### Docker Compose"), readme.indexOf("Then run:"));
  assert.doesNotMatch(compose, /TEMPORAL_|PAYLOAD_KEYS/, "the first run needs no Temporal setting");
  assert.match(readme, /^### The JSON store \(the default\)$/m);
  assert.match(readme, /^### The Temporal store$/m);
  const sw = readme.slice(readme.indexOf("### Switch between stores"), readme.indexOf("## Step 3"));
  assert.match(sw, /lost/);
  assert.match(sw, /switch when no message waits/);
  assert.match(sw, /last 20 activity lines/);
});

function helm(args: string[]): string | null {
  try {
    return execFileSync("helm", ["template", "t", `${root}chart/relay`, "--set", "publicUrl=https://r.example.com", "--set", "secrets.ownerPasswordHash=x", ...args], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

test("pluggable-relay-store R5.S1: the chart's defaults run the JSON store on a persistent volume, one replica, Recreate", (t) => {
  const out = helm([]);
  if (out === null) return t.skip("helm is not installed");
  assert.match(out, /^kind: PersistentVolumeClaim$/m);
  assert.match(out, /helm\.sh\/resource-policy: keep/);
  assert.match(out, /- name: RELAY_STORE\n\s+value: "json"/);
  assert.match(out, /mountPath: \/data/);
  assert.match(out, /claimName: t-relay-data/);
  assert.match(out, /replicas: 1/);
  assert.match(out, /type: Recreate/);
  assert.match(out, /fsGroup: 1000/);
  assert.doesNotMatch(out, /TEMPORAL_ADDRESS|PAYLOAD_KEYS/);
  const values = readFileSync(`${root}chart/relay/values.yaml`, "utf8");
  assert.match(values, /the state is in the files of one pod/i);
});

test("pluggable-relay-store R5.S2: the chart with Temporal has no data volume and sets the Temporal settings", (t) => {
  const out = helm(["--set", "store=temporal", "--set", "temporal.address=t:7233", "--set", "secrets.payloadKeys=k"]);
  if (out === null) return t.skip("helm is not installed");
  assert.doesNotMatch(out, /PersistentVolumeClaim|mountPath: \/data|RELAY_DATA_DIR/);
  assert.match(out, /- name: RELAY_STORE\n\s+value: "temporal"/);
  assert.match(out, /- name: TEMPORAL_ADDRESS\n\s+value: "t:7233"/);
  assert.match(out, /key: PAYLOAD_KEYS/);
  // An older values file with temporal.address and no store keeps Temporal.
  const inferred = helm(["--set", "temporal.address=t:7233", "--set", "secrets.payloadKeys=k"]);
  assert.match(inferred!, /- name: RELAY_STORE\n\s+value: "temporal"/);
});

test("relay-plugin-host R5.S1: Compose has the plugin settings, empty, and a commented sidecar example", () => {
  const compose = readFileSync(`${root}compose.yaml`, "utf8");
  assert.match(compose, /^\s*RELAY_PLUGIN: \$\{RELAY_PLUGIN:-\}$/m);
  assert.match(compose, /^\s*RELAY_PLUGIN_URL: \$\{RELAY_PLUGIN_URL:-\}$/m);
  assert.match(compose, /^\s*RELAY_PLUGIN_TOKEN: \$\{RELAY_PLUGIN_TOKEN:-\}$/m);
  // The example service is in comments, so a plain `docker compose up` runs no plugin.
  assert.match(compose, /^\s*# plugin:$/m);
  assert.match(compose, /^\s*#\s+RELAY_PLUGIN_URL=http:\/\/plugin:7070\/rpc$/m);
  assert.match(compose, /^\s*#\s+command: \["--listen", "0\.0\.0\.0:7070"\]$/m);
  assert.doesNotMatch(compose, /^  plugin:$/m);
});

test("relay-plugin-host R5.S2: the chart's defaults have no plugin; plugin.sidecar adds a container, the URL and the token", (t) => {
  const plain = helm([]);
  if (plain === null) return t.skip("helm is not installed");
  assert.doesNotMatch(plain, /RELAY_PLUGIN|PLUGIN_TOKEN|name: plugin$/m);
  assert.equal([...plain.matchAll(/^          image: /gm)].length, 1, "one container");
  const out = helm([
    "--set", "plugin.sidecar.enabled=true",
    "--set", "plugin.sidecar.image=ghcr.io/me/plugin-mine:1.0.0",
    "--set", "secrets.pluginToken=tok-123",
    "--set", "plugin.settings.address=temporal:7233",
  ])!;
  assert.deepEqual([...out.matchAll(/^        - name: (\w+)\n          image: /gm)].map((m) => m[1]), ["relay", "plugin"]);
  assert.match(out, /- name: RELAY_PLUGIN_URL\n\s+value: "http:\/\/127\.0\.0\.1:7070\/rpc"/);
  assert.match(out, /- name: RELAY_PLUGIN_TOKEN\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: t-relay\n\s+key: PLUGIN_TOKEN/);
  assert.match(out, /- name: RELAY_PLUGIN_ADDRESS\n\s+value: "temporal:7233"/);
  assert.match(out, /PLUGIN_TOKEN: "tok-123"/);
  assert.match(out, /- --listen\n\s+- "127\.0\.0\.1:7070"/);
  assert.doesNotMatch(out, /RELAY_STORE|PersistentVolumeClaim/);
  // A command in the image: RELAY_PLUGIN, and the data volume for its files.
  const command = helm(["--set", "plugin.command=/usr/local/bin/plugin-mine --data /data"])!;
  assert.match(command, /- name: RELAY_PLUGIN\n\s+value: "\/usr\/local\/bin\/plugin-mine --data \/data"/);
  assert.match(command, /mountPath: \/data/);
  assert.doesNotMatch(command, /RELAY_STORE/);
  // A plugin and a store at once is an error.
  assert.throws(() => helm(["--set", "plugin.command=x", "--set", "store=json"]), /not both/);
});

test("relay-plugin-host R6.S2: the release notes of 0.7.0 say that nothing changes for owners, and the release uses them", () => {
  const notes = readFileSync(`${root}release-notes/0.7.0.md`, "utf8");
  assert.match(notes, /\*\*Nothing changes for owners\.\*\*/);
  assert.match(notes, /RELAY_PLUGIN/);
  const release = readFileSync(`${root}.github/workflows/release.yml`, "utf8");
  assert.match(release, /release-notes\/\$version\.md/);
});

test("relay-plugin-host R6.S1: the README explains a store plugin and keeps JSON the default", () => {
  const readme = readFileSync(`${root}README.md`, "utf8");
  const section = readme.slice(readme.indexOf("### A store plugin"), readme.indexOf("### Switch between stores"));
  assert.match(section, /plugin-json/);
  assert.match(section, /plugin-temporal/);
  assert.match(section, /RELAY_PLUGIN_URL/);
  assert.match(readme, /The JSON store stays the default/);
});

test("review: the chart refuses store: plugin alone, takes plugin.url, mounts the data volume in a sidecar with dataPath, and renders a values file with no plugin key", (t) => {
  if (helm([]) === null) return t.skip("helm is not installed");
  assert.throws(() => helm(["--set", "store=plugin"]), /For a plugin, set plugin\.command, plugin\.url or plugin\.sidecar\.enabled/);
  const url = helm(["--set", "plugin.url=http://plugin.relay.svc:7070/rpc", "--set", "secrets.pluginToken=tok"])!;
  assert.match(url, /- name: RELAY_PLUGIN_URL\n\s+value: "http:\/\/plugin\.relay\.svc:7070\/rpc"/);
  assert.match(url, /PLUGIN_TOKEN: "tok"/);
  assert.doesNotMatch(url, /RELAY_STORE|PersistentVolumeClaim/);
  const sidecar = helm(["--set", "plugin.sidecar.enabled=true", "--set", "plugin.sidecar.image=i", "--set", "secrets.pluginToken=t", "--set", "plugin.sidecar.dataPath=/data"])!;
  assert.match(sidecar, /kind: PersistentVolumeClaim/);
  const plugin = sidecar.slice(sidecar.indexOf("        - name: plugin\n"));
  assert.match(plugin, /- name: data\n\s+mountPath: "\/data"/);
  // A large number stays a whole number (no YAML round trip).
  assert.match(helm(["--set", "plugin.command=x", "--set", "plugin.settings.max_bytes=10485760"])!, /- name: RELAY_PLUGIN_MAX_BYTES\n\s+value: "10485760"/);
  // The same from a values file, where Helm reads every number as a float.
  const valuesDir = mkdtempSync(join(tmpdir(), "relay-values-"));
  writeFileSync(`${valuesDir}/v.yaml`, "plugin:\n  command: x\n  settings:\n    max_bytes: 10485760\n    ratio: 0.5\n");
  const fromFile = helm(["-f", `${valuesDir}/v.yaml`])!;
  assert.match(fromFile, /- name: RELAY_PLUGIN_MAX_BYTES\n\s+value: "10485760"/);
  assert.match(fromFile, /- name: RELAY_PLUGIN_RATIO\n\s+value: "0\.5"/);
  // An empty setting is empty, not "<nil>"; a --set number keeps all its digits, in a setting and in the sidecar args.
  writeFileSync(`${valuesDir}/e.yaml`, "plugin:\n  command: x\n  settings:\n    empty:\n");
  assert.match(helm(["-f", `${valuesDir}/e.yaml`])!, /- name: RELAY_PLUGIN_EMPTY\n\s+value: ""/);
  assert.match(helm(["--set", "plugin.command=x", "--set", "plugin.settings.big=9007199254740993"])!, /value: "9007199254740993"/);
  const args = helm(["--set", "plugin.sidecar.enabled=true", "--set", "plugin.sidecar.image=i", "--set", "secrets.pluginToken=t", "--set", "plugin.sidecar.args={--n,9007199254740993}"])!;
  assert.match(args, /- "--n"\n\s+- "9007199254740993"/);
  rmSync(valuesDir, { recursive: true, force: true });
  // A values file from 0.5.0 (helm upgrade --reuse-values) has no plugin key.
  const dir = mkdtempSync(join(tmpdir(), "relay-chart-"));
  cpSync(`${root}chart/relay`, `${dir}/relay`, { recursive: true });
  const values = readFileSync(`${dir}/relay/values.yaml`, "utf8");
  writeFileSync(`${dir}/relay/values.yaml`, values.replace(/\nplugin:\n[\s\S]*?\n(?=# The volume at)/, "\n"));
  assert.doesNotMatch(readFileSync(`${dir}/relay/values.yaml`, "utf8"), /^plugin:/m);
  const old = execFileSync("helm", ["template", "t", `${dir}/relay`, "--set", "publicUrl=https://r.example.com", "--set", "secrets.ownerPasswordHash=x"], { encoding: "utf8" });
  assert.match(old, /- name: RELAY_STORE\n\s+value: "json"/);
  rmSync(dir, { recursive: true, force: true });
});

test("review: the Compose sidecar example names its volume under volumes", () => {
  const compose = readFileSync(`${root}compose.yaml`, "utf8");
  assert.match(compose, /^\s*#\s+- plugin-data:\/data$/m);
  assert.match(compose, /^volumes:\n[\s\S]*^\s*# plugin-data:$/m);
});
