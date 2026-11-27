#!/usr/bin/env node
// A fake backend plugin for the relay's tests (relay-plugin-host). It speaks
// the protocol over stdio, keeps documents in memory, gives no watch and no
// events, and does what its flags say:
//   --refuse-settings     the handshake fails with bad_settings
//   --version2            it speaks only protocol 2.0
//   --conflict            every state.put answers conflict
//   --exit-file <path>    when the file exists at a call, remove it and exit with code 3
//   --record <path>       append each method name to the file
//   --echo-settings       plugin.repo in the handshake result holds the settings it got (as JSON)
import { appendFileSync, existsSync, rmSync } from "node:fs";
import { createInterface } from "node:readline";

const args = process.argv.slice(2);
const flag = (name) => args.includes(name);
const option = (name) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
};
const exitFile = option("--exit-file");
const record = option("--record");
const versions = flag("--version2") ? ["2.0"] : ["1.0"];
const docs = new Map();
let version = 0;
let shook = false;

const info = (settings) => ({
  name: "fake",
  version: "9.9.9",
  repo: flag("--echo-settings") ? JSON.stringify(settings) : "https://example.com/fake-plugin",
});

function send(msg) {
  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", ...msg }) + "\n");
}

function error(id, code, word, message, data = {}) {
  send({ id, error: { code, message, data: { code: word, ...data } } });
}

function call(id, method, params) {
  if (record) appendFileSync(record, method + "\n");
  if (exitFile && existsSync(exitFile)) {
    rmSync(exitFile);
    process.stderr.write("fake plugin: told to exit\n");
    process.exit(3);
  }
  if (method === "plugin.handshake") {
    if (shook) return error(id, -32005, "refused", "The handshake was done already.");
    if (flag("--refuse-settings")) return error(id, -32007, "bad_settings", "The setting address is required.");
    const shared = (params.protocolVersions ?? []).filter((v) => v.split(".")[0] === versions[0].split(".")[0]);
    if (shared.length === 0) {
      return error(id, -32005, "refused", `No shared protocol version. The host speaks ${params.protocolVersions.join(", ")}; the plugin speaks ${versions.join(", ")}.`);
    }
    shook = true;
    return send({
      id,
      result: {
        plugin: info(params.settings ?? {}),
        protocolVersion: versions[0],
        capabilities: ["state"],
        testHooks: [],
        durable: false,
        limits: { maxMessageBytes: 2097152, maxChunkBytes: 1048576, maxPageSize: 1000, maxWaitMs: 60000 },
      },
    });
  }
  if (!shook) return error(id, -32005, "refused", "Send plugin.handshake first.", { reason: "handshake_required" });
  const k = `${params.collection}/${params.key}`;
  switch (method) {
    case "plugin.ping":
      return send({ id, result: { status: "ok", time: new Date().toISOString() } });
    case "plugin.shutdown":
      send({ id, result: {} });
      return setTimeout(() => process.exit(0), 10);
    case "state.get": {
      const d = docs.get(k);
      return send({ id, result: d ? { exists: true, ...d } : { exists: false } });
    }
    case "state.put": {
      const d = docs.get(k);
      const stored = d ? d.version : null;
      if (flag("--conflict") || stored !== params.expectedVersion) return error(id, -32002, "conflict", "The version is not the stored version.", { version: stored });
      const next = { value: params.value, attributes: params.attributes ?? {}, version: String(++version), updatedAt: new Date().toISOString() };
      docs.set(k, next);
      return send({ id, result: { version: next.version, updatedAt: next.updatedAt } });
    }
    case "state.delete": {
      const d = docs.get(k);
      if (!d) return error(id, -32001, "not_found", "No such document.");
      if (d.version !== params.expectedVersion) return error(id, -32002, "conflict", "The version is not the stored version.", { version: d.version });
      docs.delete(k);
      return send({ id, result: { deleted: true } });
    }
    case "state.list": {
      const items = [];
      for (const [key, d] of [...docs.entries()].sort()) {
        const [collection, ...rest] = key.split("/");
        if (collection !== params.collection) continue;
        const match = Object.entries(params.filter ?? {}).every(([a, v]) => (d.attributes[a] ?? null) === v);
        if (match) items.push({ key: rest.join("/"), attributes: d.attributes, version: d.version, updatedAt: d.updatedAt, ...(params.withValues ? { value: d.value } : {}) });
      }
      return send({ id, result: { items, cursor: null } });
    }
    default:
      return error(id, -32601, "", `No method ${method}.`);
  }
}

createInterface({ input: process.stdin }).on("line", (line) => {
  if (!line.trim()) return;
  const msg = JSON.parse(line);
  call(msg.id, msg.method, msg.params ?? {});
});
process.stdin.on("end", () => process.exit(0));
