import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));

test("the dev compose profile publishes the Temporal Web UI on loopback only", () => {
  const compose = readFileSync(`${root}compose.yaml`, "utf8");
  const ports = [...compose.matchAll(/^\s*-\s*"([^"]*8233[^"]*)"/gm)].map((m) => m[1]);
  assert.deepEqual(ports, ["127.0.0.1:8233:8233"]);
});

test("R4.S3: the package and the Helm chart say the same relay version", () => {
  const pkg = JSON.parse(readFileSync(`${root}package.json`, "utf8")) as { version: string };
  const chart = readFileSync(`${root}chart/relay/Chart.yaml`, "utf8");
  assert.equal(pkg.version, "0.3.0");
  assert.match(chart, /^version: 0\.3\.0$/m);
  assert.match(chart, /^appVersion: "0\.3\.0"$/m);
});
