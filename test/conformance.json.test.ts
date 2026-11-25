// The conformance suite on the JSON store, each scenario in a new data folder.
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openJsonStore } from "../src/store/json/index.js";
import { pairCodeHash } from "../src/pairing.js";
import { MemoryBucket } from "../src/bucket.js";
import { runConformance } from "./conformance.js";

runConformance("JSON store", async () => {
  const dir = mkdtempSync(join(tmpdir(), "relay-json-"));
  const store = openJsonStore(
    { dataDir: dir, deployId: "default", publicUrl: "https://relay.example.com" },
    { bucket: new MemoryBucket(), log: () => {}, graceMs: 60_000 },
  );
  await store.specs.load();
  return {
    specs: store.specs,
    config: store.config,
    async startConfig(code) {
      await store.config.create({ hash: pairCodeHash(code), expiresAt: Date.now() + 3_600_000 });
    },
    async close() {
      await store.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
});
