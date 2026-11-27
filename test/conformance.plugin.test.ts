// The conformance suite on the plugin store (relay-plugin-host R3.S1), with
// the plugin-sdk memory plugin over stdio and over HTTP. Each scenario gets a
// new plugin and a new data file.
import { pairCodeHash } from "../src/pairing.js";
import { runConformance } from "./conformance.js";
import { memoryStore, skipWithoutPlugin } from "./plugin.js";

const skip = skipWithoutPlugin();

for (const transport of ["stdio", "http"] as const) {
  runConformance(
    `Plugin store (memory, ${transport})`,
    async () => {
      const s = await memoryStore(transport, { start: false });
      return {
        specs: s.store.specs,
        config: s.store.config,
        async startConfig(code) {
          await s.store.config.create({ hash: pairCodeHash(code), expiresAt: Date.now() + 3_600_000 });
        },
        close: () => s.close(),
      };
    },
    skip,
  );
}
