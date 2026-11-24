import { createServer } from "node:http";
import { createRelay } from "./server.js";
import { loadConfig } from "./config.js";
import { consoleLogger } from "./log.js";

let config;
try {
  config = loadConfig(process.env);
} catch (error) {
  process.stderr.write(`relay: ${(error as Error).message}\n`);
  process.exit(1);
}

const relay = createRelay({
  publicUrl: config.publicUrl,
  loginPasswordBcrypt: config.ownerPasswordHash,
  agentSecretSha256: config.agentSecretSha256,
  callTimeoutMs: config.callTimeoutMs,
  extraRedirectUris: config.extraRedirectUris,
  log: consoleLogger,
});
const server = createServer(relay.app);
relay.attach(server);

server.listen(config.port, () => {
  consoleLogger({ event: "listening", port: config.port });
});

const shutdown = () => {
  relay.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
