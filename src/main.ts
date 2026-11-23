import { createServer } from "node:http";
import { configFromEnv, createRelay } from "./server.js";
import { consoleLogger } from "./log.js";

let config;
try {
  config = configFromEnv(process.env);
} catch (error) {
  process.stderr.write(`studio-relay: ${(error as Error).message}\n`);
  process.exit(1);
}

const relay = createRelay({ ...config.relay, log: consoleLogger });
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
