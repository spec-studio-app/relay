#!/usr/bin/env node
// The relay command.
//   relay            Run the relay (the same as `relay serve`).
//   relay pair-code  Make a new pairing code for a Mac.
//   relay keys       Print a new PAYLOAD_KEYS value.
import { ConfigError, loadConfig } from "./config.js";
import { newPayloadKeys } from "./codec.js";
import { StartupError, makePairCode, pairingText } from "./startup.js";
import { connectClient, makeClients } from "./temporal/connection.js";
import { serve, TEMPORAL_UNREACHABLE } from "./serve.js";

const USAGE = `Usage:
  relay [serve]     Run the relay.
  relay pair-code   Make a new pairing code for a Mac. The old code stops working.
  relay keys        Print a new PAYLOAD_KEYS value. Keep it: the relay needs the same keys at each start.
`;

function fail(error: unknown): never {
  const known = error instanceof ConfigError || error instanceof StartupError;
  process.stderr.write(`relay: ${known ? (error as Error).message : `The command failed (${(error as Error).name}).`}\n`);
  process.exit(1);
}

async function pairCode(): Promise<void> {
  const config = loadConfig(process.env);
  let connection;
  try {
    connection = await connectClient(config.temporal);
  } catch {
    throw new StartupError(TEMPORAL_UNREACHABLE);
  }
  const clients = makeClients(connection, config.temporal.namespace, config.payloadKeys);
  let code: string;
  try {
    code = await makePairCode(clients.config, config.deployId);
  } catch {
    throw new StartupError("The relay cannot make a pairing code. Start the relay once, then try again.");
  }
  process.stdout.write(pairingText(config, code) + "\n");
  await connection.close();
}

function keys(): void {
  process.stdout.write(
    [
      "Set this value as PAYLOAD_KEYS, in a secret. Keep a copy: without the same keys, the relay cannot read its data.",
      "",
      `PAYLOAD_KEYS=${newPayloadKeys()}`,
      "",
    ].join("\n"),
  );
}

const command = process.argv[2] ?? "serve";
try {
  switch (command) {
    case "serve":
      await serve(loadConfig(process.env));
      break;
    case "pair-code":
      await pairCode();
      break;
    case "keys":
      keys();
      break;
    case "help":
    case "--help":
    case "-h":
      process.stdout.write(USAGE);
      break;
    default:
      process.stderr.write(USAGE);
      process.exit(2);
  }
} catch (error) {
  fail(error);
}
