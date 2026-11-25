#!/usr/bin/env node
// The relay command.
//   relay            Run the relay (the same as `relay serve`).
//   relay pair-code  Make a new pairing code for a Mac.
//   relay keys       Print a new PAYLOAD_KEYS value.
//   relay password-hash  Read a password on stdin and print its bcrypt hash.
import bcrypt from "bcryptjs";
import { ConfigError, loadConfig } from "./config.js";
import { newPayloadKeys } from "./codec.js";
import { StartupError, newPairCodeIn, pairingText } from "./startup.js";
import { serve } from "./serve.js";
import { openStore } from "./store/index.js";
import { consoleLogger } from "./log.js";

const USAGE = `Usage:
  relay [serve]     Run the relay.
  relay pair-code   Make a new pairing code for a Mac. The old code stops working.
  relay keys        Print a new PAYLOAD_KEYS value. Keep it: the relay needs the same keys at each start.
  relay password-hash
                    Read the owner password on stdin and print its bcrypt hash (OWNER_PASSWORD_HASH).
`;

function fail(error: unknown): never {
  const known = error instanceof ConfigError || error instanceof StartupError;
  process.stderr.write(`relay: ${known ? (error as Error).message : `The command failed (${(error as Error).name}).`}\n`);
  process.exit(1);
}

async function pairCode(): Promise<void> {
  const config = loadConfig(process.env);
  // The JSON store writes the config file under its lock; the Temporal store
  // sends an update that the running relay's worker applies.
  const store = await openStore(config, { bucket: null, log: consoleLogger, worker: false });
  let code: string;
  try {
    code = await newPairCodeIn(store.config);
  } catch {
    void store.close().catch(() => undefined);
    throw new StartupError(
      config.store === "json"
        ? "The relay cannot make a pairing code. Run the command with the same RELAY_DATA_DIR as the relay, then try again."
        : "The relay cannot make a pairing code. Check that the relay runs with the same settings, then try again.",
    );
  }
  process.stdout.write(pairingText(config, code) + "\n");
  await store.close().catch(() => undefined);
  process.exit(0);
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

async function passwordHash(): Promise<void> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) chunks.push(chunk as Buffer);
  const password = Buffer.concat(chunks).toString("utf8").replace(/\r?\n$/, "");
  if (password.length === 0) {
    process.stderr.write("relay: Give the password on stdin.\n");
    process.exit(1);
  }
  process.stdout.write(bcrypt.hashSync(password, 12) + "\n");
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
    case "password-hash":
      await passwordHash();
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
