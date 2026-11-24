// Run the relay. The `relay` command (cli.ts) does the same with no arguments.
import { ConfigError, loadConfig } from "./config.js";
import { StartupError } from "./startup.js";
import { serve } from "./serve.js";

try {
  await serve(loadConfig(process.env));
} catch (error) {
  // Config and startup errors name the setting, never its value. Other errors
  // print only their class, because an SDK message can hold a setting value.
  const known = error instanceof ConfigError || error instanceof StartupError;
  process.stderr.write(`relay: ${known ? (error as Error).message : `The start failed (${(error as Error).name}).`}\n`);
  process.exit(1);
}
