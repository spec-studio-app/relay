import { readFileSync } from "node:fs";

/** The relay's version, from package.json. */
export const RELAY_VERSION: string = (() => {
  try {
    const pkg = JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8")) as { version?: string };
    return pkg.version ?? "0.0.0";
  } catch {
    return "0.0.0";
  }
})();

/** The agent wire version that this relay speaks. */
export const WIRE_VERSION = 3;

/** The agent wire versions that this relay speaks, lowest first. */
export const SUPPORTED_WIRES: readonly number[] = [WIRE_VERSION];

/** The first app version that speaks each wire version. */
export const MIN_APP_VERSION_FOR_WIRE: Readonly<Record<number, string>> = { 3: "0.1.1" };

/** The close code when the agent and the relay have no common wire version. */
export const INCOMPATIBLE_CLOSE_CODE = 4002;
