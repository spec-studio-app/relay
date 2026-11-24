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
export const WIRE_VERSION = 2;
