// Pairing codes: one code mints one agent secret.
import { createHash, randomInt } from "node:crypto";

const CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
export const PAIR_CODE_TTL_MS = 24 * 60 * 60 * 1000;

/** A new pairing code, as XXXX-XXXX-XXXX (about 60 bits). */
export function newPairCode(): string {
  const chars = Array.from({ length: 12 }, () => CODE_ALPHABET[randomInt(CODE_ALPHABET.length)]);
  return [0, 4, 8].map((i) => chars.slice(i, i + 4).join("")).join("-");
}

/** The hash of a pairing code. Case, spaces and dashes do not count. */
export function pairCodeHash(code: string): string {
  const normal = code.toUpperCase().replace(/[^A-Z0-9]/g, "");
  return createHash("sha256").update(`pair-code\0${normal}`, "utf8").digest("hex");
}
