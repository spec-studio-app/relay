// The payload codec: AES-256-GCM for every Temporal payload, with the key ID in
// the metadata. Spec workflows use the data key; the relay-config workflow uses
// the config key (it has its own client, worker and task queue). The codec
// endpoints for the Web UI load only the data key.
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import type { Payload, PayloadCodec } from "@temporalio/common";
import proto from "@temporalio/proto";
import type { PayloadKey } from "./config.js";

const { temporal } = proto;

export const ENCODING = "binary/encrypted";
export const KEY_ID_HEADER = "encryption-key-id";
const IV_BYTES = 12;
const TAG_BYTES = 16;

const utf8 = (text: string) => new TextEncoder().encode(text);
const fromUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes);

export function encrypt(plain: Uint8Array, key: Buffer): Uint8Array {
  const iv = randomBytes(IV_BYTES);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  const body = Buffer.concat([cipher.update(plain), cipher.final()]);
  return Buffer.concat([iv, body, cipher.getAuthTag()]);
}

export function decrypt(data: Uint8Array, key: Buffer): Uint8Array {
  const buf = Buffer.from(data);
  if (buf.length < IV_BYTES + TAG_BYTES) throw new Error("The encrypted payload is too short.");
  const iv = buf.subarray(0, IV_BYTES);
  const tag = buf.subarray(buf.length - TAG_BYTES);
  const decipher = createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(buf.subarray(IV_BYTES, buf.length - TAG_BYTES)), decipher.final()]);
}

export interface AesGcmCodecOptions {
  /** The key that encrypts. */
  encryptKey: PayloadKey;
  /** Every key that may decrypt (the encrypt key is added). */
  decryptKeys?: PayloadKey[];
  /** With "keep", a payload with an unknown key ID stays encrypted instead of an error. */
  unknownKey?: "throw" | "keep";
}

export class AesGcmCodec implements PayloadCodec {
  private readonly keys = new Map<string, Buffer>();

  constructor(private readonly options: AesGcmCodecOptions) {
    for (const k of [options.encryptKey, ...(options.decryptKeys ?? [])]) this.keys.set(k.id, k.key);
  }

  async encode(payloads: Payload[]): Promise<Payload[]> {
    const { id, key } = this.options.encryptKey;
    return payloads.map((payload) => ({
      metadata: { encoding: utf8(ENCODING), [KEY_ID_HEADER]: utf8(id) },
      data: encrypt(temporal.api.common.v1.Payload.encode(payload).finish(), key),
    }));
  }

  async decode(payloads: Payload[]): Promise<Payload[]> {
    return payloads.map((payload) => {
      const encoding = payload.metadata?.encoding;
      if (!encoding || fromUtf8(encoding) !== ENCODING) return payload;
      const idBytes = payload.metadata?.[KEY_ID_HEADER];
      const id = idBytes ? fromUtf8(idBytes) : "";
      const key = this.keys.get(id);
      if (!key) {
        if (this.options.unknownKey === "keep") return payload;
        throw new Error(`No payload key has the ID ${id}.`);
      }
      return temporal.api.common.v1.Payload.decode(decrypt(payload.data ?? new Uint8Array(), key));
    });
  }
}

export function dataCodec(keys: { data: PayloadKey[] }): AesGcmCodec {
  return new AesGcmCodec({ encryptKey: keys.data[0], decryptKeys: keys.data });
}

export function configCodec(keys: { config: PayloadKey[] }): AesGcmCodec {
  return new AesGcmCodec({ encryptKey: keys.config[0], decryptKeys: keys.config });
}

/** A new PAYLOAD_KEYS value with one data key and one config key. */
export function newPayloadKeys(): string {
  const stamp = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const entry = (scope: string) => `${scope}:${scope}-${stamp}:${randomBytes(32).toString("base64")}`;
  return `${entry("data")},${entry("config")}`;
}
