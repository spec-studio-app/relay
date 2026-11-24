// Attachments: files in the private bucket, never in workflow history. Claude AI
// uploads with upload_attachment (base64); the agent moves files with signed URLs.
import { createHash, randomUUID } from "node:crypto";
import { attachmentPrefix } from "./activities.js";
import type { Bucket } from "./bucket.js";
import type { Attachment, SpecAddress } from "./workflows/types.js";

export const NO_BUCKET_TEXT = "The relay has no bucket for attachments. The owner of the relay must set the BUCKET settings.";

export function limitText(maxBytes: number): string {
  const mb = maxBytes / (1024 * 1024);
  const shown = Number.isInteger(mb) ? `${mb} MB` : `${maxBytes} bytes`;
  return `The limit for an attachment is ${shown} (${maxBytes} bytes).`;
}

/** A file name that is safe in an object key and on the Mac. */
export function safeFilename(name: string): string {
  const base = name.split(/[\\/]/).pop() ?? "";
  const clean = base.replace(/[^A-Za-z0-9._ -]/g, "_").replace(/^\.+/, "").trim().slice(0, 120);
  return clean || "file";
}

export type UploadUrlAnswer =
  | { ok: true; url: string; objectKey: string; headers: Record<string, string> }
  | { ok: false; text: string };

export class Attachments {
  constructor(
    private readonly o: { bucket: Bucket | null; deployId: string; maxBytes: number },
  ) {}

  get maxBytes(): number {
    return this.o.maxBytes;
  }

  get enabled(): boolean {
    return this.o.bucket !== null;
  }

  private newKey(spec: SpecAddress, filename: string): string {
    return `${attachmentPrefix(this.o.deployId, spec.repoKey, spec.slug)}${randomUUID()}-${safeFilename(filename)}`;
  }

  /** True when the object key belongs to this spec of this deploy. */
  belongsTo(objectKey: string, spec: SpecAddress): boolean {
    const prefix = attachmentPrefix(this.o.deployId, spec.repoKey, spec.slug);
    return objectKey.startsWith(prefix) && !objectKey.includes("..");
  }

  /** upload_attachment: store a file from Claude AI. */
  async upload(spec: SpecAddress, filename: string, contentType: string, body: Buffer): Promise<Attachment> {
    const bucket = this.o.bucket!;
    const objectKey = this.newKey(spec, filename);
    const sha256 = createHash("sha256").update(body).digest("hex");
    const name = safeFilename(filename);
    await bucket.put(objectKey, body, contentType, { filename: name, sha256 });
    return { objectKey, filename: name, contentType, size: body.length, sha256 };
  }

  /** The attachment of an object key (for send_message), or null when it does not exist. */
  async describe(objectKey: string): Promise<Attachment | null> {
    const info = await this.o.bucket!.head(objectKey);
    if (!info) return null;
    return {
      objectKey,
      filename: info.metadata.filename ?? objectKey.split("/").pop() ?? "file",
      contentType: info.contentType,
      size: info.size,
      sha256: info.metadata.sha256 ?? "",
    };
  }

  /** The agent's upload-url: a signed PUT URL for a session attachment. */
  async uploadUrl(spec: SpecAddress, filename: string, contentType: string, size: number): Promise<UploadUrlAnswer> {
    if (!this.o.bucket) return { ok: false, text: NO_BUCKET_TEXT };
    if (!Number.isFinite(size) || size < 0) return { ok: false, text: "The size is not valid." };
    if (size > this.o.maxBytes) {
      return { ok: false, text: `The file is ${size} bytes. ${limitText(this.o.maxBytes)}` };
    }
    const objectKey = this.newKey(spec, filename);
    const url = await this.o.bucket.signPut(objectKey, contentType);
    return { ok: true, url, objectKey, headers: { "Content-Type": contentType } };
  }

  /** A signed GET URL for an object of this deploy, or null. */
  async downloadUrl(objectKey: string): Promise<string | null> {
    if (!this.o.bucket) return null;
    if (!objectKey.startsWith(`${this.o.deployId}/`) || objectKey.includes("..")) return null;
    return this.o.bucket.signGet(objectKey);
  }
}
