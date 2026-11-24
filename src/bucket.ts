// The attachment bucket: a private S3-compatible bucket (Cloudflare R2 by
// default). Attachments never go into workflow history. Tests use MemoryBucket.
import {
  DeleteObjectsCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  GetObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import type { BucketSettings } from "./config.js";

/** Signed URLs are valid for 15 minutes. */
export const SIGNED_URL_SECONDS = 15 * 60;

export interface ObjectInfo {
  size: number;
  contentType: string;
  metadata: Record<string, string>;
}

export interface Bucket {
  /** Throws when the bucket cannot be reached with the keys. */
  check(): Promise<void>;
  put(key: string, body: Buffer, contentType: string, metadata: Record<string, string>): Promise<void>;
  /** Null when the object does not exist. */
  head(key: string): Promise<ObjectInfo | null>;
  signPut(key: string, contentType: string): Promise<string>;
  signGet(key: string): Promise<string>;
  /** Remove every object under the prefix. Returns the count. */
  removePrefix(prefix: string): Promise<number>;
}

export class S3Bucket implements Bucket {
  private readonly s3: S3Client;
  private readonly name: string;

  constructor(settings: BucketSettings) {
    this.name = settings.name;
    this.s3 = new S3Client({
      endpoint: settings.endpoint,
      region: settings.region,
      forcePathStyle: true,
      credentials: { accessKeyId: settings.accessKey, secretAccessKey: settings.secretKey },
    });
  }

  async check(): Promise<void> {
    await this.s3.send(new HeadBucketCommand({ Bucket: this.name }));
  }

  async put(key: string, body: Buffer, contentType: string, metadata: Record<string, string>): Promise<void> {
    await this.s3.send(
      new PutObjectCommand({ Bucket: this.name, Key: key, Body: body, ContentType: contentType, Metadata: metadata }),
    );
  }

  async head(key: string): Promise<ObjectInfo | null> {
    try {
      const out = await this.s3.send(new HeadObjectCommand({ Bucket: this.name, Key: key }));
      return {
        size: out.ContentLength ?? 0,
        contentType: out.ContentType ?? "application/octet-stream",
        metadata: out.Metadata ?? {},
      };
    } catch (error) {
      const status = (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode;
      if (status === 404) return null;
      throw error;
    }
  }

  signPut(key: string, contentType: string): Promise<string> {
    return getSignedUrl(this.s3, new PutObjectCommand({ Bucket: this.name, Key: key, ContentType: contentType }), {
      expiresIn: SIGNED_URL_SECONDS,
    });
  }

  signGet(key: string): Promise<string> {
    return getSignedUrl(this.s3, new GetObjectCommand({ Bucket: this.name, Key: key }), {
      expiresIn: SIGNED_URL_SECONDS,
    });
  }

  async removePrefix(prefix: string): Promise<number> {
    let removed = 0;
    let token: string | undefined;
    do {
      const page = await this.s3.send(
        new ListObjectsV2Command({ Bucket: this.name, Prefix: prefix, ContinuationToken: token }),
      );
      const keys = (page.Contents ?? []).map((o) => o.Key).filter((k): k is string => !!k);
      if (keys.length > 0) {
        await this.s3.send(
          new DeleteObjectsCommand({ Bucket: this.name, Delete: { Objects: keys.map((Key) => ({ Key })), Quiet: true } }),
        );
        removed += keys.length;
      }
      token = page.IsTruncated ? page.NextContinuationToken : undefined;
    } while (token);
    return removed;
  }
}

/** An in-memory bucket for tests. Signed URLs use the fake scheme memory://. */
export class MemoryBucket implements Bucket {
  readonly objects = new Map<string, { body: Buffer; contentType: string; metadata: Record<string, string> }>();
  failCheck = false;

  async check(): Promise<void> {
    if (this.failCheck) throw new Error("bucket not reachable");
  }

  async put(key: string, body: Buffer, contentType: string, metadata: Record<string, string>): Promise<void> {
    this.objects.set(key, { body, contentType, metadata });
  }

  async head(key: string): Promise<ObjectInfo | null> {
    const o = this.objects.get(key);
    return o ? { size: o.body.length, contentType: o.contentType, metadata: o.metadata } : null;
  }

  async signPut(key: string, contentType: string): Promise<string> {
    return `memory://put/${encodeURIComponent(key)}?type=${encodeURIComponent(contentType)}`;
  }

  async signGet(key: string): Promise<string> {
    return `memory://get/${encodeURIComponent(key)}`;
  }

  async removePrefix(prefix: string): Promise<number> {
    let removed = 0;
    for (const key of [...this.objects.keys()]) {
      if (key.startsWith(prefix)) {
        this.objects.delete(key);
        removed++;
      }
    }
    return removed;
  }
}
