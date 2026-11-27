// The documents of the plugin store: a read, the rule, and a write with the
// read version (PROTOCOL.md section 14). On a conflict the relay reads again
// and applies the rule again, at most MAX_TRIES times. The plugin's errors
// become the relay's store errors here, in one place.
import { PluginError, isPluginError, type Attributes, type Json, type NewEvent, type PluginClient } from "../../vendor/plugin-sdk/index.js";
import { StoreRefusedError, StoreUnavailableError } from "../errors.js";

/** A read, apply, write that lost this many races in a row fails as "not available". */
export const MAX_TRIES = 5;

export interface Doc<T> {
  value: T;
  version: string;
  attributes: Attributes;
}

/** What a rule does with a document: write a new value, delete it, or nothing. */
export type Outcome<T, R> =
  | { write: { value: T; attributes: Attributes }; result: R }
  | { delete: true; result: R }
  | { result: R };

/** The relay's store error for a plugin error. Other errors stay as they are. */
export function storeError(error: unknown): unknown {
  if (isPluginError(error, "unavailable")) return new StoreUnavailableError();
  if (isPluginError(error, "refused") || isPluginError(error, "bad_settings")) return new StoreRefusedError(error.message);
  // Any other plugin error (an internal error, a call the plugin does not know): the store cannot do the call now.
  if (error instanceof PluginError) return new StoreUnavailableError();
  return error;
}

/** The text when a document of another deploy is in the way. */
export const OTHER_DEPLOY_TEXT =
  "The plugin's storage holds the data of another relay deploy (another DEPLOY_ID). Give each deploy its own plugin storage.";

/** Run a plugin call, and give the relay's store error when it fails. */
export async function guard<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    throw storeError(error);
  }
}

export class Docs {
  constructor(readonly client: PluginClient) {}

  async get<T>(collection: string, key: string): Promise<Doc<T> | null> {
    const r = await guard(() => this.client.state.get({ collection, key }));
    if (!r.exists) return null;
    return { value: r.value as T, version: r.version, attributes: r.attributes };
  }

  /** Make the document. False when it exists already. */
  async create<T>(collection: string, key: string, value: T, attributes: Attributes = {}): Promise<boolean> {
    try {
      await this.client.state.put({ collection, key, value: value as Json, attributes, expectedVersion: null });
      return true;
    } catch (error) {
      if (isPluginError(error, "conflict")) return false;
      throw storeError(error);
    }
  }

  /** Delete the document (at its version, else at the version read now). False when it does not exist. */
  async remove(collection: string, key: string, expectedVersion?: string): Promise<boolean> {
    try {
      const version = expectedVersion ?? (await this.get(collection, key))?.version;
      if (version === undefined) return false;
      await this.client.state.delete({ collection, key, expectedVersion: version });
      return true;
    } catch (error) {
      if (isPluginError(error, "not_found")) return false;
      throw storeError(error);
    }
  }

  /**
   * Read, apply, write. The rule gets the document (or null) and says what to
   * write. It runs again on each conflict, with the document read again, so it
   * must change only its own copy. An error from the rule ends the call.
   */
  async update<T, R>(collection: string, key: string, rule: (doc: Doc<T> | null) => Outcome<T, R>): Promise<R> {
    for (let tries = 0; tries < MAX_TRIES; tries++) {
      const doc = await this.get<T>(collection, key);
      const out = rule(doc);
      try {
        if ("write" in out) {
          await this.client.state.put({
            collection,
            key,
            value: out.write.value as Json,
            attributes: out.write.attributes,
            expectedVersion: doc ? doc.version : null,
          });
        } else if ("delete" in out && doc) {
          await this.client.state.delete({ collection, key, expectedVersion: doc.version });
        }
        return out.result;
      } catch (error) {
        if (isPluginError(error, "conflict")) continue;
        if (isPluginError(error, "not_found")) continue;
        throw storeError(error);
      }
    }
    throw new StoreUnavailableError();
  }

  /**
   * Every document of the collection that matches the filter, with its value.
   * The list has keys only, and each document comes with its own get: a page
   * of values could pass the plugin's message limit, but one document cannot
   * (the plugin refuses a write that large).
   */
  async list<T>(collection: string, filter: Attributes): Promise<Array<{ key: string; value: T; version: string; attributes: Attributes }>> {
    const pageSize = this.client.info.limits.maxPageSize;
    const items = await guard(() => this.client.state.listAll({ collection, filter, withValues: false, pageSize }));
    const out: Array<{ key: string; value: T; version: string; attributes: Attributes }> = [];
    for (let i = 0; i < items.length; i += 8) {
      const docs = await Promise.all(items.slice(i, i + 8).map((item) => this.get<T>(collection, item.key).then((doc) => ({ key: item.key, doc }))));
      // A document that went away between the list and the get is not listed.
      for (const { key, doc } of docs) if (doc) out.push({ key, value: doc.value, version: doc.version, attributes: doc.attributes });
    }
    return out;
  }

  /**
   * Refuse events that the plugin could not take, before the state changes:
   * so a call fails whole, with the reason, and a retry does not apply it twice.
   */
  checkEvents(events: NewEvent[]): void {
    if (!this.client.has("events")) return;
    const limit = this.client.info.limits.maxMessageBytes - 4096;
    for (const event of events) {
      const size = Buffer.byteLength(JSON.stringify(event), "utf8");
      if (size > limit) {
        throw new StoreRefusedError(`An event of ${size} bytes is larger than the plugin's limit of ${limit} bytes. Send less text, or put it in an attachment.`);
      }
    }
  }
}

/** Calls of one key in this process run one after the other, so they do not race each other. */
export class KeyQueue {
  private readonly tails = new Map<string, Promise<unknown>>();

  run<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const tail = this.tails.get(key) ?? Promise.resolve();
    const run = tail.then(fn, fn);
    const settled = run.catch(() => undefined);
    this.tails.set(key, settled);
    void settled.then(() => {
      if (this.tails.get(key) === settled) this.tails.delete(key);
    });
    return run;
  }
}

/** Wait ms, or less when the signal aborts. */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    }
    signal?.addEventListener("abort", done, { once: true });
  });
}

/**
 * Wait until the document's version is not `version`, at most ms: with
 * watch.wait when the plugin gives it, else by a read each second. True when
 * the wait ended at a change.
 */
export async function waitForChange(
  docs: Docs,
  where: { collection: string; key: string; version: string | null },
  ms: number,
  signal: AbortSignal,
): Promise<boolean> {
  if (ms <= 0 || signal.aborted) return false;
  const client = docs.client;
  if (client.has("watch")) {
    const timeoutMs = Math.min(ms, client.info.limits.maxWaitMs);
    try {
      const r = await client.watch.wait({ ...where, timeoutMs }, signal);
      return r.changed;
    } catch (error) {
      if (signal.aborted) return false;
      throw storeError(error);
    }
  }
  await sleep(Math.min(ms, 1000), signal);
  const doc = await docs.get(where.collection, where.key);
  return (doc?.version ?? null) !== where.version;
}
