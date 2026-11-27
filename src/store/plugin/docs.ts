// The documents of the plugin store: a read, the rule, and a write with the
// read version (PROTOCOL.md section 14). On a conflict the relay reads again
// and applies the rule again, at most MAX_TRIES times. The plugin's errors
// become the relay's store errors here, in one place.
import { isPluginError, type Attributes, type Json, type PluginClient } from "../../vendor/plugin-sdk/index.js";
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
  return error;
}

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

  /** Every document of the collection that matches the filter, with its value. */
  async list<T>(collection: string, filter: Attributes): Promise<Array<{ key: string; value: T; version: string; attributes: Attributes }>> {
    const items = await guard(() => this.client.state.listAll({ collection, filter, withValues: true }));
    return items.map((item) => ({ key: item.key, value: item.value as T, version: item.version, attributes: item.attributes }));
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
