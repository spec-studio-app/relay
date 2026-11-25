// The checks at start of the Temporal store: the namespace, the search
// attributes, the bucket and the relay-config workflow. Each failure tells the
// owner what to correct. The bucket check and the pairing text serve both stores.
import { newPairCode, pairCodeHash, PAIR_CODE_TTL_MS } from "./pairing.js";
import { WorkflowExecutionAlreadyStartedError, type Client, type Connection } from "@temporalio/client";
import proto from "@temporalio/proto";
import type { temporal as protoTypes } from "@temporalio/proto";
import Long from "long";

const { temporal } = proto;
type IndexedValueType = protoTypes.api.enums.v1.IndexedValueType;
import type { SearchAttributeType } from "@temporalio/common";
import type { Config, TemporalSettings } from "./config.js";
import type { Bucket } from "./bucket.js";
import type { Logger } from "./log.js";
import { configTaskQueue } from "./temporal/connection.js";
import { relayConfigWorkflow, setPairCodeUpdate } from "./workflows/relayConfig.js";
import { boundFor } from "./workflows/bound.js";
import { initialRelayConfigState, relayConfigWorkflowId } from "./workflows/types.js";
import { SEARCH_ATTRIBUTE_KEYS } from "./workflows/searchAttributes.js";


export const NAMESPACE_RETENTION_DAYS = 30;
export { newPairCode, pairCodeHash, PAIR_CODE_TTL_MS } from "./pairing.js";
export { StartupError, checkBucket, newPairCodeIn, pairingText } from "./startupCommon.js";
import { StartupError, checkBucket, pairingText } from "./startupCommon.js";

/** The search attribute type names that Temporal uses. */
const TYPE_NAMES: Record<SearchAttributeType, string> = {
  TEXT: "Text",
  KEYWORD: "Keyword",
  INT: "Int",
  DOUBLE: "Double",
  BOOL: "Bool",
  DATETIME: "Datetime",
  KEYWORD_LIST: "KeywordList",
};

const INDEXED: Record<string, IndexedValueType> = {
  Text: temporal.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_TEXT,
  Keyword: temporal.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
  Bool: temporal.api.enums.v1.IndexedValueType.INDEXED_VALUE_TYPE_BOOL,
};

export const REQUIRED_SEARCH_ATTRIBUTES: Array<{ name: string; type: string }> = SEARCH_ATTRIBUTE_KEYS.map((k) => ({
  name: k.name,
  type: TYPE_NAMES[k.type],
}));

/** The Temporal calls that the checks need. Tests give a fake. */
export interface TemporalAdmin {
  namespaceExists(namespace: string): Promise<boolean>;
  registerNamespace(namespace: string, retentionDays: number): Promise<void>;
  /** The names of the custom search attributes of the namespace. */
  searchAttributes(namespace: string): Promise<Set<string>>;
  addSearchAttributes(namespace: string, attributes: Array<{ name: string; type: string }>): Promise<void>;
}

function grpcCode(error: unknown): number | undefined {
  return (error as { code?: number }).code;
}
const NOT_FOUND = 5;

export function temporalAdmin(connection: Connection): TemporalAdmin {
  return {
    async namespaceExists(namespace) {
      try {
        await connection.workflowService.describeNamespace({ namespace });
        return true;
      } catch (error) {
        if (grpcCode(error) === NOT_FOUND) return false;
        throw error;
      }
    },
    async registerNamespace(namespace, retentionDays) {
      await connection.workflowService.registerNamespace({
        namespace,
        workflowExecutionRetentionPeriod: { seconds: Long.fromNumber(retentionDays * 24 * 60 * 60) },
      });
    },
    async searchAttributes(namespace) {
      const out = await connection.operatorService.listSearchAttributes({ namespace });
      return new Set(Object.keys(out.customAttributes ?? {}));
    },
    async addSearchAttributes(namespace, attributes) {
      const searchAttributes: Record<string, IndexedValueType> = {};
      for (const a of attributes) searchAttributes[a.name] = INDEXED[a.type];
      await connection.operatorService.addSearchAttributes({ namespace, searchAttributes });
    },
  };
}

export interface StartupOptions {
  config: Config;
  admin: TemporalAdmin;
  /** The client with the config key. */
  configClient: Client;
  bucket: Bucket | null;
  log: Logger;
  /** The console for the owner: the pairing code and the instructions go here, never to the log. */
  print: (text: string) => void;
  /** Wait between tries while a new namespace becomes ready. */
  retryDelayMs?: number;
}

export interface StartupResult {
  /** The pairing code, when this start made the relay-config workflow. */
  pairCode: string | null;
}


const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * True for Temporal Cloud: an API key, or an address at *.tmprl.cloud (mTLS or
 * API key). Temporal Cloud does not let the relay register a namespace or
 * search attributes.
 */
export function isTemporalCloud(t: TemporalSettings): boolean {
  if (t.auth === "api-key") return true;
  const host = t.address.replace(/^[a-z]+:\/\//i, "").replace(/:\d+$/, "").replace(/\.$/, "").toLowerCase();
  return host === "tmprl.cloud" || host.endsWith(".tmprl.cloud");
}

function missingAttributesText(namespace: string, missing: Array<{ name: string; type: string }>, cloud: boolean): string {
  return [
    `The namespace ${namespace} does not have these search attributes:`,
    ...missing.map((a) => `  ${a.name} (${a.type})`),
    ...(cloud
      ? [
          `Add them in Temporal Cloud (Namespace > Search attributes), or with tcld:`,
          ...missing.map((a) => `  tcld namespace search-attributes add -n ${namespace} --sa "${a.name}=${a.type}"`),
        ]
      : [
          `Add them with the temporal CLI:`,
          ...missing.map((a) => `  temporal operator search-attribute create --namespace ${namespace} --name ${a.name} --type ${a.type}`),
        ]),
    `Then start the relay again.`,
  ].join("\n");
}

/** The Temporal settings of a Temporal-store config. */
function temporalOf(config: Config): TemporalSettings {
  if (!config.temporal) throw new StartupError("The relay has no Temporal settings. Set TEMPORAL_ADDRESS, or use RELAY_STORE=json.");
  return config.temporal;
}

async function checkNamespace(o: StartupOptions): Promise<void> {
  const { namespace } = temporalOf(o.config);
  let exists: boolean;
  try {
    exists = await o.admin.namespaceExists(namespace);
  } catch {
    throw new StartupError("The relay cannot reach Temporal. Check TEMPORAL_ADDRESS and the Temporal auth settings.");
  }
  if (exists) return;
  if (isTemporalCloud(temporalOf(o.config))) {
    throw new StartupError(
      `The Temporal namespace ${namespace} does not exist. Make it in Temporal Cloud, or correct TEMPORAL_NAMESPACE.`,
    );
  }
  try {
    await o.admin.registerNamespace(namespace, NAMESPACE_RETENTION_DAYS);
  } catch {
    throw new StartupError(`The relay cannot register the Temporal namespace ${namespace}. Register it, or correct TEMPORAL_NAMESPACE.`);
  }
  o.log({ event: "namespace_registered", namespace, retentionDays: NAMESPACE_RETENTION_DAYS });
}

async function checkSearchAttributes(o: StartupOptions): Promise<void> {
  const { namespace } = temporalOf(o.config);
  const delay = o.retryDelayMs ?? 1000;
  let have: Set<string> | null = null;
  // A namespace that was just registered can take some seconds to be ready.
  for (let tries = 0; have === null; tries++) {
    try {
      have = await o.admin.searchAttributes(namespace);
    } catch {
      if (tries >= 30) throw new StartupError(`The relay cannot read the search attributes of the namespace ${namespace}.`);
      await sleep(delay);
    }
  }
  const missing = REQUIRED_SEARCH_ATTRIBUTES.filter((a) => !have!.has(a.name));
  if (missing.length === 0) return;
  if (isTemporalCloud(temporalOf(o.config))) {
    o.print(missingAttributesText(namespace, missing, true));
    throw new StartupError(`The namespace ${namespace} does not have the search attributes of the relay.`);
  }
  try {
    await o.admin.addSearchAttributes(namespace, missing);
  } catch {
    o.print(missingAttributesText(namespace, missing, false));
    throw new StartupError(`The relay cannot register the search attributes in the namespace ${namespace}.`);
  }
  o.log({ event: "search_attributes_registered", count: missing.length });
}


async function ensureRelayConfig(o: StartupOptions): Promise<string | null> {
  const code = newPairCode();
  const pairCode = { hash: pairCodeHash(code), expiresAt: Date.now() + PAIR_CODE_TTL_MS };
  try {
    await o.configClient.workflow.start(relayConfigWorkflow, {
      workflowId: relayConfigWorkflowId(o.config.deployId),
      taskQueue: configTaskQueue(temporalOf(o.config).taskQueue),
      args: [initialRelayConfigState(o.config.deployId, pairCode)],
      workflowIdConflictPolicy: "FAIL",
    });
  } catch (error) {
    if (error instanceof WorkflowExecutionAlreadyStartedError) return null;
    throw new StartupError("The relay cannot start its relay-config workflow.");
  }
  o.log({ event: "relay_config_started" });
  o.print(pairingText(o.config, code));
  return code;
}

export async function runStartupChecks(o: StartupOptions): Promise<StartupResult> {
  await checkNamespace(o);
  await checkSearchAttributes(o);
  await checkBucket(o);
  const pairCode = await ensureRelayConfig(o);
  return { pairCode };
}


/** Make a new pairing code in the relay-config workflow (Temporal). */
export async function makePairCode(configClient: Client, deployId: string, timeoutMs = 15_000): Promise<string> {
  const code = newPairCode();
  const handle = configClient.workflow.getHandle(relayConfigWorkflowId(deployId));
  // The relay's own worker applies the update: without a running relay, no answer comes.
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error("timeout")), timeoutMs);
  });
  try {
    await Promise.race([
      handle.executeUpdate(setPairCodeUpdate, {
        args: [{ hash: pairCodeHash(code), expiresAt: Date.now() + PAIR_CODE_TTL_MS }, boundFor(relayConfigWorkflowId(deployId))],
      }),
      timeout,
    ]);
  } finally {
    clearTimeout(timer);
  }
  return code;
}
