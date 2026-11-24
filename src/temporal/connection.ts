// The connection to Temporal and the two clients: one with the data key for
// the spec workflows, one with the config key for the relay-config workflow.
import { Client, Connection, type ConnectionOptions } from "@temporalio/client";
import { NativeConnection, type NativeConnectionOptions } from "@temporalio/worker";
import type { PayloadKeys, TemporalSettings } from "../config.js";
import { configCodec, dataCodec } from "../codec.js";

export interface TemporalClients {
  /** Spec workflows: payloads use the data key. */
  data: Client;
  /** The relay-config workflow: payloads use the config key. */
  config: Client;
}

function tlsOptions(t: TemporalSettings): ConnectionOptions["tls"] {
  switch (t.auth) {
    case "plain":
      return undefined;
    case "tls":
    case "api-key":
      return true;
    case "mtls":
      return { clientCertPair: { crt: Buffer.from(t.tlsCert!), key: Buffer.from(t.tlsKey!) } };
  }
}

function connectionOptions(t: TemporalSettings) {
  return {
    address: t.address,
    tls: tlsOptions(t),
    ...(t.auth === "api-key" ? { apiKey: t.apiKey, metadata: { "temporal-namespace": t.namespace } } : {}),
  };
}

export function connectClient(t: TemporalSettings): Promise<Connection> {
  return Connection.connect(connectionOptions(t) as ConnectionOptions);
}

export function connectNative(t: TemporalSettings): Promise<NativeConnection> {
  return NativeConnection.connect(connectionOptions(t) as NativeConnectionOptions);
}

export function makeClients(connection: Connection, namespace: string, keys: PayloadKeys): TemporalClients {
  return {
    data: new Client({ connection, namespace, dataConverter: { payloadCodecs: [dataCodec(keys)] } }),
    config: new Client({ connection, namespace, dataConverter: { payloadCodecs: [configCodec(keys)] } }),
  };
}

/** The task queue of the relay-config workflow. */
export function configTaskQueue(taskQueue: string): string {
  return `${taskQueue}-config`;
}
