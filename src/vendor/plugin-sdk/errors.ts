// The errors of the protocol. A caller tells them apart by `code`, with no
// string match.

export type ErrorCode = "not_found" | "conflict" | "closing" | "dropped" | "refused" | "unavailable" | "bad_settings";

export const ERROR_CODES: readonly ErrorCode[] = ["not_found", "conflict", "closing", "dropped", "refused", "unavailable", "bad_settings"];

const RPC_CODES: Record<ErrorCode, number> = {
  not_found: -32001,
  conflict: -32002,
  closing: -32003,
  dropped: -32004,
  refused: -32005,
  unavailable: -32006,
  bad_settings: -32007,
};

export const RPC = {
  parseError: -32700,
  invalidRequest: -32600,
  methodNotFound: -32601,
  invalidParams: -32602,
  internal: -32603,
} as const;

export interface RpcErrorObject {
  code: number;
  message: string;
  data?: { code?: string; version?: string | null; reason?: string };
}

/** An error from the plugin, or from the client for a plugin that does not answer. */
export class PluginError extends Error {
  /** The plugin error code, or null for a JSON-RPC error with no plugin code. */
  readonly code: ErrorCode | null;
  /** The JSON-RPC error code. */
  readonly rpcCode: number;
  /** For a conflict: the stored version (null when no document exists). */
  readonly storedVersion: string | null | undefined;
  /** More detail for some errors, for example "handshake_required". */
  readonly reason: string | undefined;

  constructor(code: ErrorCode | null, message: string, rpcCode?: number, storedVersion?: string | null, reason?: string) {
    super(message);
    this.name = "PluginError";
    this.code = code;
    this.rpcCode = rpcCode ?? (code ? RPC_CODES[code] : RPC.internal);
    this.storedVersion = storedVersion;
    this.reason = reason;
  }

  static fromRpc(e: RpcErrorObject): PluginError {
    const word = e.data?.code;
    const code = ERROR_CODES.includes(word as ErrorCode) ? (word as ErrorCode) : null;
    return new PluginError(code, e.message, e.code, e.data && "version" in e.data ? e.data.version : undefined, e.data?.reason);
  }
}

/** True when err is a PluginError with the code. */
export function isPluginError(err: unknown, code?: ErrorCode): err is PluginError {
  return err instanceof PluginError && (code === undefined || err.code === code);
}
