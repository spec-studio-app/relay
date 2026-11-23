// Structured logs. Callers pass only safe fields: method, path, status, tool,
// duration and event names. Never pass a secret, a password, an auth code, a
// token or a header value.
export type LogEntry = Record<string, string | number | boolean | undefined>;
export type Logger = (entry: LogEntry) => void;

export const consoleLogger: Logger = (entry) => {
  process.stdout.write(JSON.stringify({ time: new Date().toISOString(), ...entry }) + "\n");
};
