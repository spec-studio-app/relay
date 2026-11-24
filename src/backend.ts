// What the agent hub needs from Temporal. The hub's tests without Temporal give
// no backend: the hub then speaks wire version 1 only.
import type { ConfigStore } from "./configStore.js";
import type { Mirror } from "./mirror.js";
import type { SyncInput, SyncResult } from "./workflows/spec.js";
import type { SpecAddress } from "./workflows/types.js";

export interface AgentBackend {
  /** True when the SHA-256 (hex) of an agent secret belongs to a paired Mac. */
  verifyAgent(hash: string): Promise<boolean>;
  /** A version 2 hello: keep the repos and the time. */
  hello(repos: string[], at: Date): Promise<void>;
  sync(spec: SpecAddress, input: Omit<SyncInput, "at">): Promise<SyncResult>;
}

export function temporalAgentBackend(deps: { mirror: Mirror; configStore: ConfigStore }): AgentBackend {
  return {
    verifyAgent: (hash) => deps.configStore.verifyAgent(hash),
    hello: (repos, at) => deps.configStore.setRepos(repos, at),
    sync: (spec, input) => deps.mirror.sync(spec, input),
  };
}
