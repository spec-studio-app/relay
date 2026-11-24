// The relay's side of the spec workflows: sync lines, messages and reads.
import { createHash, randomUUID } from "node:crypto";
import { WithStartWorkflowOperation, WorkflowNotFoundError, type Client } from "@temporalio/client";
import {
  ackUpdate,
  readUpdate,
  sendUpdate,
  specWorkflow,
  stateQuery,
  syncUpdate,
  type ReadResult,
  type SyncInput,
  type SyncResult,
} from "./workflows/spec.js";
import {
  DEFAULT_UPDATE_LIMIT,
  SA as SAKeys,
  SPEC_WORKFLOW_TYPE,
  initialSpecState,
  specWorkflowId,
  type Attachment,
  type Message,
  type SpecAddress,
  type SpecMemo,
  type SpecWorkflowState,
} from "./workflows/types.js";

/** The first 12 hex characters of the SHA-256 of the home repo's path. */
export function repoKeyOf(repoPath: string): string {
  const path = repoPath.length > 1 ? repoPath.replace(/\/+$/, "") : repoPath;
  return createHash("sha256").update(path, "utf8").digest("hex").slice(0, 12);
}

export function specAddress(repo: string, slug: string): SpecAddress {
  return { repoKey: repoKeyOf(repo), slug, repo };
}

export class SpecNotFoundError extends Error {
  constructor() {
    super("The relay has no synced spec with this repo and slug.");
  }
}

export interface ListedSpec {
  repoKey: string;
  slug: string;
  running: boolean;
  memo: SpecMemo | null;
}

export interface MirrorOptions {
  client: Client;
  deployId: string;
  taskQueue: string;
  /** Continue as new after this many updates (tests set a small number). */
  updateLimit?: number;
  now?: () => Date;
}

/** Text for a Temporal visibility query: single quotes escaped. */
function quote(text: string): string {
  return `'${text.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
}

export class Mirror {
  constructor(private readonly o: MirrorOptions) {}

  get deployId(): string {
    return this.o.deployId;
  }

  private now(): string {
    return (this.o.now?.() ?? new Date()).toISOString();
  }

  private handle(spec: { repoKey: string; slug: string }) {
    return this.o.client.workflow.getHandle(specWorkflowId(spec.repoKey, spec.slug));
  }

  /** Apply one activity line. The first line starts the workflow. */
  async sync(spec: SpecAddress, input: Omit<SyncInput, "at">): Promise<SyncResult> {
    const startWorkflowOperation = new WithStartWorkflowOperation(specWorkflow, {
      workflowId: specWorkflowId(spec.repoKey, spec.slug),
      taskQueue: this.o.taskQueue,
      args: [initialSpecState(this.o.deployId, spec, this.o.updateLimit ?? DEFAULT_UPDATE_LIMIT)],
      workflowIdConflictPolicy: "USE_EXISTING",
    });
    return this.o.client.workflow.executeUpdateWithStart(syncUpdate, {
      args: [{ ...input, repo: input.repo ?? spec.repo, at: this.now() }],
      startWorkflowOperation,
    });
  }

  /** The workflow's state, or null when the spec has no running workflow. */
  async state(spec: { repoKey: string; slug: string }): Promise<SpecWorkflowState | null> {
    try {
      return await this.handle(spec).query(stateQuery);
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) return null;
      throw error;
    }
  }

  private async update<T>(spec: { repoKey: string; slug: string }, run: () => Promise<T>): Promise<T> {
    try {
      return await run();
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) throw new SpecNotFoundError();
      const cause = (error as { cause?: unknown }).cause;
      if (cause instanceof WorkflowNotFoundError) throw new SpecNotFoundError();
      if (/not found|already completed/i.test(String((error as Error).message))) throw new SpecNotFoundError();
      throw error;
    }
  }

  /** Add a message from Claude AI to the spec's Mac inbox. */
  send(
    spec: { repoKey: string; slug: string },
    input: { text: string; replyTo?: string; attachments?: Attachment[] },
  ): Promise<{ seq: number; message: Message }> {
    return this.update(spec, () =>
      this.handle(spec).executeUpdate(sendUpdate, { args: [{ ...input, id: randomUUID(), at: this.now() }] }),
    );
  }

  /** The agent wrote the message to disk (or refused it with an error). */
  ack(spec: { repoKey: string; slug: string }, seq: number, error?: string): Promise<{ removed: boolean }> {
    return this.update(spec, () =>
      this.handle(spec).executeUpdate(ackUpdate, { args: [{ seq, ...(error ? { error } : {}) }] }),
    );
  }

  /** Messages for Claude AI after the cursor. Waits up to waitMs for a new one. */
  read(spec: { repoKey: string; slug: string }, after: number | undefined, waitMs: number): Promise<ReadResult> {
    return this.update(spec, () => this.handle(spec).executeUpdate(readUpdate, { args: [{ after, waitMs }] }));
  }

  /**
   * The specs of this deploy from visibility. Search attributes are eventually
   * consistent, so a read of one spec queries its workflow instead.
   */
  async list(filter: {
    repoKey?: string;
    all?: boolean;
    pendingForMac?: boolean;
    pendingForCoordinator?: boolean;
  } = {}): Promise<ListedSpec[]> {
    const parts = [`WorkflowType = ${quote(SPEC_WORKFLOW_TYPE)}`, `RelayDeployId = ${quote(this.o.deployId)}`];
    if (!filter.all) parts.push(`ExecutionStatus = 'Running'`);
    if (filter.repoKey) parts.push(`RepoKey = ${quote(filter.repoKey)}`);
    if (filter.pendingForMac) parts.push(`PendingForMac = true`);
    if (filter.pendingForCoordinator) parts.push(`PendingForCoordinator = true`);
    const seen = new Set<string>();
    const out: ListedSpec[] = [];
    for await (const wf of this.o.client.workflow.list({ query: parts.join(" AND ") })) {
      // With all=true, a spec can have a closed run and a new run: keep the newest.
      if (seen.has(wf.workflowId)) continue;
      seen.add(wf.workflowId);
      const sa = wf.typedSearchAttributes;
      const repoKey = String(sa.get(SAKeys.RepoKey) ?? "");
      const slug = String(sa.get(SAKeys.SpecSlug) ?? "");
      const memo = (wf.memo?.mirror as SpecMemo | undefined) ?? null;
      out.push({ repoKey, slug, running: wf.status.name === "RUNNING", memo });
    }
    return out;
  }
}

