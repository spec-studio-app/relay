// The Temporal spec store: one workflow per spec, with updates for the sync
// lines, the messages and the reads.
import { randomUUID } from "node:crypto";
import {
  WithStartWorkflowOperation,
  WorkflowExecutionAlreadyStartedError,
  WorkflowNotFoundError,
  WorkflowUpdateFailedError,
  type Client,
} from "@temporalio/client";
import {
  ackUpdate,
  goneUpdate,
  readUpdate,
  sendUpdate,
  specWorkflow,
  stateQuery,
  stateUpdate,
  syncUpdate,
  lastSyncLine,
  startsSpec,
  type ReadResult,
  type StateInput,
  type SyncInput,
  type SyncResult,
} from "./workflows/spec.js";
import { boundFor } from "./workflows/bound.js";
import {
  CLOSING_TEXT,
  DEFAULT_UPDATE_LIMIT,
  DROPPED_TEXT,
  FINISHED_GRACE_MS,
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
import { SpecClosingError, SpecDroppedError, SpecNotFoundError, TimeoutError } from "./store/errors.js";
import type { ListFilter, ListedSpec, SpecStore } from "./store/types.js";

export { repoKeyOf, specAddress } from "./store/address.js";
export { SpecClosingError, SpecDroppedError, SpecNotFoundError, TimeoutError } from "./store/errors.js";
export type { ListedSpec } from "./store/types.js";

/** An update that gets no answer in this time fails (for example, when no worker runs). */
export const UPDATE_TIMEOUT_MS = 30_000;

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new TimeoutError()), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function isRefusal(error: unknown, text: string): boolean {
  if (!(error instanceof WorkflowUpdateFailedError)) return false;
  const cause = (error as { cause?: unknown }).cause;
  return cause instanceof Error && cause.message.includes(text);
}

/** True when the error is the workflow validators' refusal while the spec closes. */
export function isClosingRefusal(error: unknown): boolean {
  return isRefusal(error, CLOSING_TEXT);
}

export interface MirrorOptions {
  client: Client;
  deployId: string;
  taskQueue: string;
  /** Continue as new after this many updates (tests set a small number). */
  updateLimit?: number;
  /** How long a finished spec's workflow stays open after its last update (tests set less). */
  graceMs?: number;
  now?: () => Date;
}

/** Text for a Temporal visibility query: single quotes escaped. */
function quote(text: string): string {
  return `'${text.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
}

export class Mirror implements SpecStore {
  constructor(private readonly o: MirrorOptions) {}

  get deployId(): string {
    return this.o.deployId;
  }

  private now(): string {
    return (this.o.now?.() ?? new Date()).toISOString();
  }

  /** Bind an update input to the spec's workflow (see workflows/bound.ts). */
  private bound(spec: { repoKey: string; slug: string }) {
    return boundFor(specWorkflowId(this.o.deployId, spec.repoKey, spec.slug));
  }

  private handle(spec: { repoKey: string; slug: string }) {
    return this.o.client.workflow.getHandle(specWorkflowId(this.o.deployId, spec.repoKey, spec.slug));
  }

  /** Apply a batch of activity lines as one update. The first sync starts the workflow. */
  async sync(spec: SpecAddress, input: Omit<SyncInput, "at">): Promise<SyncResult> {
    try {
      return await this.syncWith(spec, input, "ALLOW_DUPLICATE_FAILED_ONLY");
    } catch (error) {
      if (isClosingRefusal(error)) {
        // The first sync of a new spec with the same slug: the old run ends now,
        // so the agent must send the batch again (then it starts a new run).
        if (startsSpec(input)) throw new SpecClosingError();
        // The finished spec's workflow is ending: the lines change nothing more.
        return { line: lastSyncLine(input), applied: false };
      }
      if (!(error instanceof WorkflowExecutionAlreadyStartedError)) throw error;
    }
    // The spec's workflow completed. A first sync is a new spec with the same
    // slug: start a new run for it. Later lines belong to the completed spec
    // and change nothing more, so the agent may drop them.
    if (!startsSpec(input)) return { line: lastSyncLine(input), applied: false };
    try {
      return await this.syncWith(spec, input, "ALLOW_DUPLICATE");
    } catch (error) {
      if (isClosingRefusal(error)) throw new SpecClosingError();
      throw error;
    }
  }

  private syncWith(
    spec: SpecAddress,
    input: Omit<SyncInput, "at">,
    workflowIdReusePolicy: "ALLOW_DUPLICATE_FAILED_ONLY" | "ALLOW_DUPLICATE",
  ): Promise<SyncResult> {
    const startWorkflowOperation = new WithStartWorkflowOperation(specWorkflow, {
      workflowId: specWorkflowId(this.o.deployId, spec.repoKey, spec.slug),
      taskQueue: this.o.taskQueue,
      args: [
        initialSpecState(this.o.deployId, spec, this.o.updateLimit ?? DEFAULT_UPDATE_LIMIT, this.o.graceMs ?? FINISHED_GRACE_MS),
      ],
      workflowIdConflictPolicy: "USE_EXISTING",
      // By default a spec whose workflow completed (merged or dropped, then
      // ended) gets no new empty run for a late line. A failed or terminated
      // run may restart.
      workflowIdReusePolicy,
    });
    return withTimeout(
      this.o.client.workflow.executeUpdateWithStart(syncUpdate, {
        args: [{ ...input, repo: input.repo ?? spec.repo, at: this.now() }, this.bound(spec)],
        startWorkflowOperation,
      }),
      UPDATE_TIMEOUT_MS,
    );
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

  private async update<T>(spec: { repoKey: string; slug: string }, run: () => Promise<T>, ms = UPDATE_TIMEOUT_MS): Promise<T> {
    try {
      return await withTimeout(run(), ms);
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) throw new SpecNotFoundError();
      if (isClosingRefusal(error)) throw new SpecClosingError();
      if (isRefusal(error, DROPPED_TEXT)) throw new SpecDroppedError();
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
      this.handle(spec).executeUpdate(sendUpdate, { args: [{ ...input, id: randomUUID(), at: this.now() }, this.bound(spec)] }),
    );
  }

  /**
   * The agent's gone frame: the spec no longer exists on the Mac. The workflow
   * marks it dropped and ends like a merged spec. A spec with no workflow, or
   * one that ends now, needs nothing more.
   */
  async gone(spec: { repoKey: string; slug: string }): Promise<void> {
    try {
      await this.update(spec, () => this.handle(spec).executeUpdate(goneUpdate, { args: [{ at: this.now() }, this.bound(spec)] }));
    } catch (error) {
      if (error instanceof SpecNotFoundError || error instanceof SpecClosingError) return;
      throw error;
    }
  }

  /**
   * The agent's state frame: replace the state and the turn of a running spec
   * that is not dropped (a merged spec in its grace period takes it too). An
   * unknown spec gets no new run; a dropped or closing run ignores it. True
   * when the workflow applied it.
   */
  async refresh(spec: { repoKey: string; slug: string }, input: Omit<StateInput, "at">): Promise<boolean> {
    try {
      const result = await this.update(spec, () =>
        this.handle(spec).executeUpdate(stateUpdate, { args: [{ ...input, at: this.now() }, this.bound(spec)] }),
      );
      return result.applied;
    } catch (error) {
      if (error instanceof SpecNotFoundError || error instanceof SpecClosingError) return false;
      throw error;
    }
  }

  /** The agent wrote the message to disk (or refused it with an error). */
  ack(spec: { repoKey: string; slug: string }, seq: number, error?: string): Promise<{ removed: boolean }> {
    return this.update(spec, () =>
      this.handle(spec).executeUpdate(ackUpdate, { args: [{ seq, ...(error ? { error } : {}) }, this.bound(spec)] }),
    );
  }

  /** Messages for Claude AI after the cursor. Waits up to waitMs for a new one. */
  read(
    spec: { repoKey: string; slug: string },
    after: number | undefined,
    waitMs: number,
    unreadOnly = false,
  ): Promise<ReadResult> {
    return this.update(
      spec,
      () => this.handle(spec).executeUpdate(readUpdate, { args: [{ after, waitMs, unreadOnly }, this.bound(spec)] }),
      waitMs + UPDATE_TIMEOUT_MS,
    );
  }

  /**
   * The specs of this deploy from visibility. Search attributes are eventually
   * consistent, so a read of one spec queries its workflow instead.
   */
  async list(filter: ListFilter = {}): Promise<ListedSpec[]> {
    const parts = [`WorkflowType = ${quote(SPEC_WORKFLOW_TYPE)}`, `RelayDeployId = ${quote(this.o.deployId)}`];
    if (!filter.all) parts.push(`ExecutionStatus = 'Running'`);
    if (filter.openOnly) parts.push(`Stage != 'merged'`);
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

