// The Temporal desk store: one workflow per desk (deskWorkflow), with updates
// for the agent's desk state, the messages, the reply lines and the reads.
import { randomUUID } from "node:crypto";
import {
  ApplicationFailure,
  WithStartWorkflowOperation,
  WorkflowNotFoundError,
  WorkflowUpdateFailedError,
  type Client,
} from "@temporalio/client";
import {
  deskAckUpdate,
  deskReadUpdate,
  deskReportUpdate,
  deskSendUpdate,
  deskStateQuery,
  deskSyncUpdate,
  deskWorkflow,
} from "./workflows/desk.js";
import { boundFor } from "./workflows/bound.js";
import {
  DEFAULT_UPDATE_LIMIT,
  deskWorkflowId,
  initialDeskState,
  type Attachment,
  type DeskAddress,
  type DeskMemo,
  type DeskMessage,
  type DeskReport,
  type DeskState,
} from "./workflows/types.js";
import { DESK_REFUSED_TYPE, type DeskListFilter, type DeskReadResult, type DeskSyncLine, type DeskSyncResult } from "./workflows/deskRules.js";
import { DESK_WORKFLOW_TYPE, SA } from "./workflows/searchAttributes.js";
import { DeskNotFoundError, DeskRefusedError, TimeoutError } from "./store/errors.js";
import type { DeskStore, ListedDesk } from "./store/types.js";
import { UPDATE_TIMEOUT_MS } from "./mirror.js";

export interface DeskMirrorOptions {
  client: Client;
  deployId: string;
  taskQueue: string;
  /** Continue as new after this many updates (tests set a small number). */
  updateLimit?: number;
  now?: () => Date;
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new TimeoutError()), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function quote(text: string): string {
  return `'${text.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
}

export class DeskMirror implements DeskStore {
  constructor(private readonly o: DeskMirrorOptions) {}

  private now(): string {
    return (this.o.now?.() ?? new Date()).toISOString();
  }

  private id(repoKey: string): string {
    return deskWorkflowId(this.o.deployId, repoKey);
  }

  private handle(desk: { repoKey: string }) {
    return this.o.client.workflow.getHandle(this.id(desk.repoKey));
  }

  private bound(desk: { repoKey: string }) {
    return boundFor(this.id(desk.repoKey));
  }

  /** The start of a desk's workflow, for an update that makes the desk when it has none. */
  private startOperation(desk: DeskAddress) {
    return new WithStartWorkflowOperation(deskWorkflow, {
      workflowId: this.id(desk.repoKey),
      taskQueue: this.o.taskQueue,
      args: [initialDeskState(this.o.deployId, desk, this.o.updateLimit ?? DEFAULT_UPDATE_LIMIT)],
      workflowIdConflictPolicy: "USE_EXISTING",
      // A desk's workflow never completes by itself; a failed or terminated run may start again.
      workflowIdReusePolicy: "ALLOW_DUPLICATE",
    });
  }

  async report(report: DeskReport): Promise<void> {
    const desk = { repoKey: report.repoKey, repo: report.repo };
    const input = { repo: report.repo, open: report.open, turn: report.turn, canMessage: report.canMessage, reason: report.reason };
    await withTimeout(
      this.o.client.workflow.executeUpdateWithStart(deskReportUpdate, {
        args: [{ ...input, at: this.now() }, this.bound(desk)],
        startWorkflowOperation: this.startOperation(desk),
      }),
      UPDATE_TIMEOUT_MS,
    );
  }

  async sync(desk: DeskAddress, lines: DeskSyncLine[]): Promise<DeskSyncResult> {
    return withTimeout(
      this.o.client.workflow.executeUpdateWithStart(deskSyncUpdate, {
        args: [{ lines, at: this.now(), ...(desk.repo ? { repo: desk.repo } : {}) }, this.bound(desk)],
        startWorkflowOperation: this.startOperation(desk),
      }),
      UPDATE_TIMEOUT_MS,
    );
  }

  async get(desk: { repoKey: string }): Promise<DeskState | null> {
    try {
      return await this.handle(desk).query(deskStateQuery);
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) return null;
      throw error;
    }
  }

  private async update<T>(run: () => Promise<T>, ms = UPDATE_TIMEOUT_MS): Promise<T> {
    try {
      return await withTimeout(run(), ms);
    } catch (error) {
      if (error instanceof WorkflowNotFoundError) throw new DeskNotFoundError();
      const cause = (error as { cause?: unknown }).cause;
      if (error instanceof WorkflowUpdateFailedError && cause instanceof ApplicationFailure && cause.type === DESK_REFUSED_TYPE) {
        throw new DeskRefusedError(cause.message);
      }
      if (cause instanceof WorkflowNotFoundError) throw new DeskNotFoundError();
      if (/not found|already completed/i.test(String((error as Error).message))) throw new DeskNotFoundError();
      throw error;
    }
  }

  send(
    desk: { repoKey: string },
    input: { text: string; replyTo?: string; attachments?: Attachment[] },
  ): Promise<{ seq: number; message: DeskMessage }> {
    return this.update(() =>
      this.handle(desk).executeUpdate(deskSendUpdate, { args: [{ ...input, id: randomUUID(), at: this.now() }, this.bound(desk)] }),
    );
  }

  ack(desk: { repoKey: string }, seq: number, error?: string): Promise<{ removed: boolean }> {
    return this.update(() =>
      this.handle(desk).executeUpdate(deskAckUpdate, { args: [{ seq, ...(error ? { error } : {}) }, this.bound(desk)] }),
    );
  }

  read(desk: { repoKey: string }, after: number | undefined, waitMs: number, unreadOnly = false): Promise<DeskReadResult> {
    return this.update(
      () => this.handle(desk).executeUpdate(deskReadUpdate, { args: [{ after, waitMs, unreadOnly }, this.bound(desk)] }),
      waitMs + UPDATE_TIMEOUT_MS,
    );
  }

  /** The desks of this deploy from visibility (eventually consistent). */
  async list(filter: DeskListFilter = {}): Promise<ListedDesk[]> {
    const parts = [
      `WorkflowType = ${quote(DESK_WORKFLOW_TYPE)}`,
      `RelayDeployId = ${quote(this.o.deployId)}`,
      `ExecutionStatus = 'Running'`,
    ];
    if (filter.repoKey) parts.push(`RepoKey = ${quote(filter.repoKey)}`);
    if (filter.pendingForMac) parts.push(`PendingForMac = true`);
    if (filter.pendingForCoordinator) parts.push(`PendingForCoordinator = true`);
    const out: ListedDesk[] = [];
    const seen = new Set<string>();
    for await (const wf of this.o.client.workflow.list({ query: parts.join(" AND ") })) {
      if (seen.has(wf.workflowId)) continue;
      seen.add(wf.workflowId);
      const memo = wf.memo?.desk as DeskMemo | undefined;
      if (!memo) continue;
      out.push({ repoKey: String(wf.typedSearchAttributes.get(SA.RepoKey) ?? ""), memo });
    }
    return out;
  }
}
