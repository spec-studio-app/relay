// One long-running workflow per desk (the repo's own session): the last state
// that the agent sent and two inboxes. It has its own workflow type, so the
// list of specs (WorkflowType = specWorkflow) never shows a desk. It never
// ends by itself: it continues as new after its update limit.
import {
  ApplicationFailure,
  allHandlersFinished,
  condition,
  continueAsNew,
  defineQuery,
  defineUpdate,
  setHandler,
  upsertMemo,
  upsertSearchAttributes,
  uuid4,
  workflowInfo,
} from "@temporalio/workflow";
import { checkBound, type Bound } from "./bound.js";
import { deskWorkflowId, type DeskMessage, type DeskState } from "./types.js";
import { SA } from "./searchAttributes.js";
import { MAX_WAIT_MS, hasNewer, readCursor, type AckInput, type ReadInput, type RuleEnv, type SendInput } from "./inboxRules.js";
import {
  DESK_REFUSED_TYPE,
  applyDeskAck,
  applyDeskSend,
  applyDeskSync,
  applyReport,
  collectDeskRead,
  deskMemo,
  deskRefusal,
  type DeskReadResult,
  type DeskReportInput,
  type DeskSyncInput,
  type DeskSyncResult,
} from "./deskRules.js";

export const deskReportUpdate = defineUpdate<{ applied: boolean }, [DeskReportInput, Bound]>("report");
export const deskSendUpdate = defineUpdate<{ seq: number; message: DeskMessage }, [SendInput, Bound]>("send");
export const deskAckUpdate = defineUpdate<{ removed: boolean }, [AckInput, Bound]>("ack");
export const deskSyncUpdate = defineUpdate<DeskSyncResult, [DeskSyncInput, Bound]>("sync");
export const deskReadUpdate = defineUpdate<DeskReadResult, [ReadInput, Bound]>("read");
export const deskStateQuery = defineQuery<DeskState>("state");

export async function deskWorkflow(input: DeskState): Promise<void> {
  const s = input;
  // A start input copied from another desk's workflow: end at once.
  if (workflowInfo().workflowId !== deskWorkflowId(s.deployId, s.desk.repoKey)) return;
  let updates = 0;
  const env: RuleEnv = { newId: () => uuid4(), now: () => new Date().toISOString() };

  const publish = () => {
    const memo = deskMemo(s);
    upsertSearchAttributes([
      { key: SA.RelayDeployId, value: s.deployId },
      { key: SA.RepoKey, value: s.desk.repoKey },
      { key: SA.PendingForMac, value: memo.waitingForMac > 0 },
      { key: SA.PendingForCoordinator, value: memo.unreadForCoordinator > 0 },
    ]);
    upsertMemo({ desk: memo });
  };

  const changed = () => {
    updates++;
    publish();
  };
  const counted = <T>(out: { result: T; changed: boolean }): T => {
    if (out.changed) changed();
    return out.result;
  };

  setHandler(deskStateQuery, () => s);

  setHandler(deskReportUpdate, (report) => counted(applyReport(s, report)), {
    validator: (_: DeskReportInput, b: Bound) => checkBound(b),
  });

  setHandler(
    deskSendUpdate,
    (send) => {
      const out = applyDeskSend(s, send);
      changed();
      return out;
    },
    {
      validator: (_: SendInput, b: Bound) => {
        checkBound(b);
        const refusal = deskRefusal(s);
        if (refusal) throw ApplicationFailure.nonRetryable(refusal, DESK_REFUSED_TYPE);
      },
    },
  );

  setHandler(deskAckUpdate, (ack) => counted(applyDeskAck(s, ack, env)), { validator: (_: AckInput, b: Bound) => checkBound(b) });

  setHandler(deskSyncUpdate, (sync) => counted(applyDeskSync(s, sync, env)), {
    validator: (_: DeskSyncInput, b: Bound) => checkBound(b),
  });

  setHandler(
    deskReadUpdate,
    async (read) => {
      const after = readCursor(s, read);
      if (!hasNewer(s, after) && (read.waitMs ?? 0) > 0) {
        await condition(() => hasNewer(s, after), Math.min(read.waitMs ?? 0, MAX_WAIT_MS));
      }
      return counted(collectDeskRead(s, after));
    },
    { validator: (_: ReadInput, b: Bound) => checkBound(b) },
  );

  publish();

  await condition(() => updates >= s.updateLimit || workflowInfo().continueAsNewSuggested);
  await condition(allHandlersFinished);
  await continueAsNew<typeof deskWorkflow>(s);
}
