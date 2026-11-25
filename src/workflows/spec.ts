// One long-running workflow per spec: the mirror of its state and two inboxes.
import {
  allHandlersFinished,
  condition,
  continueAsNew,
  defineQuery,
  patched,
  defineUpdate,
  proxyActivities,
  setHandler,
  upsertMemo,
  upsertSearchAttributes,
  uuid4,
  workflowInfo,
} from "@temporalio/workflow";
import type { Activities } from "../activities.js";
import { checkBound, type Bound } from "./bound.js";
import { CLOSING_TEXT, FINISHED_GRACE_MS, SA, specWorkflowId, type Message, type SpecWorkflowState } from "./types.js";
import {
  MAX_WAIT_MS,
  applyAck,
  applyGone,
  applySend,
  applyState,
  applySync,
  checkSend,
  collectRead,
  hasNewer,
  isDone,
  readCursor,
  specMemo,
  type AckInput,
  type ReadInput,
  type ReadResult,
  type RuleEnv,
  type SendInput,
  type StateInput,
  type SyncInput,
  type SyncResult,
} from "./specRules.js";

export {
  lastSyncLine,
  startsSpec,
  syncLines,
  type AckInput,
  type ReadInput,
  type ReadResult,
  type SendInput,
  type StateInput,
  type SyncInput,
  type SyncLine,
  type SyncResult,
} from "./specRules.js";

export const syncUpdate = defineUpdate<SyncResult, [SyncInput, Bound]>("sync");
export const sendUpdate = defineUpdate<{ seq: number; message: Message }, [SendInput, Bound]>("send");
export const ackUpdate = defineUpdate<{ removed: boolean }, [AckInput, Bound]>("ack");
export const readUpdate = defineUpdate<ReadResult, [ReadInput, Bound]>("read");
/**
 * The agent's state frame: the state and the turn changed with no new activity
 * line (for example, the turn moved). It replaces them; the line cursor and the
 * events stay. A dropped run ignores it; a merged run in its grace period takes it.
 */
export const stateUpdate = defineUpdate<{ applied: boolean }, [StateInput, Bound]>("state");

/** The agent's gone frame: the spec no longer exists on the Mac. */
export const goneUpdate = defineUpdate<{ dropped: boolean }, [{ at: string }, Bound]>("gone");
export const stateQuery = defineQuery<SpecWorkflowState>("state");

const { removeAttachments } = proxyActivities<Activities>({
  startToCloseTimeout: "1 minute",
  retry: { maximumAttempts: 10 },
});

export async function specWorkflow(input: SpecWorkflowState): Promise<void> {
  const s = input;
  // A start input copied from another spec's workflow: end at once.
  if (workflowInfo().workflowId !== specWorkflowId(s.deployId, s.spec.repoKey, s.spec.slug)) return;
  let updates = 0;
  const env: RuleEnv = { newId: () => uuid4(), now: () => new Date().toISOString() };

  const publish = () => {
    const memo = specMemo(s);
    upsertSearchAttributes([
      { key: SA.RelayDeployId, value: s.deployId },
      { key: SA.RepoKey, value: s.spec.repoKey },
      { key: SA.SpecSlug, value: s.spec.slug },
      { key: SA.SpecTitle, value: memo.title },
      { key: SA.Stage, value: memo.stage },
      { key: SA.Status, value: memo.status },
      { key: SA.Mode, value: memo.mode },
      { key: SA.NeedsYou, value: memo.turn?.needsYou ?? false },
      { key: SA.PendingForMac, value: memo.waitingForMac > 0 },
      { key: SA.PendingForCoordinator, value: memo.unreadForCoordinator > 0 },
    ]);
    upsertMemo({ mirror: memo });
  };

  /** Counts every update, so the grace period of a finished spec starts again. */
  let activity = 0;
  /** True when the workflow removes the attachments and ends: the validators refuse updates. */
  let closing = false;
  /** Every validator: the input is bound to this workflow (bound.ts), and the workflow is not closing. */
  const open = (b: Bound) => {
    checkBound(b);
    if (closing) throw new Error(CLOSING_TEXT);
  };

  const changed = () => {
    updates++;
    activity++;
    publish();
  };
  /** Count the change of a rule, and give its result. */
  const counted = <T>(out: { result: T; changed: boolean }): T => {
    if (out.changed) changed();
    return out.result;
  };

  setHandler(stateQuery, () => s);

  setHandler(syncUpdate, (sync) => counted(applySync(s, sync, env)), { validator: (_: SyncInput, b: Bound) => open(b) });

  setHandler(sendUpdate, (send) => {
    const out = applySend(s, send);
    changed();
    return out;
  }, {
    validator: (_: SendInput, b: Bound) => {
      open(b);
      checkSend(s);
    },
  });

  setHandler(ackUpdate, (ack) => counted(applyAck(s, ack, env)), { validator: (_: AckInput, b: Bound) => open(b) });

  // A merged spec in its grace period takes a new state too. Histories from
  // before this rule keep the old one (the patch marker).
  setHandler(stateUpdate, (input) => counted(applyState(s, input, () => patched("state-on-finished-run"))), {
    validator: (_: StateInput, b: Bound) => open(b),
  });

  setHandler(goneUpdate, (gone) => counted(applyGone(s, gone, env)), { validator: (_: { at: string }, b: Bound) => open(b) });

  setHandler(readUpdate, async (read) => {
    activity++;
    const after = readCursor(s, read);
    if (!hasNewer(s, after) && (read.waitMs ?? 0) > 0) {
      await condition(() => hasNewer(s, after), Math.min(read.waitMs ?? 0, MAX_WAIT_MS));
    }
    return counted(collectRead(s, after));
  }, { validator: (_: ReadInput, b: Bound) => open(b) });

  publish();

  const done = () => isDone(s);
  const tooLong = () => updates >= s.updateLimit || workflowInfo().continueAsNewSuggested;

  for (;;) {
    await condition(() => done() || tooLong());
    if (!tooLong()) {
      // A finished spec with empty inboxes: wait the grace period. Any update
      // in it (a read, a late line, a message) starts it again.
      const seen = activity;
      const interrupted = await condition(() => activity !== seen || !done() || tooLong(), s.graceMs ?? FINISHED_GRACE_MS);
      if (interrupted) continue;
      await condition(allHandlersFinished);
      if (activity !== seen || !done()) continue;
      // From here the validators refuse every update, so nothing that the
      // relay accepted can be lost when the workflow ends.
      closing = true;
      await removeAttachments({ deployId: s.deployId, repoKey: s.spec.repoKey, slug: s.spec.slug });
      return;
    }
    await condition(allHandlersFinished);
    await continueAsNew<typeof specWorkflow>(s);
  }
}
