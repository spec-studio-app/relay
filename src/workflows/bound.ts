// Every update input carries its target (the workflow ID) and the relay's time.
// The codec encrypts the input, but a person with write access to the namespace
// can copy an encrypted payload from one history and send it again. The
// validators refuse a copy that goes to another workflow, or a copy older than
// BOUND_MAX_AGE_MS.
//
// The residual risk: a copy sent again to the same workflow within
// BOUND_MAX_AGE_MS of the original is accepted. Most updates are idempotent
// for such a copy (a sync line already applied, an ack of a removed message, a
// gone that already happened); a copied send adds the same text again.
//
// This file runs in the workflow sandbox too: keep it free of Node APIs.
import { workflowInfo } from "@temporalio/workflow";

export interface Bound {
  /** The workflow ID that the input is for. */
  target: string;
  /** The relay's time when it made the input (ms since the epoch). */
  at: number;
}

export const BOUND_MAX_AGE_MS = 5 * 60 * 1000;
export const BOUND_TEXT = "The update input is not for this workflow, or it is too old.";

/** The bound of an update input (in the relay). */
export function boundFor(target: string, now: number = Date.now()): Bound {
  return { target, at: now };
}

/** In a validator: refuse an input for another workflow, or one older than BOUND_MAX_AGE_MS. */
export function checkBound(bound: Bound | undefined): void {
  if (!bound || typeof bound !== "object" || bound.target !== workflowInfo().workflowId || typeof bound.at !== "number") {
    throw new Error(BOUND_TEXT);
  }
  // Date.now() in a workflow is the time of the workflow task.
  if (Math.abs(Date.now() - bound.at) > BOUND_MAX_AGE_MS) throw new Error(BOUND_TEXT);
}
