// The errors of a store. Both stores throw the same ones.
import { CLOSING_TEXT, DESK_OFF_TEXT, DROPPED_TEXT } from "../workflows/types.js";

export class TimeoutError extends Error {
  constructor() {
    super("The store did not answer in time.");
  }
}

export class SpecNotFoundError extends Error {
  constructor() {
    super("The relay has no synced spec with this repo and slug.");
  }
}

/** The spec is finished and the store removes its data: it takes no more updates. */
export class SpecClosingError extends Error {
  constructor() {
    super(CLOSING_TEXT);
  }
}

/** The spec no longer exists on the Mac: it takes no messages. */
export class SpecDroppedError extends Error {
  constructor() {
    super(DROPPED_TEXT);
  }
}

/** The relay has no record of the repo's desk: the agent never sent it. */
export class DeskNotFoundError extends Error {
  constructor() {
    super("The relay has no desk for this repo.");
  }
}

/** Claude AI cannot message the desk. The message is the desk's reason. */
export class DeskRefusedError extends Error {
  constructor(reason: string | null) {
    super(reason || DESK_OFF_TEXT);
  }
}

/** The relay could not read the desk's file at start: it keeps the file and makes no new desk over it. */
export class DeskUnreadableError extends Error {
  constructor() {
    super("The relay cannot read the file of this desk. The owner of the relay must check the file in RELAY_DATA_DIR/desks and start the relay again.");
    this.name = "DeskUnreadableError";
  }
}
