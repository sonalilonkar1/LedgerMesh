// How the page follows a redemption (LM-011). Pure functions, so they can be
// tested without a browser (services/web/tests/polling.test.mjs).
//
// The page never gives up on an unfinished redemption: a Saga waiting on
// retries or a 2PC redemption waiting for recovery can take minutes. After
// SLOW_AFTER_MS it keeps checking, less often, and says so.

// Final states. Anything else is in progress, so a state added to the
// Coordinator later needs no change here.
export const SUCCESS = new Set(["COMMITTED", "COMPLETED"]);
export const FAILURE = new Set(["ABORTED", "FAILED", "COMPENSATED"]);

export const FAST_MS = 1000;
export const SLOW_MS = 5000;
export const SLOW_AFTER_MS = 3 * 60 * 1000;

export const STILL_PROCESSING =
  "Still processing. This can take a while if a step is being retried or recovered. " +
  "Checking every 5 seconds.";

export function isFinished(status) {
  return SUCCESS.has(status) || FAILURE.has(status);
}

export function outcome(status) {
  if (SUCCESS.has(status)) return "success";
  if (FAILURE.has(status)) return "failure";
  return "pending";
}

/**
 * Decide what to do after one poll.
 *
 * @param {{status: string|null, elapsedMs: number, failed: boolean}} poll
 *   status: the redemption's status, or null if the poll failed;
 *   elapsedMs: time since tracking started; failed: whether the poll errored.
 * @returns {{action: "stop"} | {action: "continue", delayMs: number, notice: string|null}}
 */
export function nextPoll({ status, elapsedMs, failed = false }) {
  if (!failed && isFinished(status)) {
    return { action: "stop" };
  }
  if (failed || elapsedMs >= SLOW_AFTER_MS) {
    return {
      action: "continue",
      delayMs: SLOW_MS,
      notice: elapsedMs >= SLOW_AFTER_MS ? STILL_PROCESSING : null,
    };
  }
  return { action: "continue", delayMs: FAST_MS, notice: null };
}
