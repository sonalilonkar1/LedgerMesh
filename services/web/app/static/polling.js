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

/**
 * One polling loop for the redemption on screen.
 *
 * At most one status request is in flight, and at most one timer is pending,
 * so "Check now" during a request can't start a second loop. Starting a new
 * redemption replaces the old one: its late response is ignored and it
 * schedules nothing.
 *
 * @param {object} hooks
 *   fetchStatus(transactionId) -> Promise<{redemption, timeline}>;
 *   onResult(body), onError(error), onContinue(decision), onFinish() update the page;
 *   onBusy(busy) is told when a request starts and ends (to disable "Check now").
 *   setTimer, clearTimer and now default to the browser's and can be faked in tests.
 */
export function createPoller({
  fetchStatus,
  onResult = () => {},
  onError = () => {},
  onContinue = () => {},
  onFinish = () => {},
  onBusy = () => {},
  setTimer = (fn, ms) => setTimeout(fn, ms),
  clearTimer = (id) => clearTimeout(id),
  now = () => Date.now(),
}) {
  let current = null; // { transactionId, startedAt, inFlight }
  let timer = null;

  function cancelTimer() {
    if (timer !== null) clearTimer(timer);
    timer = null;
  }

  async function run(target) {
    if (target !== current || target.inFlight) return;
    cancelTimer();
    target.inFlight = true;
    onBusy(true);
    let status = null;
    let failed = false;
    try {
      const body = await fetchStatus(target.transactionId);
      if (target !== current) return;
      status = body.redemption.status;
      onResult(body);
    } catch (error) {
      if (target !== current) return;
      failed = true;
      onError(error);
    } finally {
      target.inFlight = false;
      if (target === current) onBusy(false);
    }
    const decision = nextPoll({ status, elapsedMs: now() - target.startedAt, failed });
    if (decision.action === "stop") {
      current = null;
      onFinish();
      return;
    }
    onContinue(decision);
    timer = setTimer(() => run(target), decision.delayMs);
  }

  return {
    /** Follow a new redemption, replacing any one being followed. */
    track(transactionId) {
      cancelTimer();
      current = { transactionId, startedAt: now(), inFlight: false };
      onBusy(false);
      return run(current);
    },
    /** Check immediately; ignored while a check is already running. */
    checkNow() {
      if (!current || current.inFlight) return Promise.resolve(false);
      return run(current).then(() => true);
    },
    /** Whether a status request is running right now. */
    get busy() {
      return Boolean(current && current.inFlight);
    },
    /** Whether a check is scheduled (for tests). */
    get scheduled() {
      return timer !== null;
    },
  };
}
