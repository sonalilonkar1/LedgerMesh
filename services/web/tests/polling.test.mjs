// Polling rules for the LM-011 page: never stop on an unfinished redemption.
// Run by test_polling_js.py with `node --test`.
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  FAST_MS,
  SLOW_AFTER_MS,
  SLOW_MS,
  STILL_PROCESSING,
  isFinished,
  nextPoll,
  outcome,
} from "../app/static/polling.js";

test("finished redemptions stop polling", () => {
  for (const status of ["COMMITTED", "COMPLETED", "ABORTED", "FAILED", "COMPENSATED"]) {
    assert.deepEqual(nextPoll({ status, elapsedMs: 0 }), { action: "stop" });
    assert.ok(isFinished(status));
  }
});

test("in-progress redemptions poll every second at first", () => {
  assert.deepEqual(nextPoll({ status: "PREPARING", elapsedMs: 10_000 }), {
    action: "continue",
    delayMs: FAST_MS,
    notice: null,
  });
});

test("after the limit it keeps polling, slower, and says it is still processing", () => {
  for (const elapsedMs of [SLOW_AFTER_MS, SLOW_AFTER_MS + 1, 60 * 60 * 1000]) {
    assert.deepEqual(nextPoll({ status: "COMPENSATING", elapsedMs }), {
      action: "continue",
      delayMs: SLOW_MS,
      notice: STILL_PROCESSING,
    });
  }
});

test("a failed poll keeps trying instead of giving up", () => {
  assert.deepEqual(nextPoll({ status: null, elapsedMs: 5_000, failed: true }), {
    action: "continue",
    delayMs: SLOW_MS,
    notice: null,
  });
  assert.equal(
    nextPoll({ status: null, elapsedMs: SLOW_AFTER_MS + 1, failed: true }).action,
    "continue",
  );
});

test("a state the page has never seen is treated as in progress", () => {
  assert.equal(outcome("SOME_FUTURE_STATE"), "pending");
  assert.equal(nextPoll({ status: "SOME_FUTURE_STATE", elapsedMs: 0 }).action, "continue");
});

test("outcomes map to the status colours", () => {
  assert.equal(outcome("COMMITTED"), "success");
  assert.equal(outcome("COMPENSATED"), "failure");
  assert.equal(outcome("DEDUCTING_POINTS"), "pending");
});
