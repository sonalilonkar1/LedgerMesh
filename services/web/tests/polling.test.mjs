// Polling rules for the LM-011 page: never stop on an unfinished redemption.
// Run by test_polling_js.py with `node --test`.
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  FAST_MS,
  SLOW_AFTER_MS,
  SLOW_MS,
  STILL_PROCESSING,
  createPoller,
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

// --- One polling loop (Check now during a request) -----------------------------

// Fake clock and timers, and status requests the test answers by hand.
function harness() {
  const requests = [];
  const timers = new Map();
  let nextTimer = 1;
  let clock = 0;
  const events = [];
  const poller = createPoller({
    fetchStatus: (transactionId) =>
      new Promise((resolve, reject) => requests.push({ transactionId, resolve, reject })),
    onResult: ({ redemption }) => events.push(`result ${redemption.status}`),
    onError: (error) => events.push(`error ${error.message}`),
    onFinish: () => events.push("finish"),
    onBusy: (busy) => events.push(busy ? "busy" : "idle"),
    setTimer: (fn, ms) => {
      const id = nextTimer++;
      timers.set(id, { fn, ms });
      return id;
    },
    clearTimer: (id) => timers.delete(id),
    now: () => clock,
  });
  const answer = async (status, index = requests.length - 1) => {
    requests[index].resolve({ redemption: { status }, timeline: [] });
    await new Promise((resolve) => setImmediate(resolve));
  };
  const fire = async () => {
    const [[id, { fn }]] = timers;
    timers.delete(id);
    fn();
    await new Promise((resolve) => setImmediate(resolve));
  };
  return { poller, requests, timers, events, answer, fire, tick: (ms) => (clock += ms) };
}

test("Check now during a request does not start a second loop", async () => {
  const h = harness();
  h.poller.track("tx-1");
  assert.equal(h.requests.length, 1);
  assert.equal(h.poller.busy, true);

  // Clicked three times while the first request is still in flight.
  assert.equal(await h.poller.checkNow(), false);
  assert.equal(await h.poller.checkNow(), false);
  assert.equal(await h.poller.checkNow(), false);
  assert.equal(h.requests.length, 1);

  await h.answer("DEDUCTING_POINTS");
  assert.equal(h.poller.busy, false);
  assert.equal(h.timers.size, 1); // exactly one loop continues

  // The loop keeps going on its own, still one request and one timer at a time.
  for (let round = 0; round < 3; round++) {
    await h.fire();
    assert.equal(h.timers.size, 0);
    assert.equal(h.requests.length, 2 + round);
    await h.answer("RESERVING_INVENTORY");
    assert.equal(h.timers.size, 1);
  }
});

test("Check now between checks replaces the timer instead of adding one", async () => {
  const h = harness();
  h.poller.track("tx-1");
  await h.answer("PREPARING");
  assert.equal(h.timers.size, 1);

  const checked = h.poller.checkNow();
  assert.equal(h.timers.size, 0); // the pending check was cancelled
  assert.equal(h.requests.length, 2);
  await h.answer("PREPARING");
  assert.equal(await checked, true);
  assert.equal(h.timers.size, 1);
});

test("the button is disabled while a check runs and enabled after", async () => {
  const h = harness();
  h.poller.track("tx-1");
  assert.deepEqual(h.events, ["idle", "busy"]);
  await h.answer("PREPARING");
  assert.deepEqual(h.events, ["idle", "busy", "result PREPARING", "idle"]);
});

test("a finished redemption leaves no loop running", async () => {
  const h = harness();
  h.poller.track("tx-1");
  await h.answer("COMMITTED");

  assert.equal(h.timers.size, 0);
  assert.equal(h.events.at(-1), "finish");
  assert.equal(await h.poller.checkNow(), false);
  assert.equal(h.requests.length, 1);
});

test("a new redemption replaces the old loop; the old late answer is ignored", async () => {
  const h = harness();
  h.poller.track("tx-old");
  h.poller.track("tx-new");
  assert.deepEqual(
    h.requests.map((request) => request.transactionId),
    ["tx-old", "tx-new"],
  );

  await h.answer("COMPLETED", 0); // the old request answers late
  assert.ok(!h.events.includes("result COMPLETED"));
  assert.equal(h.timers.size, 0);

  await h.answer("DEDUCTING_POINTS", 1);
  assert.ok(h.events.includes("result DEDUCTING_POINTS"));
  assert.equal(h.timers.size, 1);
});

test("a failed check keeps one loop going, slower", async () => {
  const h = harness();
  h.poller.track("tx-1");
  h.requests[0].reject(new Error("offline"));
  await new Promise((resolve) => setImmediate(resolve));

  assert.ok(h.events.includes("error offline"));
  assert.equal(h.timers.size, 1);
  assert.equal([...h.timers.values()][0].ms, SLOW_MS);
});

test("after the limit, Check now still works and one slow loop remains", async () => {
  const h = harness();
  h.poller.track("tx-1");
  h.tick(SLOW_AFTER_MS + 1);
  await h.answer("COMPENSATING");
  assert.equal([...h.timers.values()][0].ms, SLOW_MS);

  h.poller.checkNow();
  h.poller.checkNow();
  assert.equal(h.requests.length, 2);
  await h.answer("COMPENSATING");
  assert.equal(h.timers.size, 1);
});
