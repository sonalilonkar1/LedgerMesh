// LedgerMesh Rewards (LM-011). Talks only to this app's /api.
// Every value from the API is written with textContent, never as HTML.
import { createPoller, outcome } from "./polling.js";

const MODES = { two_phase_commit: "Two-Phase Commit", saga: "Saga" };

const $ = (id) => document.getElementById(id);

async function api(path, options = {}) {
  let response;
  try {
    response = await fetch(path, {
      ...options,
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    });
  } catch {
    throw new Error("Could not reach the server. Check that the stack is running.");
  }
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const title = body.title || "Request failed";
    throw new Error(body.detail ? `${title}: ${body.detail}` : title);
  }
  return body;
}

function showError(message) {
  const box = $("error");
  box.textContent = message;
  box.hidden = false;
}

function clearError() {
  $("error").hidden = true;
  $("error").textContent = "";
}

async function loadWallet() {
  try {
    const wallet = await api(`/api/users/${encodeURIComponent($("user").value)}/wallet`);
    $("balance").textContent = wallet.available_points.toLocaleString();
  } catch (error) {
    $("balance").textContent = "—";
    showError(error.message);
  }
}

function cell(text) {
  const td = document.createElement("td");
  td.textContent = text;
  return td;
}

async function loadRewards() {
  const body = $("rewards");
  try {
    const rewards = await api("/api/rewards");
    body.replaceChildren();
    if (rewards.length === 0) {
      const row = document.createElement("tr");
      const td = cell("No rewards are available right now.");
      td.colSpan = 5;
      row.append(td);
      body.append(row);
      return;
    }
    for (const reward of rewards) {
      const row = document.createElement("tr");
      const quantity = document.createElement("input");
      quantity.type = "number";
      quantity.min = "1";
      quantity.max = String(Math.max(1, reward.available_inventory));
      quantity.value = "1";
      quantity.setAttribute("aria-label", `Quantity of ${reward.name}`);
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = "Redeem";
      button.addEventListener("click", () => redeem(reward, Number(quantity.value), button));
      const quantityCell = document.createElement("td");
      quantityCell.append(quantity);
      const actionCell = document.createElement("td");
      actionCell.append(button);
      row.append(
        cell(reward.name),
        cell(reward.point_cost.toLocaleString()),
        cell(String(reward.available_inventory)),
        quantityCell,
        actionCell,
      );
      body.append(row);
    }
  } catch (error) {
    showError(error.message);
  }
}

async function redeem(reward, quantity, button) {
  clearError();
  if (!Number.isInteger(quantity) || quantity < 1) {
    showError("Quantity must be a whole number of at least 1.");
    return;
  }
  const mode = document.querySelector('input[name="mode"]:checked').value;
  button.disabled = true;
  try {
    const created = await api("/api/redemptions", {
      method: "POST",
      // One key per click: a retried request is not a second redemption.
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: JSON.stringify({
        user_id: $("user").value,
        reward_id: reward.reward_id,
        quantity,
        coordination_mode: mode,
      }),
    });
    track(created.transaction_id);
  } catch (error) {
    showError(error.message);
  } finally {
    button.disabled = false;
  }
}

function renderStatus(redemption) {
  const status = $("status");
  status.textContent = redemption.status.replaceAll("_", " ");
  status.className = `status ${outcome(redemption.status)}`;
  $("mode-label").textContent = MODES[redemption.coordination_mode] || redemption.coordination_mode;
  $("transaction").textContent = redemption.transaction_id;
}

function renderTimeline(timeline) {
  const list = $("timeline");
  list.replaceChildren();
  for (const step of timeline) {
    const item = document.createElement("li");
    const when = document.createElement("time");
    when.dateTime = step.created_at;
    when.textContent = new Date(step.created_at).toLocaleTimeString();
    const what = document.createElement("strong");
    what.textContent = `${step.from_status || "start"} → ${step.to_status}`;
    const why = document.createElement("span");
    why.className = "muted";
    why.textContent = step.reason ? ` ${step.reason}` : "";
    item.append(when, " ", what, why);
    list.append(item);
  }
}

function showNotice(message) {
  $("notice").textContent = message || "";
  $("notice").hidden = !message;
}

// Follows the redemption until it reaches a final state; it never silently
// stops. After a few minutes it slows down and says it is still processing.
// One loop only: "Check now" is disabled while a check is running.
const poller = createPoller({
  fetchStatus: (transactionId) => api(`/api/redemptions/${transactionId}`),
  onResult: ({ redemption, timeline }) => {
    renderStatus(redemption);
    renderTimeline(timeline);
    clearError();
  },
  onError: (error) => showError(`${error.message} Still trying.`),
  onContinue: (decision) => {
    showNotice(decision.notice);
    $("check-now").hidden = false;
  },
  onFinish: () => {
    showNotice(null);
    $("check-now").hidden = true;
    loadWallet();
    loadRewards();
  },
  onBusy: (busy) => {
    $("check-now").disabled = busy;
  },
});

function track(transactionId) {
  $("progress").hidden = false;
  showNotice(null);
  poller.track(transactionId);
}

$("check-now").addEventListener("click", () => poller.checkNow());

$("user").addEventListener("change", () => {
  clearError();
  loadWallet();
});
loadWallet();
loadRewards();
