#!/usr/bin/env bash
# End-to-end smoke test for the OCaml Temporal SDK.
#
# Boots a throwaway Temporal dev server and the e-commerce example worker, then
# drives real workflows through the public API and asserts the outcomes against
# server history — the same checks that caught every wire-format and semantic bug
# during development. This is an integration test: it needs the `temporal` CLI,
# python3, cargo + protoc (dune builds the Rust bridge on demand), and builds the
# worker below.
#
# Scenarios:
#   1. happy path        -> WORKFLOW_EXECUTION_STATUS_COMPLETED + composed result
#   2. durable timer     -> TIMER_STARTED + TIMER_FIRED in history
#   3. activity failure  -> WORKFLOW_EXECUTION_STATUS_FAILED + root-cause message
#
# Exit: 0 all passed, 1 an assertion failed, 2 setup/prerequisite problem.
set -uo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
worker="$root/_build/default/examples/ecommerce/main.exe"
port="${TEMPORAL_SMOKE_PORT:-17233}"
task_queue=ecommerce
tmp="$(mktemp -d)"
fails=0

log()  { printf '\n== %s ==\n' "$*"; }
pass() { printf '  ok   %s\n' "$*"; }
fail() { printf '  FAIL %s\n' "$*"; fails=$((fails + 1)); }

cleanup() {
  [ -n "${WK:-}" ] && kill "$WK" 2>/dev/null
  [ -n "${SRV:-}" ] && kill "$SRV" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

# ---- prerequisites ------------------------------------------------------------
command -v temporal >/dev/null || { echo "temporal CLI not found on PATH"; exit 2; }
command -v python3  >/dev/null || { echo "python3 not found on PATH"; exit 2; }

log "building example worker"
if ! (cd "$root" && dune build examples/ecommerce/main.exe) 2>"$tmp/build.err"; then
  cat "$tmp/build.err"
  echo "build failed — dune builds the Rust bridge via cargo; is cargo + protoc on PATH?"
  exit 2
fi

# The CLI and worker both point at our private headless server, so this never
# collides with a dev server the user already has on the default ports.
export TEMPORAL_ADDRESS="localhost:$port"

# ---- boot server + worker -----------------------------------------------------
log "starting temporal dev server (headless, port $port)"
temporal server start-dev --headless --port "$port" >"$tmp/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 60); do temporal operator namespace list >/dev/null 2>&1 && break; sleep 1; done
temporal operator namespace list >/dev/null 2>&1 || { echo "server never became ready"; cat "$tmp/server.log"; exit 2; }

log "starting worker"
TEMPORAL_TARGET="http://localhost:$port" TEMPORAL_TASK_QUEUE="$task_queue" \
  "$worker" >"$tmp/worker.log" 2>&1 &
WK=$!
for _ in $(seq 1 30); do grep -q "worker polling" "$tmp/worker.log" && break; sleep 1; done
grep -q "worker polling" "$tmp/worker.log" || { echo "worker never started polling"; cat "$tmp/worker.log"; exit 2; }

# ---- helpers ------------------------------------------------------------------
start_wf() { # id type input
  temporal workflow start --task-queue "$task_queue" --type "$2" \
    --workflow-id "$1" --input "$3" >/dev/null 2>&1
}
status() { # id -> status string
  temporal workflow describe --workflow-id "$1" --output json 2>/dev/null \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["workflowExecutionInfo"]["status"])' 2>/dev/null
}
await_terminal() { # id -> final (non-Running) status
  local s
  for _ in $(seq 1 30); do
    s="$(status "$1")"
    case "$s" in ""|Running|*RUNNING) sleep 1 ;; *) echo "$s"; return ;; esac
  done
  echo "${s:-TIMEOUT}"
}
result() { # id -> decoded completed result value
  temporal workflow show --workflow-id "$1" --output json 2>/dev/null | python3 -c '
import sys, json, base64
d = json.load(sys.stdin)
ev = [e for e in d["events"] if "workflowExecutionCompletedEventAttributes" in e]
if ev:
    print(base64.b64decode(ev[0]["workflowExecutionCompletedEventAttributes"]["result"]["payloads"][0]["data"]).decode())
' 2>/dev/null
}
failure_msg() { # id -> failure message
  temporal workflow show --workflow-id "$1" --output json 2>/dev/null | python3 -c '
import sys, json
d = json.load(sys.stdin)
ev = [e for e in d["events"] if "workflowExecutionFailedEventAttributes" in e]
print(ev[0]["workflowExecutionFailedEventAttributes"]["failure"].get("message", "") if ev else "")
' 2>/dev/null
}
has_events() { # id type-substring... -> yes|no
  local id="$1"; shift
  temporal workflow show --workflow-id "$id" --output json 2>/dev/null | python3 -c '
import sys, json
d = json.load(sys.stdin)
types = {e["eventType"] for e in d["events"]}
want = sys.argv[1:]
print("yes" if all(any(w in t for t in types) for w in want) else "no")
' "$@" 2>/dev/null
}
query() { # id type -> decoded query result value
  # `--output json` already applies the data converter, so the result is the
  # decoded value under queryResult (a list), not a base64 payload envelope.
  temporal workflow query --workflow-id "$1" --type "$2" --output json 2>/dev/null | python3 -c '
import sys, json
d = json.load(sys.stdin)
r = d.get("queryResult")
if isinstance(r, list) and r:
    v = r[0]
    print(v if isinstance(v, str) else json.dumps(v))
' 2>/dev/null
}
update() { # id name input -> decoded update result (empty if not accepted)
  temporal workflow update execute --workflow-id "$1" --name "$2" --input "$3" --output json 2>/dev/null | python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
r = d.get("result")
if r is not None:
    print(r if isinstance(r, str) else json.dumps(r))
' 2>/dev/null
}
count_events() { # id type-substring -> count of matching history events
  temporal workflow show --workflow-id "$1" --output json 2>/dev/null | python3 -c '
import sys, json
d = json.load(sys.stdin)
print(sum(1 for e in d["events"] if sys.argv[1] in e["eventType"]))
' "$2" 2>/dev/null
}
has_activity() { # id activity-name -> yes|no (was this activity type scheduled?)
  temporal workflow show --workflow-id "$1" --output json 2>/dev/null | python3 -c '
import sys, json
d = json.load(sys.stdin)
names = {e["activityTaskScheduledEventAttributes"]["activityType"]["name"]
         for e in d["events"] if "activityTaskScheduledEventAttributes" in e}
print("yes" if sys.argv[1] in names else "no")
' "$2" 2>/dev/null
}
await_history() { # id type-substring: poll until an event of this type appears
  for _ in $(seq 1 30); do
    case "$(has_events "$1" "$2")" in yes) return 0 ;; *) sleep 1 ;; esac
  done
  return 1
}

# ---- scenario 1 + 2: happy path with a durable timer --------------------------
log "scenario 1+2: happy path + durable timer"
order='{"order_id":"o-1","customer":"alice","ship_to":"123 Main St","items":[{"sku":"WIDGET","qty":2,"unit_price":1500},{"sku":"GADGET","qty":1,"unit_price":1200}]}'
start_wf smoke-ok OrderWorkflow "$order"
st="$(await_terminal smoke-ok)"
case "$st" in *COMPLETED) pass "happy path COMPLETED" ;; *) fail "happy path status: $st" ;; esac
res="$(result smoke-ok)"
case "$res" in
  *"charged ch_o-1_4200, reserved rsv_WIDGET-GADGET, shipment delivered:1Z_pkg_2_items"*) pass "composed result value (shipment from child)" ;;
  *) fail "unexpected result: $res" ;;
esac
case "$(has_events smoke-ok TIMER_STARTED TIMER_FIRED)" in
  yes) pass "durable timer started and fired" ;;
  *)   fail "timer events missing from history" ;;
esac
# ShipmentWorkflow ran as a child: its start+completion appear in the parent's
# history, and it exists as its own execution under the derived id.
case "$(has_events smoke-ok CHILD_WORKFLOW_EXECUTION_STARTED CHILD_WORKFLOW_EXECUTION_COMPLETED)" in
  yes) pass "child workflow started and completed in parent history" ;;
  *)   fail "child workflow events missing from parent history" ;;
esac
cst="$(await_terminal 'smoke-ok/1')"
case "$cst" in *COMPLETED) pass "child completed as its own execution (id smoke-ok/1)" ;; *) fail "child status: $cst" ;; esac

# ---- scenario 3: activity failure fails the workflow --------------------------
log "scenario 3: activity failure -> workflow FAILED"
start_wf smoke-bad OrderWorkflow '{"order_id":"o-empty","customer":"bob","ship_to":"123 Main St","items":[]}'
st="$(await_terminal smoke-bad)"
case "$st" in *FAILED) pass "bad order FAILED" ;; *) fail "bad order status: $st" ;; esac
msg="$(failure_msg smoke-bad)"
case "$msg" in *"no line items"*) pass "root-cause message surfaced" ;; *) fail "unexpected failure message: $msg" ;; esac

# ---- scenario 4: continue-as-new ----------------------------------------------
log "scenario 4: continue-as-new"
# n=2 => run1 continues-as-new to run2 continues-as-new to run3, which completes.
# Reaching the countdown-finished result is only possible if the CAN chain ran.
start_wf smoke-can CountdownWorkflow '2'
st="$(await_terminal smoke-can)"
case "$st" in *COMPLETED) pass "countdown COMPLETED after continue-as-new chain" ;; *) fail "countdown status: $st" ;; esac
res="$(result smoke-can)"
case "$res" in *"countdown finished"*) pass "reached completion via continue-as-new" ;; *) fail "unexpected result: $res" ;; esac

# ---- scenario 5: signals + wait_condition -------------------------------------
log "scenario 5: signals + wait_condition"
# approve path: workflow blocks on wait_condition until the signal arrives
start_wf smoke-approve ApprovalWorkflow '"expense-42"'
sleep 2
temporal workflow signal --workflow-id smoke-approve --name approve >/dev/null 2>&1
st="$(await_terminal smoke-approve)"
case "$st" in *COMPLETED) pass "approve signal COMPLETED" ;; *) fail "approve status: $st" ;; esac
res="$(result smoke-approve)"
case "$res" in *"expense-42 -> approved"*) pass "approve decision recorded" ;; *) fail "approve result: $res" ;; esac
# reject path: signal carries a typed string argument
start_wf smoke-reject ApprovalWorkflow '"expense-43"'
sleep 2
temporal workflow signal --workflow-id smoke-reject --name reject --input '"too expensive"' >/dev/null 2>&1
st="$(await_terminal smoke-reject)"
case "$st" in *COMPLETED) pass "reject signal COMPLETED" ;; *) fail "reject status: $st" ;; esac
res="$(result smoke-reject)"
case "$res" in *"expense-43 -> rejected: too expensive"*) pass "reject decision + typed reason" ;; *) fail "reject result: $res" ;; esac

# ---- scenario 6: queries ------------------------------------------------------
log "scenario 6: query a running then a closed workflow"
# while running (blocked in wait_condition) the status query answers "pending" via
# a read-only replay to the frontier that emits no workflow-advancing commands
start_wf smoke-query ApprovalWorkflow '"expense-99"'
sleep 2
q1="$(query smoke-query status)"
case "$q1" in *pending*) pass "query while running -> pending" ;; *) fail "running query: $q1" ;; esac
# after a decision + completion, querying the closed workflow answers "approved"
temporal workflow signal --workflow-id smoke-query --name approve >/dev/null 2>&1
st="$(await_terminal smoke-query)"
case "$st" in *COMPLETED) pass "queried workflow COMPLETED after approve" ;; *) fail "query wf status: $st" ;; esac
q2="$(query smoke-query status)"
case "$q2" in *approved*) pass "query after close -> approved" ;; *) fail "closed query: $q2" ;; esac

# ---- scenario 7: signal buffering ---------------------------------------------
log "scenario 7: signal buffered until its handler is registered"
# the workflow sleeps on a durable timer before registering its handler; send the
# signal during that window so it lands before any handler exists. Without
# buffering the signal is dropped and the workflow hangs (never COMPLETED).
start_wf smoke-buffer BufferedSignalWorkflow '"batch-7"'
sleep 1
temporal workflow signal --workflow-id smoke-buffer --name resume --input '"payload-7"' >/dev/null 2>&1
st="$(await_terminal smoke-buffer)"
case "$st" in *COMPLETED) pass "buffered-signal workflow COMPLETED" ;; *) fail "buffer status: $st" ;; esac
res="$(result smoke-buffer)"
case "$res" in *"batch-7 resumed with payload-7"*) pass "signal delivered after late handler registration" ;; *) fail "buffer result: $res" ;; esac

# ---- scenario 8: updates ------------------------------------------------------
log "scenario 8: update mutates state, returns a value, and is validator-gated"
start_wf smoke-acct AccountWorkflow '100'
sleep 2
d1="$(update smoke-acct deposit 50)"
case "$d1" in *150*) pass "update deposit 50 -> 150" ;; *) fail "deposit1: $d1" ;; esac
d2="$(update smoke-acct deposit 25)"
case "$d2" in *175*) pass "second update -> 175 (state accumulates)" ;; *) fail "deposit2: $d2" ;; esac
# the validator rejects a non-positive deposit (CLI exits non-zero with the message)
rej="$(temporal workflow update execute --workflow-id smoke-acct --name deposit --input '-5' 2>&1)"
case "$rej" in *positive*) pass "validator rejected negative deposit" ;; *) fail "rejection: $rej" ;; esac
# the query reflects update-mutated state, unchanged by the rejected update
qb="$(query smoke-acct balance)"
case "$qb" in *175*) pass "query balance -> 175 (rejected update mutated nothing)" ;; *) fail "balance query: $qb" ;; esac
# a close signal ends the workflow with the final balance
temporal workflow signal --workflow-id smoke-acct --name close >/dev/null 2>&1
st="$(await_terminal smoke-acct)"
case "$st" in *COMPLETED) pass "account workflow COMPLETED" ;; *) fail "account status: $st" ;; esac
res="$(result smoke-acct)"
case "$res" in *175*) pass "final balance 175" ;; *) fail "account result: $res" ;; esac

# ---- scenario 9: workflow cancellation ----------------------------------------
log "scenario 9: cancel a workflow blocked on wait_condition"
# the workflow blocks in wait_condition; a CancelWorkflow interrupts that wait with
# Canceled, which escapes the body and closes the run as CANCELED (rather than hanging
# on a signal that is no longer coming).
start_wf smoke-cancel ApprovalWorkflow '"expense-cancel"'
sleep 2
temporal workflow cancel --workflow-id smoke-cancel >/dev/null 2>&1
st="$(await_terminal smoke-cancel)"
case "$st" in
  *CANCELED|*Canceled|*CANCELLED|*Cancelled) pass "canceled workflow reaches CANCELED" ;;
  *) fail "cancel status: $st" ;;
esac

# ---- scenario 10: parallel fan-out --------------------------------------------
log "scenario 10: parallel fan-out with start_activity + await_all"
items='[{"sku":"A","qty":1,"unit_price":100},{"sku":"B","qty":1,"unit_price":100},{"sku":"C","qty":1,"unit_price":100}]'
start_wf smoke-fanout BulkPackWorkflow "$items"
st="$(await_terminal smoke-fanout)"
case "$st" in *COMPLETED) pass "fan-out workflow COMPLETED" ;; *) fail "fanout status: $st" ;; esac
res="$(result smoke-fanout)"
case "$res" in *"packed 3 item(s) concurrently"*) pass "fanned out and joined 3 activities" ;; *) fail "fanout result: $res" ;; esac
# all three pack activities were scheduled (started eagerly, before any resolved)
case "$(count_events smoke-fanout ACTIVITY_TASK_SCHEDULED)" in
  3) pass "three activities scheduled concurrently" ;;
  *) fail "expected 3 scheduled activities, got $(count_events smoke-fanout ACTIVITY_TASK_SCHEDULED)" ;;
esac

# ---- scenario 11: cancellation with saga compensation -------------------------
log "scenario 11: cancel triggers a detached refund (saga compensation)"
# the workflow charges, then holds on a durable timer. Cancelling it raises Canceled
# at the sleep; the handler refunds in a detached scope (which the cancel does not
# reach), so the refund runs before the workflow closes as CANCELED.
start_wf smoke-saga SagaCheckoutWorkflow "$order"
if await_history smoke-saga TIMER_STARTED; then
  temporal workflow cancel --workflow-id smoke-saga >/dev/null 2>&1
  st="$(await_terminal smoke-saga)"
  case "$st" in
    *CANCELED|*Canceled|*CANCELLED|*Cancelled) pass "saga workflow reaches CANCELED" ;;
    *) fail "saga status: $st" ;;
  esac
  case "$(has_activity smoke-saga refund_payment)" in
    yes) pass "detached refund compensation ran during cancellation" ;;
    *) fail "no refund_payment activity in saga history" ;;
  esac
else
  fail "saga never reached its hold window (no TIMER_STARTED)"
fi

# ---- scenario 12: await_any interrupted by cancellation -----------------------
log "scenario 12: cancel a workflow parked in await_any"
# the workflow races two long timers and parks in await_any. Cancelling it interrupts
# the race with Canceled, which escapes the body and closes the run as CANCELED.
start_wf smoke-race RaceWorkflow '"race"'
if await_history smoke-race TIMER_STARTED; then
  temporal workflow cancel --workflow-id smoke-race >/dev/null 2>&1
  st="$(await_terminal smoke-race)"
  case "$st" in
    *CANCELED|*Canceled|*CANCELLED|*Cancelled) pass "await_any interrupted by cancel reaches CANCELED" ;;
    *) fail "race status: $st" ;;
  esac
else
  fail "race workflow never started its timers"
fi

# ---- summary ------------------------------------------------------------------
echo
if [ "$fails" -eq 0 ]; then
  echo "SMOKE TEST PASSED"
  exit 0
else
  echo "SMOKE TEST FAILED ($fails check(s))"
  exit 1
fi
