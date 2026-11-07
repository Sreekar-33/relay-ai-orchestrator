#!/usr/bin/env bash
# Kill-and-resume drill for wf_slow_fulfillment (payload pay_201).
#   delay        : SIGKILL the worker container during the 20s pack_delay, then restart it.
#   crash-window : the worker kills itself (exit 137) right AFTER the shipment POST and BEFORE the step is persisted
#                  (the crash window from DATA_MODEL.md), then restarts without the fault.
set -euo pipefail
MODE=${1:-delay}
API=${API:-http://localhost:8080}; WORLD=${WORLD:-http://localhost:9210}; TOKEN=${TOKEN:-relay-demo-token}
OUT=docs/verification/drill_${MODE}.txt
mkdir -p docs/verification; : > "$OUT"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
api() { curl -fsS -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$@"; }
ledger_has() { curl -fsS "$WORLD/admin/ledger" | python3 -c "import sys,json;sys.exit(0 if any(e['action']=='$1' for e in json.load(sys.stdin)['entries']) else 1)"; }

log "mode=$MODE: resetting the mock world ledger"
curl -fsS -X POST "$WORLD/admin/reset" >/dev/null
if [[ $MODE == crash-window ]]; then
  log "restarting worker WITH fault crash_after_side_effect:create_shipment"
  RELAY_FAULTS=crash_after_side_effect:create_shipment docker compose up -d --force-recreate worker >/dev/null 2>&1
  sleep 3
fi
RUN=$(api -X POST "$API/workflows/wf_slow_fulfillment/trigger" -d '{"input":{"order_id":"ord_2003","customer_email":"lena@example.com"}}' \
      | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
log "triggered run $RUN"
if [[ $MODE == delay ]]; then
  for _ in $(seq 1 60); do ledger_has email.send && break; sleep 0.5; done
  log "confirmation email is in the ledger; the run is inside the 20s pack_delay"
  sleep 3
  api "$API/runs/$RUN" | python3 scripts/trace_summary.py | tee -a "$OUT"
  log "kill -9 the worker:  docker compose kill -s SIGKILL worker"
  docker compose kill -s SIGKILL worker >/dev/null 2>&1
  sleep 4
  log "restarting the worker"
  docker compose up -d worker >/dev/null 2>&1
else
  CID=$(docker compose ps -aq worker)
  for _ in $(seq 1 120); do [[ "$(docker inspect -f '{{.State.Status}}' "$CID")" == exited ]] && break; sleep 0.5; done
  log "worker exited with code $(docker inspect -f '{{.State.ExitCode}}' "$CID") right after the shipment POST, before persisting the step"
  api "$API/runs/$RUN" | python3 scripts/trace_summary.py | tee -a "$OUT"
  log "restarting the worker WITHOUT the fault"
  RELAY_FAULTS= docker compose up -d --force-recreate worker >/dev/null 2>&1
fi
ST=unknown
for _ in $(seq 1 120); do
  ST=$(api "$API/runs/$RUN" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])")
  [[ $ST == succeeded || $ST == failed ]] && break; sleep 1
done
log "final run status: $ST"
api "$API/runs/$RUN" | python3 scripts/trace_summary.py | tee -a "$OUT"
log "duplication check:"
python3 scripts/duplication_check.py --url "$WORLD" | tee -a "$OUT"
