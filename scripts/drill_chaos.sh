#!/usr/bin/env bash
# World outage mid-run, flaky world (30% errors), and the runaway step cap.
set -euo pipefail
API=${API:-http://localhost:8080}; WORLD=${WORLD:-http://localhost:9210}; TOKEN=${TOKEN:-relay-demo-token}
OUT=docs/verification/chaos.txt; mkdir -p docs/verification; : > "$OUT"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
api() { curl -fsS -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$@"; }
rid() { python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])"; }
wait_terminal() { for _ in $(seq 1 180); do S=$(api "$API/runs/$1" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])"); [[ $S =~ ^(succeeded|failed|cancelled)$ ]] && { echo "$S"; return; }; sleep 1; done; echo timeout; }

curl -fsS -X POST "$WORLD/admin/reset" >/dev/null
log "1) world DOWN mid-run"
curl -fsS -X POST "$WORLD/admin/config" -d '{"mode":"down"}' >/dev/null
RUN=$(api -X POST "$API/workflows/wf_expense_approval/trigger" -d '{"input":{"employee_email":"dev2@example.com","amount_usd":40,"description":"Team lunch"}}' | rid)
sleep 8
log "while down:"; api "$API/runs/$RUN" | python3 scripts/trace_summary.py | tee -a "$OUT"
log "restoring the world"
curl -fsS -X POST "$WORLD/admin/config" -d '{"mode":"ok"}' >/dev/null
log "final: $(wait_terminal "$RUN")"; api "$API/runs/$RUN" | python3 scripts/trace_summary.py | tee -a "$OUT"

log "2) flaky world fail_rate=0.3, 5 runs"
curl -fsS -X POST "$WORLD/admin/reset" >/dev/null
curl -fsS -X POST "$WORLD/admin/config" -d '{"fail_rate":0.3}' >/dev/null
IDS=()
for i in 1 2 3 4 5; do
  BODY=$(printf '{"input":{"employee_email":"f%s@example.com","amount_usd":%s,"description":"flaky %s"}}' "$i" "$i" "$i")
  IDS+=("$(api -X POST "$API/workflows/wf_expense_approval/trigger" -d "$BODY" | rid)")
done
for id in "${IDS[@]}"; do log "$id -> $(wait_terminal "$id")"; done
curl -fsS -X POST "$WORLD/admin/config" -d '{"fail_rate":0}' >/dev/null
python3 scripts/duplication_check.py --url "$WORLD" | tee -a "$OUT"

log "3) runaway poll loop vs the step cap"
RUN=$(api -X POST "$API/workflows/wf_runaway/trigger" -d '{"input":{}}' | rid)
log "final: $(wait_terminal "$RUN")"
api "$API/runs/$RUN" | python3 -c "import sys,json;d=json.load(sys.stdin);print('steps_executed', d['steps_executed'], 'error', d['error'])" | tee -a "$OUT"
