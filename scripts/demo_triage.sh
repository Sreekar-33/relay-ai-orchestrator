#!/usr/bin/env bash
# Sends every wf_support_triage sample payload through the WEBHOOK (external-caller path) and prints what happened.
set -euo pipefail
API=${API:-http://localhost:8080}; WORLD=${WORLD:-http://localhost:9210}; TOKEN=${TOKEN:-relay-demo-token}
OUT=docs/verification/injection.txt; mkdir -p docs/verification; : > "$OUT"
curl -fsS -X POST "$WORLD/admin/reset" >/dev/null
for PID in pay_001 pay_002 pay_003 pay_inject_001 pay_inject_002; do
  BODY=$(python3 -c "import json;print(json.dumps(next(json.loads(l)['body'] for l in open('data/sample_payloads.jsonl') if l.strip() and json.loads(l)['id']=='$PID')))")
  RUN=$(curl -fsS -X POST "$API/hooks/wf_support_triage" -H 'X-Relay-Secret: whsec_triage_501' -H 'Content-Type: application/json' -d "$BODY" \
        | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
  ST=queued
  for _ in $(seq 1 60); do
    ST=$(curl -fsS -H "Authorization: Bearer $TOKEN" "$API/runs/$RUN" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])")
    [[ $ST =~ ^(succeeded|failed|cancelled|waiting_approval)$ ]] && break; sleep 1
  done
  echo "== $PID -> $RUN ($ST)" | tee -a "$OUT"
  curl -fsS -H "Authorization: Bearer $TOKEN" "$API/runs/$RUN" | python3 -c "
import sys, json; r = json.load(sys.stdin)
c = next((s for s in r['steps'] if s['node_id'] == 'classify'), {})
print('   classify output:', json.dumps(c.get('output')))
print('   model / tokens :', (c.get('meta') or {}).get('model'), (c.get('tokens') or {}).get('total'))
print('   risk flags     :', [f['rule'] for f in r['risk_flags']])
print('   path           :', ' -> '.join(s['node_id'] for s in r['steps']))" | tee -a "$OUT"
done
echo "Mock-world ledger (no order.refund can appear before a human approves):" | tee -a "$OUT"
curl -fsS "$WORLD/admin/ledger" | python3 -c "import sys,json;[print('  ', e['seq'], e['action'], json.dumps(e['payload'])) for e in json.load(sys.stdin)['entries']]" | tee -a "$OUT"
