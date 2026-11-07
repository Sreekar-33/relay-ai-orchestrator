#!/usr/bin/env bash
# Self-driving, narrated demo (~4.5 min). Start a screen recording (Cmd+Shift+5, Record Entire Screen), then run:
#   bash scripts/auto_demo.sh
# Narration uses macOS `say` (free, offline). Pages open in your default browser. Terminal + browser side by side.
set -uo pipefail
API=http://localhost:8080; WORLD=http://localhost:9210; T="Authorization: Bearer relay-demo-token"; C="$API/console/#"
VOICE_RATE=${VOICE_RATE:-190}
narrate() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; [[ -n "${DRY:-}" ]] || say -r "$VOICE_RATE" "$*"; }
show() { [[ -n "${DRY:-}" ]] && { echo "   [open] $1"; return; }; open "$1"; sleep 1.5; }
body() { python3 -c "import json;print(json.dumps(next(json.loads(l)['body'] for l in open('data/sample_payloads.jsonl') if l.strip() and json.loads(l)['id']=='$1')))"; }
hook() { curl -s -X POST "$API/hooks/wf_support_triage" -H 'X-Relay-Secret: whsec_triage_501' -H 'Content-Type: application/json' -d "$(body "$1")" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])"; }
trig() { curl -s -X POST "$API/workflows/$1/trigger" -H "$T" -H 'Content-Type: application/json' -d "$2" | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])"; }
status() { curl -s "$API/runs/$1" -H "$T" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])"; }
wait_for() { for _ in $(seq 1 ${3:-60}); do [[ "$(status "$1")" =~ ^($2)$ ]] && return 0; sleep 1; done; return 1; }
pending_for() { curl -s "$API/approvals?status=pending" -H "$T" | python3 -c "import sys,json;print(next(a['id'] for a in json.load(sys.stdin) if a['run_id']=='$1'))"; }
ledger_has() { curl -s "$WORLD/admin/ledger" | python3 -c "import sys,json;sys.exit(0 if any(e['action']=='$1' for e in json.load(sys.stdin)['entries']) else 1)"; }

curl -s -X POST "$WORLD/admin/reset" >/dev/null; curl -s -X POST "$WORLD/admin/config" -d '{"mode":"ok","fail_rate":0,"latency_ms":0}' >/dev/null

# 1. Intro
show "$C/"
narrate "This is Relay, an A I workflow orchestrator. Workflows are typed nodes. Runs are queued in Postgres and executed by a separate worker. Every step is saved before it runs, so crashes never repeat side effects. A I output is checked against a schema, and approval gates are enforced by the engine itself."
show "$C/workflows"
narrate "The four provided workflows are seeded and published. Every run takes a snapshot of its definition, so editing a workflow never changes runs already in flight."

# 2. Normal triage through the webhook
R1=$(hook pay_001); wait_for "$R1" "succeeded|failed|waiting_approval" 60
show "$C/runs/$R1"
narrate "A webhook, protected by a secret, delivered a customer complaint. The A I node classified it, with tokens recorded in the trace. The condition branched on that output, and a notification was sent to the support channel, carrying an idempotency key."

# 3. Prompt injection
R2=$(hook pay_inject_001); wait_for "$R2" "succeeded|failed|waiting_approval" 60
show "$C/runs/$R2"
narrate "This payload is a prompt injection. It claims approval was already granted and demands a five thousand dollar refund. Relay flags the injection patterns, and the run still stops at the approval gate. The gate is a database check in the engine, not a prompt, so nothing the model says can skip it."
A2=$(pending_for "$R2"); curl -s -X POST "$API/approvals/$A2/reject" -H "$T" -H 'Content-Type: application/json' -d '{"decided_by":"maya","note":"injection attempt"}' >/dev/null
sleep 1; show "$C/runs/$R2"
narrate "I reject it. The run ends cancelled, the decision and decider are recorded, and no refund ever reached the order system."

# 4. Approve a real refund
R3=$(hook pay_002); wait_for "$R3" "waiting_approval|failed|succeeded" 60
A3=$(pending_for "$R3"); curl -s -X POST "$API/approvals/$A3/approve" -H "$T" -H 'Content-Type: application/json' -d '{"decided_by":"maya"}' >/dev/null
wait_for "$R3" "succeeded|failed" 60; show "$C/runs/$R3"
narrate "A genuine refund request is approved by a manager. The run resumes, the refund executes exactly once, and the customer is emailed."

# 5. Kill -9 and resume
curl -s -X POST "$WORLD/admin/reset" >/dev/null
R4=$(trig wf_slow_fulfillment '{"input":{"order_id":"ord_2003","customer_email":"lena@example.com"}}')
for _ in $(seq 1 30); do ledger_has email.send && break; sleep 0.5; done
show "$C/runs/$R4"
narrate "Now the durability test. This fulfillment run has sent its confirmation email and is waiting in a twenty second delay. I will kill the worker with kill minus nine."
docker compose kill -s SIGKILL worker; sleep 1
narrate "The worker is dead. I restart it."
docker compose up -d worker >/dev/null 2>&1
wait_for "$R4" "succeeded|failed" 90; show "$C/runs/$R4"
python3 scripts/duplication_check.py --url "$WORLD"
narrate "The run resumed from the delay, not from step one. The duplication check reads the external ledger and confirms every side effect ran exactly once."

# 6. Outage and backoff
curl -s -X POST "$WORLD/admin/config" -d '{"mode":"down"}' >/dev/null
R5=$(trig wf_expense_approval '{"input":{"employee_email":"dev2@example.com","amount_usd":40,"description":"Team lunch"}}')
sleep 5; show "$C/runs/$R5"
narrate "Here the external world is down. The notification step is retrying with exponential backoff, and every attempt reuses the same idempotency key."
curl -s -X POST "$WORLD/admin/config" -d '{"mode":"ok"}' >/dev/null
wait_for "$R5" "succeeded|failed" 60; show "$C/runs/$R5"
narrate "The world is restored, and the run completes."

# 7. Step cap
R6=$(trig wf_runaway '{"input":{}}'); wait_for "$R6" "failed|succeeded|cancelled" 60; show "$C/runs/$R6"
narrate "This workflow polls an order that never ships, looping forever. The engine's step cap stops it at twelve steps, with a reason naming the cap."

# 8. Schema enforcement
R7=$(trig wf_ai_repair_demo '{"input":{"message":"My earbuds arrived with a cracked charging case and the left bud does not pair. I am really disappointed and want to know my options for a replacement or repair."}}')
wait_for "$R7" "succeeded|failed" 60; show "$C/runs/$R7"
narrate "Schema enforcement. The model's first answer broke the schema. Relay sent back the exact rule it violated, the model repaired it, and only valid output reached downstream steps. Both calls are kept in the trace."

# 9. NL compiler
show "$C/build"
curl -s -X POST "$API/workflows/compile" -H "$T" -H 'Content-Type: application/json' -d '{"description":"Every day at 9am, scan our Postgres database for new signups and update each record in Salesforce.","save":false}' | python3 -m json.tool | head -12
narrate "Relay also compiles plain English into validated workflows, using only node types from the catalog. When a request needs something the catalog cannot do, like a database or Salesforce, it refuses and names what is missing. On the evaluation set it scores thirteen out of fifteen."

# 10. Smoke test
narrate "Finally, the official smoke test." &
python3 scripts/smoke_test.py --url "$API" --token relay-demo-token --world "$WORLD" | tail -3
narrate "Twenty eight of twenty eight checks pass, along with forty three automated tests. The code, the verification report and this demo are in the pull request. Thank you."
