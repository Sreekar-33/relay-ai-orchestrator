#!/usr/bin/env bash
# Self-driving demo (~4.5 min). RECOMMENDED: PRESENT=1 bash scripts/auto_demo.sh  -> shows each line for YOU to read aloud, Enter to continue.
# Otherwise narrated by a TTS voice (note: macOS screen recording captures the mic, not computer audio). Start a screen recording (Cmd+Shift+5, Record Entire Screen), then run:
#   bash scripts/auto_demo.sh
# Narration uses macOS `say` (free, offline). Pages open in your default browser. Terminal + browser side by side.
set -uo pipefail
API=http://localhost:8080; WORLD=http://localhost:9210; T="Authorization: Bearer relay-demo-token"; C="$API/console/#"
VOICE_RATE=${VOICE_RATE:-190}; VOICE=${VOICE:-prabhat}   # prabhat (neural, natural) | Aman | Rishi (macOS)
# Voice: VOICE=prabhat (default) = Microsoft's free neural Indian-English male voice (natural, needs internet + edge-tts in .venv);
#        VOICE=Aman / Rishi = built-in macOS voices (offline). Sentences are spoken one at a time with short natural pauses.
EDGE=.venv/bin/edge-tts
speak_sentence() {
  if [ "$VOICE" = "prabhat" ] && [ -x "$EDGE" ]; then
    local f="/tmp/relay_tts_$$.mp3"
    if "$EDGE" --voice en-IN-PrabhatNeural --rate "${EDGE_RATE:--3%}" --text "$1" --write-media "$f" >/dev/null 2>&1; then
      afplay "$f"; rm -f "$f"; return
    fi
  fi
  say -v "$([ "$VOICE" = "prabhat" ] && echo Aman || echo "$VOICE")" -r "$VOICE_RATE" "$(echo "$1" | sed 's/AI/A.I./g')"
}
narrate() {
  if [ -n "${PRESENT:-}" ]; then   # presenter mode: YOU read the line aloud, then press Enter to continue
    printf '\n\033[1;33m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m\n'
    printf '\033[1;37m  SAY:  %s\033[0m\n' "$*"
    printf '\033[2m  (read it aloud, then press Enter)\033[0m'
    read -r _ </dev/tty
    return
  fi
  printf '\n\033[1;36m▶ %s\033[0m\n' "$*"
  [ -n "${DRY:-}" ] && return
  echo "$*" | python3 -c "import re,sys;[print(x) for x in re.split(r'(?<=[.!?])\s+', sys.stdin.read().strip()) if x]" | while IFS= read -r line; do
    speak_sentence "$line"; sleep 0.35
  done
}
# Navigate ONE browser tab (the active tab of the front window) instead of opening new tabs.
show() {
  [ -n "${DRY:-}" ] && { echo "   [open] $1"; return; }
  local u="$1" b
  for b in "Google Chrome" "Brave Browser" "Microsoft Edge" "Arc"; do
    if pgrep -xq "$b" 2>/dev/null || pgrep -fq "/$b.app/" 2>/dev/null; then
      osascript -e "tell application \"$b\" to set URL of active tab of front window to \"$u\"" >/dev/null 2>&1 && { sleep 1.5; return; }
    fi
  done
  if pgrep -xq Safari 2>/dev/null; then
    osascript -e "tell application \"Safari\" to set URL of current tab of front window to \"$u\"" >/dev/null 2>&1 && { sleep 1.5; return; }
  fi
  open "$u"; sleep 1.5   # fallback
}
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
narrate "Hi! This is Relay, an AI workflow orchestrator. You define a workflow as a set of connected steps. When it is triggered, the run is queued in Postgres, and a separate worker executes it, one step at a time. Each step is saved before it runs, so even if the worker crashes, nothing happens twice. And approvals are enforced by the engine itself, not by the AI."
show "$C/workflows"
narrate "Here are the four workflows that came with the project, already published. Every run keeps its own snapshot of the workflow, so editing a workflow never changes a run that is already in progress."

# 2. Normal triage through the webhook
R1=$(hook pay_001); wait_for "$R1" "succeeded|failed|waiting_approval" 60
show "$C/runs/$R1"
narrate "Let us send a normal customer complaint through the webhook. The AI step read the message and classified it as a complaint. You can see the tokens it used right here in the trace. Based on that answer, the run took the support branch, and posted a message to the support channel."

# 3. Prompt injection
R2=$(hook pay_inject_001); wait_for "$R2" "succeeded|failed|waiting_approval" 60
show "$C/runs/$R2"
narrate "Now, something sneaky. This message is a prompt injection. It claims that approval was already granted, and asks for a five thousand dollar refund. Relay flags it as suspicious. And notice, the run still stops and waits for a human. That is because the approval check lives in the engine, so nothing the AI says can skip it."
A2=$(pending_for "$R2"); curl -s -X POST "$API/approvals/$A2/reject" -H "$T" -H 'Content-Type: application/json' -d '{"decided_by":"maya","note":"injection attempt"}' >/dev/null
sleep 1; show "$C/runs/$R2"
narrate "I will reject it. The run is cancelled, my decision is recorded, and no refund ever happened."

# 4. Approve a real refund
R3=$(hook pay_002); wait_for "$R3" "waiting_approval|failed|succeeded" 60
A3=$(pending_for "$R3"); curl -s -X POST "$API/approvals/$A3/approve" -H "$T" -H 'Content-Type: application/json' -d '{"decided_by":"maya"}' >/dev/null
wait_for "$R3" "succeeded|failed" 60; show "$C/runs/$R3"
narrate "Here is a genuine refund request. A manager approves it, the run picks up where it paused, the refund goes through exactly once, and the customer gets an email."

# 5. Kill -9 and resume
curl -s -X POST "$WORLD/admin/reset" >/dev/null
R4=$(trig wf_slow_fulfillment '{"input":{"order_id":"ord_2003","customer_email":"lena@example.com"}}')
for _ in $(seq 1 30); do ledger_has email.send && break; sleep 0.5; done
show "$C/runs/$R4"
narrate "Now, the most important test. This order run has already sent its confirmation email, and is waiting out a twenty second delay. I am going to kill the worker, abruptly, right now."
docker compose kill -s SIGKILL worker; sleep 1
narrate "The worker is gone. Let us bring it back."
docker compose up -d worker >/dev/null 2>&1
wait_for "$R4" "succeeded|failed" 90; show "$C/runs/$R4"
python3 scripts/duplication_check.py --url "$WORLD"
narrate "And it picked up exactly where it left off, not from the beginning. The duplication check confirms that every email and shipment happened exactly once."

# 6. Outage and backoff
curl -s -X POST "$WORLD/admin/config" -d '{"mode":"down"}' >/dev/null
R5=$(trig wf_expense_approval '{"input":{"employee_email":"dev2@example.com","amount_usd":40,"description":"Team lunch"}}')
sleep 5; show "$C/runs/$R5"
narrate "What if an outside service goes down? Here, the email step keeps retrying, waiting a little longer each time, without ever sending duplicates."
curl -s -X POST "$WORLD/admin/config" -d '{"mode":"ok"}' >/dev/null
wait_for "$R5" "succeeded|failed" 60; show "$C/runs/$R5"
narrate "As soon as the service is back, the run finishes on its own."

# 7. Step cap
R6=$(trig wf_runaway '{"input":{}}'); wait_for "$R6" "failed|succeeded|cancelled" 60; show "$C/runs/$R6"
narrate "This workflow keeps checking an order that never ships, so it would loop forever. The engine stops it after twelve steps, and tells you exactly why."

# 8. Schema enforcement
R7=$(trig wf_ai_repair_demo '{"input":{"message":"My earbuds arrived with a cracked charging case and the left bud does not pair. I am really disappointed and want to know my options for a replacement or repair."}}')
wait_for "$R7" "succeeded|failed" 60; show "$C/runs/$R7"
narrate "AI answers are always checked against a schema. Here, the first answer was too long. Relay told the model exactly which rule it broke, the model fixed it, and only the valid answer moved forward."

# 9. NL compiler
show "$C/build"
curl -s -X POST "$API/workflows/compile" -H "$T" -H 'Content-Type: application/json' -d '{"description":"Every day at 9am, scan our Postgres database for new signups and update each record in Salesforce.","save":false}' | python3 -m json.tool | head -12
narrate "You can also describe a workflow in plain English, and Relay builds it for you. But if you ask for something it cannot do, like connecting to a database or Salesforce, it politely refuses, and tells you what is missing. On the evaluation set, it scored thirteen out of fifteen."

# 10. Smoke test
if [ -n "${PRESENT:-}" ]; then narrate "Finally, the official smoke test."; else narrate "Finally, the official smoke test." & fi
python3 scripts/smoke_test.py --url "$API" --token relay-demo-token --world "$WORLD" | tail -3
narrate "All twenty eight checks pass, along with forty three automated tests. Everything, including the verification report, is in the pull request. Thanks for watching!"
