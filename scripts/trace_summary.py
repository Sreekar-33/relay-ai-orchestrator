#!/usr/bin/env python3
"""Pretty-print a run trace: GET /runs/{id} JSON on stdin -> one line per step."""
import json
import sys

run = json.load(sys.stdin)
print(f"run {run['run_id']} status={run['status']} steps_executed={run['steps_executed']} error={run.get('error')}")
for s in run["steps"]:
    attempts = [a.get("outcome", "?") + (" (replayed)" if a.get("replayed") else "") for a in s.get("attempts") or []]
    key = f"  key={s['idempotency_key']}" if s.get("idempotency_key") else ""
    print(f"  #{s['sequence']:<3} {s['node_id']:<18} {s['type']:<13} {s['status']:<10} attempts={attempts}{key}")
