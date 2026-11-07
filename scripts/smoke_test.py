#!/usr/bin/env python3
"""Smoke test for a Relay platform. Zero dependencies (Python 3.9+ stdlib).

Checks the required API contract and run lifecycle:
  1. seed workflows are loaded and published
  2. create + publish + trigger a minimal workflow; the run succeeds with a step trace
  3. publish validation rejects broken definitions (unknown type, missing param, bad reference)
  4. webhook secret enforcement (X-Relay-Secret)
  5. approval lifecycle on wf_expense_approval: pause -> approve -> succeed; small amounts skip the gate
  6. wf_runaway is stopped by the max-steps cap
  7. run statuses stay within the documented vocabulary

Prerequisites: your engine running with the seed data loaded, a worker running,
and the mock world up (scripts/mock_world.py) so notify steps can execute.

Usage:
    python3 smoke_test.py --url http://localhost:8080 [--token <demo-token>] [--world http://localhost:9210]

AI nodes are NOT exercised here (wf_support_triage needs a real model); they are
covered by your NL eval and demo. Passing this test is the baseline, not the goal:
it does not check durability (kill-and-resume), idempotency, or injection handling.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

STATUS_VOCAB = {"queued", "running", "waiting_approval", "succeeded", "failed", "cancelled"}
RESULTS = []
SEEN_STATUSES = set()


def record(name, ok, detail="", soft=False):
    tag = "PASS" if ok else ("WARN" if soft else "FAIL")
    RESULTS.append((tag, name, detail))
    print(f"  {tag:4}  {name}" + (f"  ({detail})" if detail else ""))


class Client:
    def __init__(self, base, token):
        self.base = base.rstrip("/")
        self.token = token

    def request(self, method, path, body=None, headers=None):
        hdrs = {"Content-Type": "application/json"}
        if self.token:
            hdrs["Authorization"] = f"Bearer {self.token}"
        hdrs.update(headers or {})
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode(errors="replace")
                return resp.status, json.loads(raw) if raw.strip() else {}
        except urllib.error.HTTPError as e:
            raw = e.read().decode(errors="replace")
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = {"_raw": raw}
            return e.code, parsed
        except (urllib.error.URLError, TimeoutError) as e:
            return 0, {"_transport_error": str(e)}


def get_run_id(body):
    for key in ("run_id", "id", "runId"):
        if isinstance(body, dict) and body.get(key):
            return body[key]
    return None


def poll_run(client, run_id, until_statuses, timeout_s=60):
    """Polls GET /runs/{id} until status is in until_statuses or timeout. Returns (status, body)."""
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        code, body = client.request("GET", f"/runs/{run_id}")
        if code == 200 and isinstance(body, dict):
            status = body.get("status")
            if status:
                SEEN_STATUSES.add(status)
                last = body
                if status in until_statuses:
                    return status, body
        time.sleep(1.0)
    return (last or {}).get("status"), last or {}


def minimal_workflow(world_url):
    wid = f"wf_smoke_{uuid.uuid4().hex[:8]}"
    return wid, {
        "id": wid,
        "name": "Smoke minimal",
        "trigger": {"type": "manual"},
        "entry": "hello",
        "limits": {"max_steps": 5, "timeout_seconds": 60, "max_ai_tokens": 0},
        "nodes": [
            {"id": "hello", "type": "notify",
             "params": {"channel": "chat", "to": "#smoke", "message": f"smoke {wid}"},
             "next": None}
        ],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="Relay base URL, e.g. http://localhost:8080")
    parser.add_argument("--token", default=None, help="demo auth token, sent as a bearer token")
    parser.add_argument("--world", default="http://localhost:9210", help="mock world base URL (for ledger cross-checks)")
    args = parser.parse_args()

    client = Client(args.url, args.token)
    data_dir = Path(__file__).resolve().parents[1] / "data"
    seeds = json.loads((data_dir / "seed_workflows.json").read_text())["workflows"]
    seed_by_id = {w["id"]: w for w in seeds}

    print("\n[1] Seed workflows loaded and published")
    code, body = client.request("GET", "/workflows")
    record("GET /workflows returns 200", code == 200, f"got {code}")
    listed = body if isinstance(body, list) else body.get("workflows", []) if isinstance(body, dict) else []
    ids = {w.get("id") for w in listed if isinstance(w, dict)}
    for wid in ("wf_support_triage", "wf_expense_approval", "wf_slow_fulfillment", "wf_runaway"):
        record(f"seed {wid} present", wid in ids)
    published = {w.get("id") for w in listed if isinstance(w, dict) and str(w.get("status", "")).lower() == "published"}
    record("seeds are published", all(w in published for w in seed_by_id), soft=True,
           detail="expected status 'published' on seed workflows")

    print("\n[2] Create, publish, trigger a minimal workflow")
    wid, definition = minimal_workflow(args.world)
    code, body = client.request("POST", "/workflows", definition)
    record("create draft returns 2xx", 200 <= code < 300, f"got {code}")
    code, body = client.request("POST", f"/workflows/{wid}/publish")
    record("publish returns 2xx", 200 <= code < 300, f"got {code}")
    code, body = client.request("POST", f"/workflows/{wid}/trigger", {"input": {}})
    run_id = get_run_id(body)
    record("trigger returns a run id", bool(run_id), f"got {code}: {str(body)[:80]}")
    if run_id:
        status, run = poll_run(client, run_id, {"succeeded", "failed", "cancelled"}, timeout_s=30)
        record("run reaches succeeded", status == "succeeded", f"got {status}")
        steps = run.get("steps") or []
        record("trace has a step for node 'hello'",
               any(s.get("node_id") == "hello" for s in steps if isinstance(s, dict)),
               f"{len(steps)} steps")
        # cross-check the side effect landed in the world's ledger
        try:
            with urllib.request.urlopen(f"{args.world.rstrip('/')}/admin/ledger", timeout=5) as resp:
                entries = json.load(resp)["entries"]
            hit = any(e["action"] == "chat.message" and wid in json.dumps(e["payload"]) for e in entries)
            record("notify step visible in mock world ledger", hit, soft=True)
            keyed = any(e["action"] == "chat.message" and wid in json.dumps(e["payload"]) and e["idempotency_key"] for e in entries)
            record("notify carried an Idempotency-Key", keyed, soft=True,
                   detail="required for exactly-once recovery")
        except (urllib.error.URLError, TimeoutError, KeyError):
            record("mock world ledger reachable", False, "skipped cross-check", soft=True)

    print("\n[3] Publish validation rejects broken definitions")
    bad_cases = [
        ("unknown node type", {"id": "n1", "type": "teleport", "params": {}, "next": None}),
        ("missing required param", {"id": "n1", "type": "notify", "params": {"channel": "chat"}, "next": None}),
        ("reference to nonexistent node", {"id": "n1", "type": "delay", "params": {"seconds": 1}, "next": "ghost"}),
    ]
    for label, node in bad_cases:
        bad_id = f"wf_bad_{uuid.uuid4().hex[:8]}"
        bad = {"id": bad_id, "name": f"bad: {label}", "trigger": {"type": "manual"},
               "entry": "n1", "limits": {"max_steps": 5, "timeout_seconds": 60, "max_ai_tokens": 0},
               "nodes": [node]}
        code, _ = client.request("POST", "/workflows", bad)
        if not (200 <= code < 300):
            record(f"rejects {label} (at create)", True, f"got {code}")
            continue
        code, body = client.request("POST", f"/workflows/{bad_id}/publish")
        record(f"rejects {label} (at publish)", 400 <= code < 500, f"got {code}")

    print("\n[4] Webhook secret enforcement")
    secret = seed_by_id["wf_expense_approval"]["trigger"]["secret"]
    payload_small = {"employee_email": "dev2@example.com", "amount_usd": 40, "description": "Team lunch"}
    code, _ = client.request("POST", "/hooks/wf_expense_approval", payload_small,
                             headers={"X-Relay-Secret": "wrong-secret"})
    record("wrong secret rejected 401/403", code in (401, 403), f"got {code}")
    code, body = client.request("POST", "/hooks/wf_expense_approval", payload_small,
                                headers={"X-Relay-Secret": secret})
    small_run = get_run_id(body)
    record("correct secret accepted with a run id", 200 <= code < 300 and bool(small_run), f"got {code}")

    print("\n[5] Approval lifecycle (wf_expense_approval)")
    if small_run:
        status, _ = poll_run(client, small_run, {"succeeded", "failed", "cancelled", "waiting_approval"}, timeout_s=30)
        record("small expense auto-approves (no gate)", status == "succeeded", f"got {status}")
    payload_large = {"employee_email": "dev1@example.com", "amount_usd": 250, "description": "Conference ticket"}
    code, body = client.request("POST", "/hooks/wf_expense_approval", payload_large,
                                headers={"X-Relay-Secret": secret})
    large_run = get_run_id(body)
    record("large expense triggered", bool(large_run), f"got {code}")
    if large_run:
        status, _ = poll_run(client, large_run, {"waiting_approval", "succeeded", "failed"}, timeout_s=30)
        record("run pauses in waiting_approval", status == "waiting_approval", f"got {status}")
        code, body = client.request("GET", "/approvals?status=pending")
        approvals = body if isinstance(body, list) else body.get("approvals", []) if isinstance(body, dict) else []
        match = next((a for a in approvals if isinstance(a, dict) and a.get("run_id") == large_run), None)
        record("pending approval listed for the run", match is not None, f"{len(approvals)} pending")
        if match:
            code, _ = client.request("POST", f"/approvals/{match.get('id')}/approve")
            record("approve returns 2xx", 200 <= code < 300, f"got {code}")
            status, _ = poll_run(client, large_run, {"succeeded", "failed", "cancelled"}, timeout_s=30)
            record("approved run resumes and succeeds", status == "succeeded", f"got {status}")

    print("\n[6] Run caps stop wf_runaway")
    code, body = client.request("POST", "/workflows/wf_runaway/trigger", {"input": {}})
    runaway = get_run_id(body)
    record("wf_runaway triggered", bool(runaway), f"got {code}")
    if runaway:
        status, run = poll_run(client, runaway, {"failed", "cancelled", "succeeded"}, timeout_s=90)
        record("runaway run is stopped (failed/cancelled)", status in ("failed", "cancelled"), f"got {status}")
        reason = json.dumps(run).lower()
        record("stop reason mentions the cap", any(w in reason for w in ("max_steps", "max steps", "step cap", "step limit", "cap")),
               soft=True, detail="expected a cap-exceeded reason in the run record")

    print("\n[7] Status vocabulary")
    unknown = SEEN_STATUSES - STATUS_VOCAB
    record("all observed statuses in documented set", not unknown,
           f"unexpected: {sorted(unknown)}" if unknown else f"saw {sorted(SEEN_STATUSES)}")

    fails = [r for r in RESULTS if r[0] == "FAIL"]
    warns = [r for r in RESULTS if r[0] == "WARN"]
    print(f"\n{'=' * 56}")
    print(f"  {len(RESULTS) - len(fails) - len(warns)} passed, {len(warns)} warnings, {len(fails)} failed")
    if fails:
        print("  Failed checks:")
        for _, name, detail in fails:
            print(f"    - {name}" + (f" ({detail})" if detail else ""))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
