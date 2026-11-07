#!/usr/bin/env python3
"""Exactly-once verifier for the Relay capstone. Zero dependencies (Python 3.9+ stdlib).

Reads the mock world's ledger and checks that no side effect executed twice.
Run it after the kill-and-resume drill:

    1. POST /admin/reset on the mock world (clean slate)
    2. Trigger wf_slow_fulfillment (payload pay_201)
    3. Kill the worker during the 20s delay; restart it; let the run finish
    4. python3 duplication_check.py --url http://localhost:9210

A duplicate is two *successfully executed* (non-replayed, 2xx) ledger entries
with the same action and payload. Replayed entries are good news, not
duplicates — they mean your engine retried with the same Idempotency-Key and
the world absorbed it. Rejected calls (4xx/5xx) executed no side effect and
are not counted.

Exit code 0 = clean, 1 = duplicates found.

Note: two intentionally identical sends (same action, same payload, by design)
would be flagged — that is why the drill starts from /admin/reset. Use --since
to scope the check to entries after a known sequence number instead.
"""

import argparse
import json
import sys
import urllib.request
from collections import Counter, defaultdict


def canonical(payload):
    return json.dumps(payload, sort_keys=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:9210", help="mock world base URL")
    parser.add_argument("--since", type=int, default=0, help="only check ledger entries with seq > SINCE")
    args = parser.parse_args()

    with urllib.request.urlopen(f"{args.url.rstrip('/')}/admin/ledger?since={args.since}", timeout=10) as resp:
        entries = json.load(resp)["entries"]

    executed = [e for e in entries if not e["replayed"] and 200 <= e.get("status", 200) < 300]
    replayed = [e for e in entries if e["replayed"]]
    rejected = [e for e in entries if not e["replayed"] and not 200 <= e.get("status", 200) < 300]

    groups = defaultdict(list)
    for e in executed:
        groups[(e["action"], canonical(e["payload"]))].append(e)

    duplicates = {k: v for k, v in groups.items() if len(v) > 1}

    keyless = [e for e in executed if not e["idempotency_key"]]

    print(f"Ledger entries checked: {len(entries)} (executed: {len(executed)}, replays absorbed: {len(replayed)}, rejected: {len(rejected)})")
    action_counts = Counter(e["action"] for e in executed)
    for action, count in sorted(action_counts.items()):
        print(f"  {action}: {count} executed")

    if keyless:
        print(f"\nWARN: {len(keyless)} executed side effect(s) carried no Idempotency-Key:")
        for e in keyless[:10]:
            print(f"  seq {e['seq']}: {e['action']} {canonical(e['payload'])[:100]}")
        print("  Without a key, a crash between execution and acknowledgment WILL duplicate on resume.")

    if duplicates:
        print(f"\nFAIL: {len(duplicates)} duplicated side effect(s):")
        for (action, payload), copies in duplicates.items():
            seqs = ", ".join(str(e["seq"]) for e in copies)
            print(f"  {action} executed {len(copies)}x (seq {seqs}): {payload[:120]}")
        print("\nYour engine re-executed a completed side effect. Resume must skip completed steps,")
        print("and retries must reuse the same Idempotency-Key.")
        sys.exit(1)

    print("\nPASS: every side effect executed exactly once.")
    sys.exit(0)


if __name__ == "__main__":
    main()
