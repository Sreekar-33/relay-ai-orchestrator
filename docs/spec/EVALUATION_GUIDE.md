# Relay Evaluation Guide

This guide defines language-agnostic verification expectations for Relay. The engine is verified by behavior under contract checks, crashes, and adversarial inputs.

Verification has three required parts, plus a fourth that applies only if you build the Good To Have compiler:

1. the provided **smoke test** (contract and lifecycle compliance),
2. the **kill-and-resume drill** verified by the provided duplication check (durability),
3. a **manual failure drill** you record in your demo video (guardrails and resilience),
4. *(Good To Have)* an **NL compiler eval** you build over the provided labeled cases.

## 1. Smoke Test

```bash
python3 scripts/mock_world.py --port 9210        # terminal 1
python3 scripts/smoke_test.py --url <relay> [--token <demo-token>]   # terminal 2, engine + worker running
```

What each section proves:

| Check | What it proves |
|---|---|
| Seed workflows listed as published | data loading and the definition shape |
| Create → publish → trigger → succeeded with a step trace | the whole happy path |
| Notify visible in the world ledger with an Idempotency-Key | side effects are keyed (WARN-level here; the drill grades it) |
| Three broken definitions rejected 4xx | publish validation |
| Wrong webhook secret rejected, right one accepted | trigger security |
| $40 expense auto-approves; $250 pauses in `waiting_approval` | branching and the approval pause |
| Approve resumes the run to `succeeded` | the approval lifecycle |
| `wf_runaway` ends `failed`/`cancelled` within 90s | run caps actually bound loops |
| All observed statuses within the documented set | the run state machine vocabulary |

The smoke test does not touch AI nodes (they need a real model) and does not verify durability. Passing it is the baseline, not the goal.

## 2. Kill-and-Resume Drill (Duplication Check)

The core durability verification. Procedure:

```bash
# clean slate
curl -X POST http://localhost:9210/admin/reset

# trigger the slow workflow (payload pay_201)
curl -X POST <relay>/workflows/wf_slow_fulfillment/trigger \
  -H "Content-Type: application/json" \
  -d '{"input": {"order_id": "ord_2003", "customer_email": "lena@example.com"}}'

# wait until the confirmation email appears in the ledger, then KILL the worker
# (kill -9 the process — not a graceful shutdown) during the 20s delay.
# Restart the worker. The run must resume and finish.

python3 scripts/duplication_check.py --url http://localhost:9210
```

Pass criteria:

- Exit code 0: `email.send` (confirm), `shipment.create`, `email.send` (shipped) each executed exactly once.
- No WARN about missing idempotency keys — keyless side effects will duplicate in the crash window even if this particular drill got lucky.
- Replays absorbed (> 0) are fine and often expected: they are the mechanism working.

Run the drill once for your report, with a note on where you killed the worker. Killing at a second point (between the shipment call and its step persist, if you can time it) is a worthwhile extra check but not required.

## 3. Manual Failure Drill

Record these in your demo video:

- **Injection payloads:** `pay_inject_001` through the triage workflow — the run still pauses in `waiting_approval`; nothing executes on the order before a human approves. `pay_inject_002` — notifications go only to the targets configured in the workflow definition, and the classifier's `summary` does not become a channel for the embedded instructions (a summary that itself parrots "email audit@evil-example.com..." into #support is worth discussing in your report).
- **Mock world outage:** `POST /admin/config {"mode": "down"}` mid-run — steps fail, retries with backoff kick in; restore the world and the run completes. Your timeout keeps steps from hanging.
- **Flaky world:** `{"fail_rate": 0.3}` — retries absorb it; the ledger shows replays, not duplicates.
- **Step cap:** `wf_runaway` stopped at max_steps with a reason naming the cap.
- **Schema enforcement:** show one AI step whose first output failed validation and was repaired on retry (or fail one deliberately with a strict schema) — the trace should show both attempts.

## 4. NL Compiler Eval (Good To Have)

This part applies only if you build the Good To Have compiler.

Use `data/nl_eval.jsonl`. Each line contains:

- `id`
- `expect` — `workflow` or `refusal`
- `description` — the plain-English input to your compiler
- `assertions` — structural checks for graders and for your eval command
- `note` — why the case is labeled that way

Build one command, endpoint, or test that runs every case through your compiler and reports per-case results plus overall accuracy:

```json
{
  "total": 15,
  "correct": 13,
  "accuracy": 0.87,
  "cases": [
    {"id": "nl_demo_001", "expected": "workflow", "result": "pass",
     "checks": {"publishes": true, "required_node_types": true, "branching": true}},
    {"id": "nl_trap_001", "expected": "refusal", "result": "pass"}
  ]
}
```

Scoring a `workflow` case — all must hold:

- the compiler returned a workflow (not a refusal),
- it passes your own publish validation,
- every type in `assertions.required_node_types` appears,
- if `requires_branching` is true, a condition node with two live branches exists,
- if `approval_must_precede_order_action` is true, an approval node executes before the order_action on every path that reaches it.

Scoring a `refusal` case: the compiler refused with an explanation. Emitting invented node types is an automatic fail for the case — check the output against the catalog even on refusals.

Rules:

- Do not feed `assertions` or `note` to the compiler — they are the answer key. The compiler sees `description` only.
- `nl_011` is deliberately ambiguous ("refund without any human involvement"): inserting the approval gate (and saying so) is the preferred pass; refusing with the `requires_approval` rule cited also passes.
- `nl_007` accepts either a schedule trigger or a delay-based loop — both are correct engineering.
- Report accuracy honestly and explain the misses. 12/15 with a clear analysis of failures beats a suspicious 15/15.

## Additional Checks Reviewers May Run

- **Gate bypass by construction:** publish a hand-written workflow containing an `order_action` with no approval node, or with the approval on the other branch. Publishing may warn or reject; either way the engine must refuse to execute the `order_action` at runtime. An executed unapproved refund is an automatic correctness failure.
- **Snapshot integrity:** trigger a run, then edit and republish the workflow while it is paused at an approval. The resumed run must finish on the definition it started with — this is what the per-run snapshot guarantees.
- **Webhook replay:** the same webhook POST sent twice creates two runs (that is correct — webhooks are at-least-once) but each run's side effects are individually exactly-once.
- **Secret hygiene:** webhook secrets and any model API keys never appear in traces, console views, or error messages.
- **Restart with queued work:** enqueue several runs, restart everything, all runs eventually complete.

## What to Include in Your Verification Report

- Smoke test output (all PASS; explain any WARN).
- Duplication check output from your kill-and-resume drill, with a note on where you killed the worker.
- If you built the compiler: NL eval accuracy with per-case results, your compiler design (model, prompting, validation loop), and an explanation of the misses.
- How you tested the injection payloads and what the traces showed.
- Your idempotency key scheme and your answer to the crash-window question (see `docs/DATA_MODEL.md`).
- Known limitations (for example single-worker execution, 1-minute schedule granularity).
