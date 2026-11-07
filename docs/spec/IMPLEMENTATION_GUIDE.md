# Relay Implementation Guide

This guide gives you a practical path through the capstone. It does not add new requirements. The source of truth for scope is `RELAY_PROBLEM_STATEMENT.md`.

## Must Have Checklist

Before working on Good To Have or Stretch items, make sure you can check off every item below:

- [ ] Load the node catalog and the four seed workflows as Published.
- [ ] Create, list, publish workflows in the seed JSON shape; publish validation rejects broken definitions; runs snapshot the definition at trigger time.
- [ ] Trigger runs via webhook (with `X-Relay-Secret`) and the manual API.
- [ ] Execute `http_request`, `condition`, `delay`, and `notify` nodes with template resolution.
- [ ] Execute `ai` nodes with schema validation and one repair retry.
- [ ] Runs execute on a worker loop via a durable queue; the API never runs a workflow inline.
- [ ] Kill the worker mid-run; a restarted worker resumes from the last completed step.
- [ ] Side-effect calls carry stable idempotency keys; `scripts/duplication_check.py` passes after the drill.
- [ ] `approval` nodes pause runs; approve/reject APIs resume/cancel; `order_action` is engine-gated on approval.
- [ ] Per-node retries with backoff; the max_steps cap stops `wf_runaway`.
- [ ] Run traces show resolved input, output, attempts, timing, and AI token usage per step.
- [ ] A simple console shows workflows, run history, a trace view, and pending approvals.
- [ ] `scripts/smoke_test.py` passes.

Once every box is checked, the NL compiler (Step 8 below) is the highest-value Good To Have to attempt first.

## Suggested Build Order

### Step 1: Run the Mock World

```bash
python3 scripts/mock_world.py --port 9210
```

Curl every endpoint once — send an email twice with the same `Idempotency-Key` and look at `GET /admin/ledger`. The replay mechanics you just observed are the heart of Must Have 6.

### Step 2: Workflow CRUD and Validation

- Load `data/node_catalog.json` and `data/seed_workflows.json`.
- Implement create/list/get, publish with validation against the catalog, and a per-run definition snapshot at trigger time.
- Validation errors are worth real effort here — the smoke test checks three specific rejection cases, and the Good To Have NL compiler reuses this validator if you attempt it.

### Step 3: Engine v1 (Happy Path)

- A durable queue (a database table is fine) and a worker loop.
- Manual trigger → run → execute nodes following `next`/`on_true`/`on_false` → persist each step.
- Template resolution (`{{trigger.body.x}}`, `{{nodes.id.output.y}}`) with clear errors for unresolvable references.
- Get `wf_expense_approval`'s auto-approve branch (payload `pay_102`) working end to end against the mock world.

### Step 4: Durability

- Persist step completion before advancing; on worker startup, reclaim in-flight runs and resume from the last completed step.
- Idempotency keys (`{run_id}:{node_id}`) on every side-effect call.
- Now run the kill-and-resume drill on `wf_slow_fulfillment` for the first time. Expect to find a bug; that is the point of doing it early.

### Step 5: Approvals, Retries, Caps

- The `approval` node: pause, pending-approvals API, approve/reject, resume.
- Engine-level gating of `requires_approval` nodes on an approval record.
- Retries with exponential backoff (test with `{"fail_rate": 0.3}` on the mock world).
- The max_steps cap; verify with `wf_runaway`.
- The webhook trigger if you have not added it yet.

### Step 6: AI Nodes

- Provider adapter (real model behind it in production, fake in tests).
- Prompt template → model → parse → validate against `output_schema` → one repair retry with the validation error → fail cleanly.
- Run `wf_support_triage` with `pay_001`, `pay_002`, then the injection payloads. Watch the traces.
- Record token usage per step in the trace.

### Step 7: Console

- Workflows, run history, trace view, pending approvals with buttons. A plain server-rendered page or a small SPA — either is fine, and you may vibe-code it.

### Step 8 (Good To Have): NL Compiler and Eval

Attempt this only after the Must Have checklist is green.

- Compiler = prompt(model) → candidate definition → your Step 2 validator → repair loop → publishable draft or refusal.
- Give the model the catalog (types, params, rules) in the prompt; instruct it to refuse when capabilities are missing rather than improvise.
- Build the eval command over `data/nl_eval.jsonl` immediately and rerun it on every prompt tweak — that is the workflow the capstone wants you to practice.

### Step 9: Verify

- Smoke test green; a kill-and-resume drill with duplication check output; automated tests for validation, templates, schema enforcement, idempotency. (Plus the NL eval report if you built Step 8.)

## Recommended Demo Scenarios

### Scenario 1: AI Triage with an Injection Attempt

- `pay_001` → classified `complaint`, `#support` notified. Show the trace: AI input, validated output, the branch decision.
- `pay_inject_001` → whatever the classifier says, the run parks in `waiting_approval`. Show the pending approval, reject it, show the order untouched in the mock world.

### Scenario 2: Kill and Resume

- Reset the world, trigger `wf_slow_fulfillment` (`pay_201`), `kill -9` the worker during the delay, restart, run `duplication_check.py` → PASS on camera.

### Scenario 3 (Good To Have): Compile from English

- Compile `nl_demo_001`, show the generated draft JSON, publish, trigger with a sample body, show the run.
- Compile `nl_trap_001`, show the refusal naming the missing capabilities.
- Run the eval command, show the accuracy table.

### Scenario 4: Caps and Chaos

- `wf_runaway` → stopped at step 12 with a cap reason.
- `{"mode": "down"}` on the world mid-run → retries/backoff in the trace → restore → run completes.

## FAQ

### Do I need a real AI provider?

For `ai` nodes in demos (and the NL compiler, if you build it): yes, but free-tier (Gemini, Groq) or local (Ollama) is fine. For engine tests: no — the adapter can return canned JSON, and `scripts/mock_provider.py` exists if you want an HTTP-level fake.

### Can the worker run inside the API process?

Yes. A background worker loop in the same process is acceptable — execution just must never happen in the request handler, and queued or mid-flight runs must survive a full process restart (the kill drill kills the whole process). A separate worker process is the cleaner architecture and worth doing if time allows.

### How sophisticated must template resolution be?

Path lookups (`{{trigger.body.x.y}}`, `{{nodes.id.output.z}}`) interpolated into strings. No expressions, no filters, no arithmetic. An unresolvable path should fail the step with a clear error, not silently render an empty string.

### What exactly is the idempotency key?

`{run_id}:{node_id}` (any stable equivalent works). Stable across retries and resumes; NOT including the attempt number. If a node legitimately executes twice because the run looped back to it, include the step sequence — loop iterations are distinct effects; retries of one iteration are not.

### How do I resume a run safely?

From persisted steps: find the last completed step, resume from its `next` edge. For the node that may have been in flight when the worker died, re-execute it with the same idempotency key — the mock world absorbs the replay if the first attempt landed.

### What stops the AI from approving its own refund?

Nothing the AI outputs can create an approval record. The gate is a database check in the engine (`requires_approval` node → look for an approved Approval row for this run). The injection payloads exist to prove this holds.

### What if the model returns invalid JSON twice?

The step fails, normal failure handling applies (it is not a transient error — do not burn retries re-asking identically), and the run ends `failed` with the validation error in the trace.

### Does the Good To Have NL compiler have to produce perfect workflows?

No — it has to produce *valid* ones (your publish validator is the referee) and refuse what the catalog cannot express. The eval rewards structural correctness, not stylistic elegance. Report your misses honestly.

### Can delays be actual sleeps?

For Must Have with a single worker, a sleep is acceptable if a worker crash during the delay still resumes correctly (persist before the delay starts). A `resume_at` timestamp the worker polls is the more durable design and worth the small extra effort.

### How precise do Good To Have schedule triggers need to be?

1-minute granularity, no catch-up for missed windows. A worker loop checking `next_fire_at <= now` every few seconds is fine.

### Can I change the API routes?

The fixed contract in `docs/API_CONTRACT.md` (the routes the smoke test drives, the definition shape, the status set, `X-Relay-Secret`) must match. Everything else is yours if documented.

### Does the console need authentication or polish?

A demo token is fine. Polish is not graded; the console exists to make traces and approvals visible in your demo.

### What should be in the final README?

Include:

- setup instructions and environment variables (model provider config, mock world URL),
- how to seed the workflows and start the API, worker, and mock world,
- API overview,
- how to run the smoke test and the drill (and your NL eval, if you built the compiler),
- architecture/design decisions (run state machine, queue choice, idempotency scheme),
- known limitations.
