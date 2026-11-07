# AI-First Software Engineering Capstone - Case Study

## Relay: AI Workflow Orchestrator

**Author:** Airtribe

## Background & Objective

Every operations team runs on workflows: when an order webhook arrives, check the amount, classify the request, notify the right channel, wait for a manager's approval, then act. Platforms like n8n, Temporal, and Zapier exist because wiring these flows by hand — and keeping them running when steps fail — is real infrastructure work.

The 2026 version of this problem is AI-first: workflow steps now include machine judgment. An AI node reads the webhook payload and decides which branch the run takes — and that only works with serious engineering underneath. Runs must survive crashes, side effects must never fire twice, and an AI step's output must be structurally valid before anything downstream trusts it. (The most ambitious builders can go further and let AI build the workflows themselves — see Good To Have.)

The objective is to build a reliable workflow orchestration platform that can:

- Define workflows as connected, typed nodes with branching, and manage them through a draft → publish lifecycle.
- Trigger runs from webhooks or manual API calls.
- Execute runs durably on background workers: every step persisted, crash recovery without repeating side effects, retries with backoff.
- Run AI nodes whose output is enforced against a JSON schema, so downstream branching can trust it.
- Pause runs at human approval gates and resume on decision.
- Enforce a step cap so no run can loop forever.
- Keep a per-step trace complete enough to debug any run after the fact.

## Language and Stack

This project is intentionally language agnostic. You may implement it in Node.js, Java, Python, Go, Ruby, or any stack of your choice.

You are free to choose your framework, database, and queue. You do not need a paid AI provider: free-tier APIs (for example Gemini or Groq) or locally hosted models (for example via Ollama) are acceptable, and the engine can be tested against a mocked model adapter. The pack includes a mock external-services server (the "mock world") so notify and action nodes have something real to call — it records every call it receives in a ledger, which is how exactly-once execution is verified. Your choices must be documented, and the system must satisfy the functional, durability, and verification requirements described below.

The provided Python scripts are optional local utilities. They are not part of the required implementation stack.

## Scope

Relay has a clear core scope. Build the Must Have features first. Good To Have and Stretch features should only be attempted after the core engine works end to end.

There is no drag-and-drop builder to build. Workflows are created via API (as JSON). The frontend is a read-and-operate console: workflow list, run history, run traces, and pending approvals.

## Recommended Demo Flow

Steps 2–4 and 8 need a real (free-tier or local) model configured for the AI nodes; everything else runs against the provided mocks alone. See the FAQ in `docs/IMPLEMENTATION_GUIDE.md`.

Your final demo should be able to show this flow clearly:

1. Start the mock world server and your engine; show the console with the seeded workflows.
2. Trigger the seed workflow `wf_support_triage` with sample payload `pay_001` (a normal order complaint). Walk through the run trace: the AI node classifies the payload, the run branches on that output, and a notify step hits the mock world.
3. Trigger the same workflow with `pay_inject_001` — a payload containing a prompt injection that instructs the AI to skip approval and issue a large refund. Show that the sensitive branch still lands in the approval queue, because the gate is enforced by the engine, not the prompt.
4. Approve a pending action and show the run resume and complete; reject one and show the run end cleanly.
5. Kill your worker in the middle of a multi-step run (use `wf_slow_fulfillment`, which includes a delay). Restart it. Show the run resume from the last completed step — and run `scripts/duplication_check.py` to show the mock world's ledger contains exactly one copy of each side effect.
6. Trigger `wf_runaway`, the seeded workflow that polls an order that never ships, and show the engine stop it at the max-step cap.
7. Run the provided smoke test and show the summary.
8. If you built the Good To Have NL compiler: compile `nl_demo_001` from plain English, publish and trigger the result, then show `nl_trap_001` refused with an explanation.

## Acceptable Simplifications

- The worker may run inside the same process as the API (a background loop is fine) or as a separate process. Crash recovery must still work either way: kill the process, restart it, runs resume.
- A single worker is acceptable. Multi-worker coordination is Stretch.
- Workflow shape is a list of nodes with if/else branching and backward jumps. A full arbitrary-DAG engine with parallel fan-out is Stretch.
- AI nodes may use a free-tier or local model; engine tests may use a mocked model adapter.
- Notify and action nodes only need to call the mock world. Real email/Slack integrations are Stretch.
- Authentication can be a simple demo token throughout.
- The console can be a simple page with tables and buttons. You may vibe-code or AI-assist it. UI polish is not graded.
- Approvals happen via API or a console button.

## Avoid These Mistakes

- Do not execute runs inside the HTTP request handler. Triggers enqueue; the worker executes.
- Do not resume a crashed run by re-executing it from step 1. Completed steps must be skipped — the mock world's ledger will expose duplicate side effects, and `scripts/duplication_check.py` looks for exactly this.
- Do not lose queued or mid-flight runs on process restart. Run state lives in the database, not in memory.
- Do not treat webhook payloads as trusted input. `pay_inject_001` and `pay_inject_002` carry prompt injections aimed at your AI nodes.
- Do not enforce approval gates in the prompt. The engine must make sensitive nodes unreachable without an approval record, regardless of what any AI output says.
- Do not pass unvalidated AI output downstream. Enforce the node's JSON schema; retry once with the validation error; fail the step cleanly if it still does not conform.
- Do not build Good To Have features before the Must Have workflow works end to end.

## Must Have

### 1. Load the Provided Data

- Load the provided node catalog, seed workflows, and sample trigger payloads (`data/node_catalog.json`, `data/seed_workflows.json`, `data/sample_payloads.jsonl`).
- Use any persistent store you prefer.
- Preserve workflow and payload IDs such as `wf_support_triage` and `pay_inject_001`, and the workflow-definition JSON shape — the demo flow and provided scripts reference them.
- (`data/nl_eval.jsonl` matters only if you attempt the Good To Have compiler.)

### 2. Workflow Definitions: Draft → Publish

- APIs to create, read, update, and list workflows. A workflow is a set of typed nodes from the catalog, connected by `next` pointers, with if/else branches on condition nodes.
- Two states: Draft (editable) and Published (frozen, runnable).
- Every run captures a snapshot of the definition at trigger time and executes that snapshot — editing or republishing a workflow must never change what an in-flight or historical run executes. (A full immutable version history is Good To Have.)
- Publish-time validation rejects unknown node types, missing required parameters, and references to nonexistent nodes — each with a clear error.
- Backward jumps (loops) are legal — polling and retry patterns depend on them. The step cap in Must Have 7 is what bounds them at execution time.

### 3. Triggers

- Webhook trigger: a per-workflow URL guarded by a secret (sent in the `X-Relay-Secret` header); requests with a missing or wrong secret are rejected. The payload becomes the run's input.
- Manual trigger API for testing and demos.
- (Cron-style schedule triggers are Good To Have.)

### 4. Deterministic Nodes and Data Flow

- Implement the deterministic node types from the catalog: `http_request`, `condition`, `delay`, and `notify` (which calls the mock world).
- Nodes reference earlier data with template expressions such as `{{trigger.body.email}}` and `{{nodes.classify.output.category}}`, resolved by the engine.
- Every node execution persists its resolved input and its output.

### 5. AI Nodes with Enforced Structure

- Implement the `ai` node: a prompt template plus a JSON schema for the output.
- The engine validates the model's output against the schema, retries once with the validation error included, and fails the step cleanly if the output still does not conform.
- Downstream `condition` nodes can branch on AI output fields — this is how AI decides a run's path.
- Record token usage per AI step in the run trace.
- Trigger payloads flow into AI prompts and are untrusted: the injection payloads must not produce unauthorized actions (see the guardrail rule in Must Have 7).

### 6. Durable Execution

- Runs execute asynchronously via a queue and worker; triggers enqueue and return promptly, and execution never happens in the request handler. The worker may live inside the API process or run separately.
- Every step is persisted as it completes. If the worker dies mid-run, a restarted worker resumes the run from the last completed step.
- Side-effect nodes (`notify`, `order_action`, and mutating `http_request` calls) send an idempotency key to the mock world; a resumed run must not duplicate a side effect. The mock world's ledger is the proof.

### 7. Approvals, Failures, and the Step Cap

- Implement the `approval` node: the run pauses in a `waiting_approval` state; an approve API resumes it and a reject API ends it cleanly. The approval decision and decider are recorded.
- Sensitive nodes (marked `requires_approval` in the catalog, such as `order_action`) are unreachable without an approval record — enforced by the engine regardless of AI output.
- Per-node retries with exponential backoff for transient failures; a run that exhausts retries ends in `failed` with the error recorded.
- A step cap, enforced by the engine: a run that executes more than its `limits.max_steps` nodes ends `failed` with a reason naming the cap. `wf_runaway` must be stopped by it. (Wall-clock timeouts and AI token budgets are Good To Have; the seed definitions carry those fields for engines that implement them.)
- Runs use a documented status set: `queued`, `running`, `waiting_approval`, `succeeded`, `failed`, `cancelled`.

### 8. Run Traces, Console, and Verification

- A trace API per run: every step with resolved input, output, attempts, timing, and token usage for AI steps — complete enough to debug a wrong branch decision after the fact.
- A simple console frontend: workflow list, run history, a run-detail view showing the step trace, and a pending-approvals view with approve/reject buttons.
- Your platform must pass `scripts/smoke_test.py`.
- Run `scripts/duplication_check.py` after the kill-and-resume demo and include its output (it inspects the mock world ledger for duplicate side effects).
- Provide automated tests for critical logic: publish validation, template resolution, schema enforcement, and idempotency.

## Good To Have

Attempt these only after every Must Have works. The first item is the highest-value addition by far — if you take on one, take on that.

- **Natural-language workflow compiler:** an endpoint that turns a plain-English description into a valid Draft workflow using only catalog node types — and refuses, with an explanation, when the catalog cannot express the request. Build a one-command eval over `data/nl_eval.jsonl` (12 buildable cases, 3 traps) that scores structural assertions and reports accuracy. Scoring rules are in `docs/EVALUATION_GUIDE.md`.
- **Schedule trigger:** cron-style recurring runs. 1-minute granularity is fine; no catch-up for missed windows.
- **Additional run caps:** a wall-clock timeout and a per-run AI token budget, alongside the max-steps cap.
- **Immutable version history:** publishing creates numbered versions; runs record the version they executed.
- **Replay:** re-run a failed run from its failed step with the original inputs.
- **Live run view:** stream step progress to the console over SSE or WebSocket instead of polling.
- **AI failure triage:** when a node fails repeatedly, an AI pass annotates the run with a probable cause and suggested fix.

## Stretch

- Multiple concurrent workers with leases or locks so two workers never execute the same run, documented and demonstrated.
- An `agent` node: a bounded tool-using loop inside a single step (the model may call catalog-defined tools up to N iterations before returning a structured result).
- Parallel branches: fan out independent branches and join before a later node.
- Sub-workflows: a node that triggers another workflow and waits for its result.
- Cost accounting: price AI-node token usage, per-run and per-workflow spend, and budgets that block new runs when exhausted.
- NL editing (builds on the compiler): "add an approval step before the refund" compiles to a diff against an existing workflow.
- Approval over email: the mock world delivers an approval link that resumes the run.
- Real connector integrations (Slack, Gmail, Sheets) behind the same node interface.
- Organizations and multi-tenancy with isolated data and quotas.

## Technical Requirements

- Expose core functionality as RESTful APIs. The workflow-definition JSON shape and the core routes exercised by the smoke test must match `docs/API_CONTRACT.md`, because the provided scripts depend on them. Other routes may be reshaped if documented.
- Use a reliable persistent store for workflows, runs (with their definition snapshots), steps, and approvals. Run and step state must survive process restarts.
- Execution must be asynchronous: a durable queue (a database-backed queue is fine) and a worker loop off the request path — in-process or separate.
- Side-effect calls to the mock world must carry idempotency keys; recovery must be demonstrably exactly-once at the ledger.
- Every external call — model providers and the mock world — needs a timeout; a hung dependency must fail the step, not the engine.
- Keep the AI provider integration behind an adapter so it can be mocked in engine tests.
- Enforce approval gates and the step cap in the engine, independent of model output.
- Provide automated tests for critical flows and guardrails.
- Do not depend on any specific programming language, framework, or hosted AI provider for the core design.

## Suggested API Surface

You may design your own API shape, but a complete solution should cover flows similar to these (the ones marked ✱ are exercised by the smoke test and defined in `docs/API_CONTRACT.md`):

- `POST /workflows` ✱
- `GET /workflows` ✱ / `GET /workflows/{workflowId}` ✱
- `POST /workflows/{workflowId}/publish` ✱
- `POST /hooks/{workflowId}` (webhook trigger) ✱
- `POST /workflows/{workflowId}/trigger` (manual) ✱
- `GET /runs?workflowId=...`
- `GET /runs/{runId}` (full step trace) ✱
- `POST /runs/{runId}/cancel`
- `GET /approvals?status=pending` ✱
- `POST /approvals/{approvalId}/approve` ✱ / `POST /approvals/{approvalId}/reject`
- `POST /workflows/compile` (Good To Have)

## Suggested Milestones

You may plan your own schedule, but this staging keeps scope manageable:

1. **Core platform:** Data loading, workflow CRUD, publish validation, per-run definition snapshots.
2. **Engine v1:** Queue and worker, manual trigger, deterministic nodes, template resolution, per-step persistence, run traces.
3. **Durability and gates:** Crash resume, idempotency keys against the mock world, retries and backoff, the step cap, approval nodes, the webhook trigger.
4. **AI nodes:** Schema enforcement, injection-safe handling of trigger payloads, token usage in traces.
5. **Console and verification:** Console, smoke test green, duplication check after a live kill-and-resume, documentation, demo video.

The Must Have section above defines the minimum viable submission. If you attempt the Good To Have compiler, treat it as a sixth milestone after all five are green.

For a more detailed build path and FAQ, see `docs/IMPLEMENTATION_GUIDE.md`.

## Provided Starter Dataset

This capstone pack includes:

- A node catalog (`data/node_catalog.json`): every node type with its parameters, output shape, and flags such as `requires_approval`.
- Seed workflow definitions (`data/seed_workflows.json`): `wf_support_triage` (webhook → AI classify → branch → approval → refund → notify), `wf_expense_approval` (a deterministic approval flow the smoke test drives), `wf_slow_fulfillment` (for the kill-and-resume demo), and `wf_runaway` (a poll loop that never completes, for the step-cap demo).
- Sample trigger payloads (`data/sample_payloads.jsonl`), including two prompt-injection payloads aimed at AI nodes.
- A labeled NL eval set (`data/nl_eval.jsonl`) for the Good To Have compiler: plain-English workflow descriptions with structural assertions, including trap cases the node catalog cannot express.
- A zero-dependency mock world server (`scripts/mock_world.py`): endpoints for email, chat messages, orders, refunds, and shipments, with a queryable ledger of every call received (the exactly-once proof), idempotency-key replay detection, and live failure injection for retry testing.
- A zero-dependency mock LLM provider (`scripts/mock_provider.py`, shared with the Prism pack) for deterministic engine tests.
- A smoke test (`scripts/smoke_test.py`) that checks the required API surface and run lifecycle.
- A duplication check (`scripts/duplication_check.py`) that inspects the mock world ledger after crash recovery.

You may extend the dataset, but you must document any added data and how it affects verification.

## Assessment Criteria

- **Must Have workflow - 60%:** Does the platform load the provided data, manage draft/published workflows with per-run snapshots, trigger from webhooks and the manual API, execute deterministic and AI nodes durably with traces, pause and resume on approvals, enforce the step cap, and demo through a simple console?
- **Durability and AI correctness under pressure - 25%:** Does a killed worker resume without duplicate side effects (ledger-verified)? Do injection payloads fail to bypass approval gates? Does schema enforcement catch malformed AI output? Is the runaway workflow stopped by the step cap?
- **Engineering quality, documentation, and demo - 15%:** Is the code maintainable, is setup clear, are design decisions documented, and does the demo clearly show the core workflow?
- Good To Have and Stretch work can strengthen the project — the NL compiler is the highest-value addition — but it should not compensate for missing Must Have functionality.

## Deliverables

1. Final functional product: the orchestration engine and a simple console.
2. README with setup instructions, API documentation, architecture, and design decisions (especially: the run state machine and how exactly-once recovery works).
3. Public GitHub repository link.
4. Seeded demo workflows and instructions to reproduce the demo with the mock world.
5. Verification report: smoke test output and the duplication check result after a live kill-and-resume (plus your NL eval results, if you built the compiler).
6. Explainer video demonstrating the project, including a live worker kill and resume, and an injection payload handled safely.
