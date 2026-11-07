# Relay API Contract

This document describes the expected product and API behavior without requiring any specific programming language or framework.

You may reshape routes and response bodies if you document the changes clearly — with one exception: the **fixed contract** below is exercised by `scripts/smoke_test.py` and must match.

## Fixed Contract (the smoke test depends on this)

1. The workflow-definition JSON shape from `data/seed_workflows.json`: `id`, `name`, `trigger`, `entry`, `limits`, and `nodes[]` with `next` (or `on_true`/`on_false` on condition nodes).
2. These routes and behaviors:
   - `GET /workflows` — lists workflows with at least `id` and `status`.
   - `POST /workflows` — accepts a definition in the seed shape, creates a Draft.
   - `POST /workflows/{workflowId}/publish` — publishes, or rejects invalid definitions with 4xx.
   - `POST /workflows/{workflowId}/trigger` — body `{"input": {...}}`, returns a run id as `run_id` or `id`.
   - `POST /hooks/{workflowId}` — webhook trigger; secret checked from the `X-Relay-Secret` header; wrong/missing secret rejected 401/403; returns a run id.
   - `GET /runs/{runId}` — returns at least `status` and `steps[]`, each step with a `node_id` and `status`.
   - `GET /approvals?status=pending` — lists pending approvals with at least `id` and `run_id`.
   - `POST /approvals/{approvalId}/approve` — resumes the paused run.
3. The run status set: `queued`, `running`, `waiting_approval`, `succeeded`, `failed`, `cancelled`.
4. Invalid publishes are rejected at create or publish time with a 4xx and a clear error body.

Everything else — route names, pagination, extra fields, the compile endpoint's shape — is yours to design and document.

## Personas

- `builder`: Creates and publishes workflows via API (or the Good To Have NL compiler).
- `operator`: Watches runs, reads traces, cancels stuck runs.
- `approver`: Reviews and decides pending approvals.
- `external caller`: A system POSTing to a webhook URL with the workflow's secret. Unauthenticated beyond the secret.

For Must Have, a single demo token covering builder/operator/approver is fine. Role separation is Good To Have.

## Authentication

- Platform APIs: a simple documented mechanism (`Authorization: Bearer <demo-token>` is enough). The smoke test sends this header when given `--token`.
- Webhook endpoint: the per-workflow secret in `X-Relay-Secret`. No other auth required on this route.

## Core Resources

- Workflow (draft or published)
- Run (with its definition snapshot) and its steps (the trace)
- Approval
- Schedule (for cron triggers — Good To Have)

## Core API Flows

### 1. Create and Publish a Workflow

`POST /workflows` with a definition in the seed shape, then `POST /workflows/{id}/publish`.

Publish-time validation must reject, with a distinct message per case:

- a node `type` not present in `data/node_catalog.json`,
- a missing required param (per the catalog),
- a `next`/`on_true`/`on_false` pointing to a nonexistent node,
- an `entry` pointing to a nonexistent node.

Backward jumps (loops) are legal and must not be rejected — run caps bound them at execution time.

### 2. Trigger a Run

Webhook:

```
POST /hooks/wf_expense_approval
X-Relay-Secret: whsec_expense_774
Content-Type: application/json

{"employee_email": "dev1@example.com", "amount_usd": 250, "description": "Conference ticket"}
```

Response: `202` (or `200`/`201`) with `{"run_id": "run_..."}`. The body becomes `{{trigger.body}}`.

Manual: `POST /workflows/{id}/trigger` with `{"input": {...}}` — same response shape.

Both routes must enqueue and return promptly; execution happens on the worker.

### 3. Read a Run Trace

`GET /runs/{runId}`

```json
{
  "run_id": "run_01h9...",
  "workflow_id": "wf_expense_approval",
  "status": "waiting_approval",
  "started_at": "2026-07-04T10:02:11Z",
  "steps": [
    {
      "node_id": "is_large",
      "type": "condition",
      "status": "succeeded",
      "attempt": 1,
      "input": {"left": "250", "op": "greater_than", "right": "100"},
      "output": {"result": true},
      "started_at": "2026-07-04T10:02:12Z",
      "duration_ms": 3
    },
    {
      "node_id": "finance_gate",
      "type": "approval",
      "status": "waiting",
      "approval_id": "apr_7f2c"
    }
  ]
}
```

The exact field names beyond `status`, `steps[].node_id`, and `steps[].status` are yours; the trace must show resolved input, output, attempts, timing, and (for `ai` steps) token usage.

### 4. Approvals

- `GET /approvals?status=pending` → `[{"id": "apr_7f2c", "run_id": "run_01h9...", "message": "Expense of $250 from dev1@example.com: Conference ticket"}]`
- `POST /approvals/{id}/approve` → run leaves `waiting_approval` and continues.
- `POST /approvals/{id}/reject` → run ends `cancelled` (or a documented equivalent), with the decision recorded.

Record who decided and when. With a single demo token, the decider can be a constant — the field must still exist.

### 5. Cancel a Run

`POST /runs/{runId}/cancel`

Expected behavior:

- A `queued` run ends `cancelled` without executing anything.
- A `running` run stops cooperatively: the engine checks for cancellation between steps. The step currently in flight is allowed to finish (or fail) — mid-step abort is not required — and no further nodes execute.
- A `waiting_approval` run ends `cancelled` and its pending approval is closed (no longer actionable, not listed as pending).
- Cancelling a run that is already terminal (`succeeded`, `failed`, `cancelled`) returns 409.
- The run records that it ended by cancellation.

Note that rejecting an approval also ends the run `cancelled` — same terminal state, two doors.

### 6. Compile from Natural Language (Good To Have)

Applies only if you attempt the Good To Have compiler.

`POST /workflows/compile` (shape suggested, not fixed):

```json
{"description": "When a webhook receives a new order, check if the total amount is over 100 dollars. ..."}
```

Success → a Draft workflow in the standard definition shape (plus any explanation you want to attach). The draft must pass your own publish validation.

Refusal (for trap cases) →

```json
{
  "refusal": {
    "reason": "This needs a database scan and a Salesforce connector; the catalog has neither.",
    "missing_capabilities": ["postgres query", "salesforce update"]
  }
}
```

The compiler must never emit node types outside the catalog. If the description implies a sensitive action (`order_action`), the generated workflow must place an `approval` node before it.

### 7. Error Responses

Every rejection returns a clear error body:

```json
{"error": {"message": "Node 'n1' has unknown type 'teleport'", "code": "invalid_node_type"}}
```

Suggested mapping:

| Case | Status |
|---|---|
| Missing/invalid platform token | 401 |
| Wrong or missing webhook secret | 401 or 403 |
| Unknown workflow/run/approval id | 404 |
| Invalid definition (create or publish) | 400 or 422 |
| Triggering an unpublished workflow | 400 or 409 |
| Approving an already-decided approval | 409 |
| Cancelling an already-terminal run | 409 |

You may choose different codes if documented, but each case must be distinguishable from the body.

## Engine Behavior Contract

Not routes, but behavior the reviewer will check:

- **Idempotency keys:** every side-effect call to the mock world (`notify`, `order_action`, mutating `http_request`) carries an `Idempotency-Key` header that is stable across retries and resumes of the same step — `{run_id}:{node_id}` is the canonical choice. Do not include the attempt number: a retry must reuse the key, or replay detection cannot work.
- **Approval gating:** a node whose catalog entry has `requires_approval: true` must not execute unless an `approval` node earlier in the same run was approved. This is engine logic; no prompt text can override it.
- **Step cap:** when `limits.max_steps` is exceeded, the run ends `failed` with a reason that names the cap. (`limits.timeout_seconds` and `limits.max_ai_tokens` are Good To Have; the seed definitions carry the fields for engines that implement them.)
- **AI output:** validated against the node's `output_schema`; one retry with the validation error appended; then the step fails.
- **Timeouts:** every call to the mock world or a model provider has a timeout; a hung dependency fails the step (and triggers retry/backoff), never the engine.
