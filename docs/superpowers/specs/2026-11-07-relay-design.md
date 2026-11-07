# Relay — Design Spec

**Date:** 2026-11-07
**Source requirements (authoritative, copied into the repo by Task 1):**
`docs/spec/RELAY_PROBLEM_STATEMENT.md`, `docs/spec/API_CONTRACT.md`, `docs/spec/DATA_MODEL.md`,
`docs/spec/IMPLEMENTATION_GUIDE.md`, `docs/spec/EVALUATION_GUIDE.md`, `relay_airtribe.pdf`,
and the pack data in `data/` + verification scripts in `scripts/`
(upstream: https://github.com/airtribe-projects/relay-capstone).

Submission requirements: public GitHub repo, **Dockerized** solution with a detailed README,
demo video < 5 minutes.

---

## 1. Goals and grading map

| Grade bucket | Weight | What wins it | Where in this design |
|---|---|---|---|
| Must Have workflow | 60% | data loaded, draft→publish, snapshots, webhook+manual, deterministic+AI nodes, approvals, step cap, console | §4–§9, §11 |
| Durability & AI correctness | 25% | kill-and-resume with ledger proof, injection can't bypass gate, schema enforcement, runaway stopped | §6, §7, §8 |
| Engineering, docs, demo | 15% | clean code, README (state machine, exactly-once), demo | §12, §13 |
| Good To Have | bonus | NL compiler + eval (highest value), schedules, timeout/token caps, versions, replay, SSE, AI triage | §10 |
| Stretch | bonus | multi-worker leases, cost accounting | §6.4, §8.6 |

Non-goals: drag-and-drop builder, agent node, parallel fan-out/join, sub-workflows,
real Slack/Gmail connectors, multi-tenancy, role separation (single demo token).

## 2. Stack and why

| Concern | Choice | Reason |
|---|---|---|
| Language | Python 3.12 | Pack tooling is Python; first-class `jsonschema`; fastest path to readable engine code |
| API | FastAPI + uvicorn | OpenAPI docs at `/docs` for free (API documentation deliverable), pydantic validation |
| Store + queue | PostgreSQL 16, raw SQL via psycopg 3 + psycopg_pool | One durable store for workflows, versions, runs, steps, approvals **and** the job queue. `FOR UPDATE SKIP LOCKED` + leases. Transactions make "persist, then acknowledge" literal. `LISTEN/NOTIFY` wakes the worker and powers SSE. |
| SQL style | Hand-written SQL in a `store/` layer, plain `.sql` migrations | The durability story must be reviewable line by line; an ORM hides the transactions graders look for |
| HTTP client | httpx (sync) with explicit timeouts | Every external call has a timeout (hard requirement) |
| JSON Schema | `jsonschema` (Draft 2020-12) | AI output enforcement + output_schema meta-validation |
| Cron | `croniter` | Schedule trigger (Good To Have) |
| AI | OpenAI-compatible adapter pointed at **OpenRouter** (`https://openrouter.ai/api/v1`), plus `fake` (scripted, tests) and `heuristic` (offline demo) providers | Provider behind an adapter, mockable in engine tests (hard requirement) |
| Console | Static, no-build SPA (vanilla ES modules + CSS) served by FastAPI at `/console` | No Node toolchain on the machine; zero build step inside Docker |
| Tests | pytest; integration tests against real Postgres; `scripts/mock_world.py` run as a subprocess fixture | Tests prove behaviour against the same mock world the graders use |
| Packaging | Dockerfile (python:3.12-slim) + docker-compose (postgres, mock-world, api, worker) + Makefile | "Dockerize your solution" |

## 3. Repository layout

```
capstone/                              (its own git repo — NOT the parent airtribe repo)
├── README.md
├── Dockerfile  docker-compose.yml  Makefile  pyproject.toml  .env.example  .dockerignore
├── .github/workflows/ci.yml
├── data/                 # copied verbatim from the pack (+ data/README.md documenting additions)
├── scripts/              # pack scripts verbatim + our drill/verify scripts
│   ├── smoke_test.py duplication_check.py mock_world.py mock_provider.py validate_pack.py   (pack)
│   ├── drill_kill_resume.sh  drill_chaos.sh  verify_all.sh  demo_triage.sh                   (ours)
├── docs/
│   ├── spec/             # pack docs verbatim
│   ├── ARCHITECTURE.md  API.md  VERIFICATION.md  DEMO.md
│   ├── verification/     # captured outputs (smoke, drills, nl eval, pytest)
│   └── superpowers/{specs,plans}/
├── src/relay/
│   ├── config.py  db.py  ids.py  clock.py  catalog.py  templates.py  seed.py  events.py  faults.py
│   ├── migrations/ 001_init.sql 002_schedules.sql   migrate.py
│   ├── definition/ validator.py graph.py
│   ├── store/      workflows.py runs.py steps.py approvals.py jobs.py schedules.py
│   ├── engine/     outcomes.py errors.py state.py retry.py world.py guardrails.py runner.py
│   │   └── nodes/  base.py registry.py condition.py delay.py notify.py http_request.py
│   │               order_action.py approval.py ai.py
│   ├── ai/         provider.py openai_compat.py fake.py heuristic.py factory.py schema_enforcer.py
│   ├── services/   triggers.py approvals.py runs.py workflows.py
│   ├── compiler/   prompt.py compiler.py eval.py
│   ├── worker/     main.py scheduler.py heartbeat.py
│   ├── api/        app.py auth.py errors.py serializers.py
│   │   └── routes/ workflows.py hooks.py runs.py approvals.py compile.py meta.py events.py world.py
│   └── console/    index.html styles.css app.js api.js views/*.js
└── tests/  conftest.py  unit/  integration/  e2e/
```

## 4. Data model (PostgreSQL)

```sql
workflows(id text PK, name, description, status text CHECK IN('draft','published'),
          definition jsonb,                -- the working copy (editable)
          current_version int NULL,        -- latest published version; NULL = never published
          has_unpublished_changes bool, created_at, updated_at)
workflow_versions(workflow_id, version, definition jsonb, published_at, PK(workflow_id, version))  -- immutable
runs(id text PK, workflow_id, workflow_version int, definition_snapshot jsonb,
     status text CHECK IN('queued','running','waiting_approval','succeeded','failed','cancelled'),
     trigger_type text, input jsonb, current_node_id text NULL,
     steps_executed int, ai_tokens_used int, ai_cost_usd numeric,
     cancel_requested bool, error jsonb NULL, ended_reason text NULL,
     risk_flags jsonb, waiting_seconds numeric, waiting_since timestamptz NULL,
     replay_of text NULL, diagnosis jsonb NULL,
     created_at, started_at, finished_at, updated_at)
steps(id bigserial PK, run_id, sequence int, node_id, node_type,
      status text CHECK IN('running','retrying','sleeping','waiting','succeeded','failed','cancelled','inherited'),
      attempt int, attempts jsonb,          -- [{n, started_at, finished_at, duration_ms, outcome, error, replayed}]
      resolved_input jsonb, output jsonb, error jsonb,
      tokens_prompt int, tokens_completion int, cost_usd numeric,
      idempotency_key text NULL, approval_id text NULL, gate jsonb NULL,
      wake_at timestamptz NULL, next_attempt_at timestamptz NULL, meta jsonb,
      started_at, finished_at, duration_ms, UNIQUE(run_id, sequence))
approvals(id text PK, run_id, node_id, step_sequence int, message text,
          status text CHECK IN('pending','approved','rejected','closed'),
          decided_by text NULL, decided_at NULL, decision_note text NULL, created_at)
jobs(run_id text PK REFERENCES runs, available_at timestamptz, locked_by text NULL,
     locked_until timestamptz NULL, claims int, created_at)     -- the durable queue
schedules(workflow_id PK, cron, next_fire_at, enabled, last_fired_at, last_run_id)
fault_marks(fault text, run_id text, created_at, PK(fault, run_id))   -- one-shot crash injection
schema_migrations(version text PK, applied_at)
```

Indexes: `steps(run_id, sequence)`, `runs(workflow_id, created_at desc)`, `runs(status)`,
`approvals(status) WHERE status='pending'`, `jobs(available_at) WHERE locked_by IS NULL`.

**Versioning (Good To Have, essentially free):** publish validates `workflows.definition`,
inserts `workflow_versions(version = current_version+1)`, sets `status='published'`.
Editing a published workflow (`PUT`) changes only the working copy and sets
`has_unpublished_changes=true`; triggers always run the latest *published version*.
A run stores `workflow_version` and its own `definition_snapshot` (copy), so neither
editing nor republishing can change an in-flight or historical run.

**Secret hygiene:** webhook secrets live in the DB definition; every API serializer
replaces `trigger.secret` with `"••••••"` and adds `trigger.secret_set: true`.
A `PUT` whose secret equals the mask keeps the stored secret. Snapshots are redacted
in API output too. Model API keys are never logged or echoed in errors.

## 5. Definition validation (publish-time)

`validate_definition(defn, catalog) -> ValidationResult(errors, warnings)`.
Errors (block publish, HTTP 422, distinct `code` per case):

| code | case |
|---|---|
| `invalid_definition` | missing/ill-typed top-level field (`id,name,trigger,entry,nodes,limits`) |
| `unknown_trigger_type` / `missing_webhook_secret` / `invalid_cron` | trigger checks |
| `invalid_limits` | `max_steps` not an int in 1..1000; `timeout_seconds`/`max_ai_tokens` negative |
| `duplicate_node_id` | two nodes share an id |
| `invalid_node_type` | type not in `data/node_catalog.json` — message `Node 'n1' has unknown type 'teleport'` |
| `missing_required_param` | per catalog — `Node 'n1' (notify) is missing required param 'to'` |
| `invalid_param_value` | enum/type violation (e.g. `op: "approx"`, `seconds: "ten"`) |
| `invalid_reference` | `next`/`on_true`/`on_false` → nonexistent node — `Node 'n1' next -> 'ghost' does not exist` |
| `invalid_entry` | `entry` → nonexistent node |
| `invalid_branching` | condition node missing `on_true`/`on_false` keys, or a non-condition node with them |
| `invalid_output_schema` | ai `output_schema` is not a valid JSON Schema object |
| `invalid_template` | template syntax error or root other than `trigger.body` / `nodes.<id>.output` / `run.id` |
| `invalid_template_reference` | `{{nodes.X.output...}}` where X is not a node in this workflow |

Warnings (returned, do not block): `unknown_param`, `unreachable_node`,
`unguarded_sensitive_node` (a `requires_approval` node reachable on some path with no
approval node before it), `ai_controls_side_effect_target` (taint lint: `to`, `url`,
`order_id`, `channel`, `method` templated from an `ai` node's output),
`ai_without_token_budget` (ai nodes but `max_ai_tokens` missing/0 → the engine default budget applies).

Loops/backward jumps are legal. Branch targets may be `null` (path ends).
Create-time (`POST /workflows`) only checks shape (`check_shape`) → 400; drafts may be incomplete.

## 6. Execution engine

### 6.1 Run state machine (documented set only)

```
queued ──claim──▶ running ──┬──▶ succeeded            (reached a node with next = null)
  │                         ├──▶ failed               (step failed permanently / retries exhausted /
  │                         │                          max_steps cap / timeout cap / token budget / approval_required)
  │                         ├──▶ waiting_approval ──approve──▶ running
  │                         │          │ reject ─────────────▶ cancelled (ended_reason=approval_rejected)
  │                         │          │ cancel ─────────────▶ cancelled (approval → closed)
  │                         └──▶ cancelled            (cancel_requested observed between steps)
  └──cancel──▶ cancelled
```
`engine/state.py` holds the allowed-transition table; every status write goes through
`transition(cur, run_id, from_status, to_status)` which raises on an illegal edge.
Terminal: `succeeded`, `failed`, `cancelled`.

### 6.2 Queue and worker

- `jobs` row per run that needs worker attention (PK `run_id` ⇒ never two jobs per run).
- Enqueue = `INSERT … ON CONFLICT(run_id) DO UPDATE SET available_at = LEAST(...)` + `pg_notify('relay_jobs', run_id)`.
- Claim (atomic, multi-worker safe):
  ```sql
  UPDATE jobs SET locked_by=%(w)s, locked_until=now()+make_interval(secs=>%(lease)s), claims=claims+1
  WHERE run_id = (SELECT run_id FROM jobs
                  WHERE available_at <= now() AND (locked_until IS NULL OR locked_until < now())
                  ORDER BY available_at FOR UPDATE SKIP LOCKED LIMIT 1)
  RETURNING run_id, claims;
  ```
- Lease 15 s, heartbeat thread extends every 5 s while a job is held. A `kill -9`ed worker's
  lease expires ≤ 15 s later and any worker reclaims it.
- Releases (delay, backoff) are clamped to the run's wall-clock deadline so `timeout_seconds` fires on time.
- **Fencing:** every engine write transaction first runs
  `SELECT 1 FROM jobs WHERE run_id=%s AND locked_by=%s FOR UPDATE`; if the lease was lost the
  transaction aborts (`LeaseLost`) and the worker drops the run. Combined with idempotency keys,
  a zombie worker can never double-apply a step.
- Worker startup runs `reconcile()`: every non-terminal, non-waiting run without a job gets one
  (defence in depth for "restart with queued work").
- Worker wakes on `LISTEN relay_jobs` or a 0.5 s poll, whichever first.
- Triggers and approve/cancel APIs only write rows + enqueue. **No run executes in a request handler.**

### 6.3 The tick (persist-then-acknowledge)

`Engine.process(run_id, worker_id)` loops `tick()` until it returns `YIELD`/`DONE`
(max 200 ticks per claim, then re-enqueue for fairness). One tick = one node:

1. **Prepare txn** (lock run `FOR UPDATE`, fence lease):
   - terminal → delete job, DONE. `cancel_requested` → close open step as `cancelled`, run → `cancelled`, DONE.
   - `queued` → `running` (+`started_at`).
   - **Caps:** wall-clock (`now - started_at - waiting_seconds > timeout_seconds`) → `failed`
     (`code=timeout_exceeded`).
   - Find the **open step** (`status IN ('running','retrying','sleeping')`, highest sequence).
     - none → `node_id = run.current_node_id`; if `None` → `succeeded`, DONE.
       **Step cap:** if `steps_executed >= limits.max_steps` → `failed`
       `{"code":"max_steps_exceeded","message":"Run stopped by step cap: limits.max_steps=12 reached before executing node 'check_order' (would be step 13)"}`.
       Resolve templates (only `templatable` params). `TemplateError` → insert failed step, run `failed`.
       Insert step (`status='running'`, `sequence = steps_executed+1`, `resolved_input`,
       `idempotency_key` for side-effect nodes) and `steps_executed += 1` **in the same txn**.
     - `sleeping` and `now < wake_at` → reschedule job at `wake_at`, YIELD. `now >= wake_at` → complete delay step (go to 3).
     - `retrying` and `now < next_attempt_at` → reschedule, YIELD.
   - **Approval gate:** if catalog `requires_approval` → require an `approvals` row for this run
     with `status='approved' AND step_sequence < this step's sequence`. Missing → step `failed`
     (`code=approval_required`, not retried), run `failed`, DONE. Record `gate` on the step either way.
   - **Token budget:** ai node and `ai_tokens_used >= budget` → `failed` (`code=token_budget_exceeded`); also checked right after each ai step.
     `budget = limits.max_ai_tokens` when > 0, else `RELAY_DEFAULT_MAX_AI_TOKENS` (20000) — the cap is Good To Have and must never disable AI nodes.
   - Mark any `in_flight` attempt record as `interrupted` (crash evidence), append a new
     attempt record `{n, started_at, outcome:'in_flight'}`, `attempt += 1`. **Commit.**
2. **Execute** the node handler *outside* any transaction (timeouts on all I/O).
   Fault hook `faults.maybe_crash("after_side_effect", node_id, run_id)` fires here (one-shot `os._exit(137)`).
3. **Commit txn** (lock run, fence lease) by outcome:
   - `Completed(output, tokens)` → step `succeeded`, attempt record finalised, run `current_node_id = next`
     (condition: `on_true`/`on_false` by `output.result`), `ai_tokens_used += tokens`; CONTINUE.
   - `Sleep(wake_at)` → step `sleeping`, `wake_at` stored, job `available_at = wake_at`, lease released; YIELD.
   - `AwaitApproval(message)` → insert `approvals(pending)`, step `waiting` + `approval_id`, run
     `waiting_approval`, `waiting_since=now`, delete job; DONE.
   - `TransientError` and `attempt < max_attempts` → step `retrying`, `next_attempt_at = now + backoff(attempt)`,
     job rescheduled, lease released; YIELD. Exhausted → step `failed`, run `failed` (`code=retries_exhausted`).
   - `PermanentError(code)` → step `failed`, run `failed` with that code.
   - Every commit also does `pg_notify('relay_events', {run_id})` (delivered only on commit).

Backoff: `min(base * 2**(attempt-1), cap) * jitter(0.8..1.2)`; defaults base 2 s, cap 60 s, 6 attempts
(≈ 2 minutes total — long enough to restore the world on camera). Tests use tiny values.

### 6.4 Exactly-once side effects

- Key: `{run_id}:{node_id}:{sequence}` — stable across retries (same step row) and resumes
  (the open step row is reused; its `sequence` and key are persisted *before* the call).
  The `:{sequence}` suffix is the FAQ-endorsed extension so loop iterations are distinct effects.
  Never contains the attempt number.
- Crash windows and what happens:

| Crash point | Persisted state | On resume | Ledger |
|---|---|---|---|
| before step row insert | previous step succeeded | re-run prepare → new step | effect once |
| after insert, before call | step `running`, attempt `in_flight` | attempt marked `interrupted`, call sent with same key | executed once |
| **after call, before commit** | step `running` | same key re-sent → world replays stored response | 1 executed + 1 replayed |
| after commit | step `succeeded` | skipped; next node | once |
| during delay | step `sleeping` + `wake_at`, no lease | resumes at `wake_at` | once |

- Replay (`POST /runs/{id}/replay`) re-executes the failed step with the **original** step's
  idempotency key, so even replay is exactly-once at the ledger. A failed run can be replayed only once
  (row lock + unique index on `replay_of`); further replays chain from the newest run.

### 6.5 World client

`WorldClient.call(method, url, json, idempotency_key) -> WorldResponse(status, body, replayed, elapsed_ms)`.
- URL aliasing: any URL starting with an alias in `RELAY_WORLD_URL_ALIASES` (default `http://localhost:9210`)
  is rewritten to `RELAY_MOCK_WORLD_URL` (Docker: `http://mock-world:9210`). Seeds stay verbatim.
- Timeout (`RELAY_HTTP_TIMEOUT_SECONDS`, default 10). Timeout / connection error / 408 / 429 / 5xx → `TransientError`.
- Sets `Idempotency-Key` (engine value wins over any user header).
- `notify`: email → `POST /email/send {to, subject?, message}`; chat → `POST /chat/message {channel: to, message}`.
- `order_action`: `POST /orders/{quote(order_id)}/refund {amount_usd?}` / `/replacement {}`;
  `order_id` must match `^[A-Za-z0-9_-]{1,64}$` (path-injection guard) → else `PermanentError(invalid_order_id)`.
- `http_request`: GET → output `{status, body}` for any status < 500; mutating 4xx → `PermanentError(side_effect_rejected)`.

### 6.6 Templates

`{{ path }}` with roots `trigger.body…`, `nodes.<id>.output…`, `run.id`. Segments are keys or
list indices. A string that is exactly one expression yields the **native** value
(so `{{trigger.body.amount_usd}}` stays `250`); embedded expressions are stringified
(dict/list → compact JSON). Unresolvable path → `TemplateError("Template '{{nodes.issue_refund.output.reference_id}}' could not be resolved: node 'issue_refund' has not produced output in this run")`.
`nodes.X.output` = output of the latest *succeeded/inherited* step for node X in this run.
Only params flagged `templatable` in the catalog are resolved.

### 6.7 Condition semantics

`equals`/`not_equals`: numeric compare if both parse as numbers else string compare.
`greater_than`/`less_than`: both must parse as numbers else `PermanentError(non_numeric_comparison)`.
`contains`: substring for strings, membership for lists. Output `{"result": bool, "left":…, "right":…}`.

### 6.8 Cancel

`POST /runs/{id}/cancel` (single txn, run locked): terminal → 409 `run_not_cancellable`;
`queued` → `cancelled` + delete job; `waiting_approval` → `cancelled`, pending approval → `closed`,
waiting step → `cancelled`; `running` → `cancel_requested=true` and job `available_at=now`
(so a sleeping/retrying run is picked up immediately and cancelled; an in-flight step finishes first).
`ended_reason` ∈ `completed | failed | cancelled_by_user | approval_rejected`.

## 7. Approvals

- `approval` node → `AwaitApproval(resolved message)`; trace step status `waiting`.
- `POST /approvals/{id}/approve|reject` body `{"decided_by"?: str, "note"?: str}`
  (default decider `demo-operator`). One txn: lock approval (`pending` else 409 `approval_already_decided`),
  lock run (`waiting_approval` else 409), set decision, complete the step with output
  `{"decision","decided_by","decided_at"}`, accumulate `waiting_seconds`, then approve → run `running`,
  `current_node_id = node.next`, enqueue; reject → run `cancelled` (`ended_reason=approval_rejected`).
- Gate = data, not prompt (§6.3). Nothing an AI node outputs can write an approval row; the only
  writer is the approvals service invoked by an authenticated API call.

## 8. AI nodes and guardrails

### 8.1 Provider adapter
```python
class ModelProvider(Protocol):
    name: str
    def complete(self, messages: list[dict], *, json_schema: dict | None, timeout_s: float) -> Completion: ...
@dataclass Completion(text, prompt_tokens, completion_tokens, model, cost_usd=None, estimated_tokens=False)
```
- `OpenAICompatibleProvider` (OpenRouter default): `POST {base}/chat/completions`, `temperature 0`,
  `response_format` = `{"type":"json_object"}` by default (configurable `json_schema` | `none`),
  `usage: {"include": true}` to get OpenRouter cost, headers `HTTP-Referer`/`X-Title`.
  Errors: timeout/connection/429/5xx → `ProviderTransientError`; 4xx → `ProviderPermanentError`.
  The API key is never included in any error text.
- `FakeProvider(script)` — deterministic queue of responses for tests.
- If `openrouter`/`openai_compatible` is selected without a key, the factory logs a warning and falls back to `heuristic`; `/health` reports the effective provider. The NL compiler refuses to run on `heuristic` (503 `compiler_requires_model`).
- `HeuristicProvider` — offline demo provider: builds a schema-valid instance, choosing enum values by
  keyword match against the prompt. Clearly labelled in the trace (`model: "heuristic-offline"`).
- `RELAY_AI_PROVIDER = openrouter | openai_compatible | heuristic | fake`.

### 8.2 Schema enforcement
`enforce(provider, messages, schema, timeout) -> EnforcedResult(output|None, calls[], errors[])`:
call → strip code fences → `json.loads` → `Draft202012Validator(schema)` → if invalid, **one** repair
call (original messages + assistant raw text + "Your previous response did not conform: <errors>. Return
only the corrected JSON object.") → if still invalid, `PermanentError(ai_output_invalid)` carrying both
calls and their tokens. Transient provider errors propagate as `TransientError` (normal backoff),
not as repair attempts. Tokens of every call are summed onto the step.

### 8.3 Injection defence in depth
1. **Engine gate** (§6.3) — the actual guarantee: sensitive nodes need an approval row.
2. **Routing only through the schema:** AI influences flow only via schema-validated, enum-constrained fields;
   side-effect targets are fixed in the definition (taint lint warns otherwise).
3. **Prompt framing:** system message declares the user content untrusted data, states the model cannot
   grant approvals or trigger actions, and embeds the output schema.
4. **Input risk scan:** `scan_untrusted(trigger_body)` flags known injection patterns (override/admin mode,
   "approval already granted", "ignore previous instructions", exfiltration asks). Flags are stored on the
   run (`risk_flags`) and shown in the console. Observational — never the control.
5. **Output screening:** `screen_output(output, schema)` redacts email addresses and URLs inside AI string
   fields (`[redacted]`) and flags injection parroting, so `summary` cannot carry
   `audit@evil-example.com` into `#support`. Re-validates against the schema after redaction.
   Toggle: `RELAY_AI_OUTPUT_REDACTION` (default on).

### 8.4 Token accounting and budget
Per step `tokens_prompt`, `tokens_completion`, `cost_usd` (accumulated across engine retries), plus every model call in `meta.ai_calls`.
Run accumulates `ai_tokens_used`, `ai_cost_usd`. Budget `limits.max_ai_tokens` checked before every ai node.

## 9. API (fixed contract first)

All platform routes require `Authorization: Bearer $RELAY_API_TOKEN` (default `relay-demo-token`),
except `GET /health`, `POST /hooks/{id}` (secret only), and static console assets.
SSE endpoints also accept `?token=` (EventSource cannot set headers).
Error envelope: `{"error": {"code", "message", "details"?}}`.

| Route | Notes |
|---|---|
| `GET /workflows` ✱ | `[{id, name, status, current_version, has_unpublished_changes, trigger_type, node_count, updated_at}]` |
| `POST /workflows` ✱ | seed shape; 201 draft; 400 shape error; 409 duplicate id |
| `GET /workflows/{id}` ✱ | definition (secret masked) + versions summary + last validation |
| `PUT /workflows/{id}` | edit working copy |
| `POST /workflows/{id}/validate` | dry-run validation → `{ok, errors, warnings}` |
| `POST /workflows/{id}/publish` ✱ | 200 `{id, status, version, warnings}`; 422 with first issue's `code`/`message` + `details` |
| `GET /workflows/{id}/versions[/{v}]` | immutable history |
| `POST /workflows/{id}/trigger` ✱ | `{"input": {...}}` → 202 `{run_id, id, status}`; 409 `workflow_not_published` |
| `POST /hooks/{id}` ✱ | `X-Relay-Secret` (constant-time compare) → 202; 401 `invalid_webhook_secret`; 409 not webhook/not published; 400 non-object body; 413 > 1 MB |
| `GET /runs?workflow_id=&status=&limit=&cursor=` | history, newest first |
| `GET /runs/{id}` ✱ | `run_id`,`id`,`status`,`steps[]` (`node_id`,`type`,`status`,`attempt`,`attempts`,`input`,`output`,`error`,`tokens`,`idempotency_key`,`approval_id`,`gate`,`started_at`,`duration_ms`), `approvals[]`, `error`, `risk_flags`, limits/usage |
| `POST /runs/{id}/cancel` | §6.8 |
| `POST /runs/{id}/replay` | failed runs only → new run |
| `POST /runs/{id}/diagnose` | AI failure triage |
| `GET /runs/{id}/stream`, `GET /events/stream` | SSE |
| `GET /approvals?status=pending` ✱ | `[{id, run_id, workflow_id, node_id, message, status, created_at}]` |
| `POST /approvals/{id}/approve` ✱ / `/reject` | §7 |
| `POST /workflows/compile` | NL compiler |
| `GET /catalog`, `GET /samples`, `GET /stats`, `GET /schedules`, `GET /health` | meta |
| `GET /world/ledger`, `POST /world/config`, `POST /world/reset` | console proxies to the mock world (demo convenience) |

Status codes: 401 token, 401 webhook secret, 404 unknown id, 400 shape / 422 validation,
409 unpublished trigger / decided approval / terminal cancel / duplicate id, 413 payload too large.

## 10. Good To Have / Stretch

1. **NL compiler** `POST /workflows/compile {description, save?: bool=true}` → prompt with compact catalog
   + rules + one seed example → `enforce()` against a compiler output schema
   (`kind: workflow|refusal`) → post-process (generated id `wf_nl_<slug>_<hex>`, generated webhook secret,
   default limits, **auto-insert approval before any unguarded order_action** and say so in `explanation`)
   → `validate_definition` → up to 2 repair rounds with the validator's issues → 201 draft (webhook secret returned once, masked everywhere else) or 200 refusal
   `{refusal:{reason, missing_capabilities}}` or 422 `compile_failed`. Never emits a non-catalog type
   (validator is the referee).
2. **NL eval** `python -m relay.compiler.eval` (or `make nl-eval`) over `data/nl_eval.jsonl` (description only
   is sent) → per-case checks per EVALUATION_GUIDE §4 → `docs/verification/nl_eval.json` + `.md`
   with guide accuracy plus a stricter "trigger also matches" accuracy.
3. **Schedule trigger:** publishing a `schedule` workflow upserts `schedules`; scheduler loop in the worker
   (singleton via `pg_try_advisory_lock`) fires due schedules every 5 s, 1-minute granularity, no catch-up.
4. **Caps:** wall-clock timeout (excludes time waiting for approval) and token budget (§6.3).
5. **Immutable versions** (§4). 6. **Replay** (§6.4). 7. **SSE live view.** 8. **AI failure triage**
   (`/runs/{id}/diagnose`, stored in `runs.diagnosis`).
9. Stretch: **multi-worker** (`docker compose up --scale worker=3`, leases + fencing, tested),
   **cost accounting** (OpenRouter `usage.cost` per step/run/workflow in `/stats`).

## 11. Console (`/console`)

Hash-routed vanilla SPA: Dashboard (counts by status, pending approvals, spend), Workflows (list +
detail: node flow, JSON, versions, warnings, trigger panel with sample-payload picker), Runs (filterable
list), Run detail (live via SSE: step timeline with expandable resolved input / output / attempts / tokens /
idempotency key / gate; risk flags banner; cancel / replay / diagnose), Approvals (approve/reject with
decider name), Compiler (English → draft → publish), World (ledger table with replayed badges,
chaos toggles, reset). Token stored in `localStorage`.

## 12. Verification artifacts

- `pytest` suites (unit + integration against Postgres + mock world subprocess).
- `scripts/smoke_test.py` — must be all PASS.
- `scripts/drill_kill_resume.sh` — mode A: `kill -9` the worker during `pack_delay`; mode B: one-shot
  fault `crash_after_side_effect:create_shipment` (the real crash window) → restart → `duplication_check.py`.
- `scripts/drill_chaos.sh` — world `down` mid-run then restore; `fail_rate 0.3`; runaway cap.
- `scripts/verify_all.sh` — runs everything and writes `docs/verification/*.txt`, assembled into
  `docs/VERIFICATION.md`.

## 13. Configuration (env)

`RELAY_DATABASE_URL`, `RELAY_API_TOKEN`, `RELAY_MOCK_WORLD_URL`, `RELAY_WORLD_URL_ALIASES`,
`RELAY_HTTP_TIMEOUT_SECONDS`, `RELAY_RETRY_MAX_ATTEMPTS`, `RELAY_RETRY_BASE_SECONDS`,
`RELAY_RETRY_MAX_BACKOFF_SECONDS`, `RELAY_LEASE_SECONDS`, `RELAY_POLL_INTERVAL_SECONDS`,
`RELAY_AI_PROVIDER`, `RELAY_AI_BASE_URL`, `RELAY_AI_API_KEY` (alias `OPENROUTER_API_KEY`),
`RELAY_AI_MODEL`, `RELAY_AI_TIMEOUT_SECONDS`, `RELAY_AI_RESPONSE_FORMAT`, `RELAY_AI_OUTPUT_REDACTION`,
`RELAY_FAULTS`, `RELAY_DATA_DIR`, `RELAY_WORKER_ID`, `RELAY_SEED_ON_START`.

## 14. Known limitations (to document honestly)

Mid-step abort is not supported (cancel is cooperative between steps); `nodes.X.output` resolves to the
latest iteration in loops; heuristic offline provider is a demo stand-in, not a classifier; redaction is
pattern-based; scheduler has 1-minute granularity and no catch-up; single demo token, no RBAC; the console
proxies world admin routes for demo convenience only.
