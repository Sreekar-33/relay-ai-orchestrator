CREATE TABLE IF NOT EXISTS workflows (
  id text PRIMARY KEY,
  name text NOT NULL,
  description text NOT NULL DEFAULT '',
  status text NOT NULL CHECK (status IN ('draft','published')),
  definition jsonb NOT NULL,
  current_version int,
  has_unpublished_changes boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS workflow_versions (
  workflow_id text NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  version int NOT NULL,
  definition jsonb NOT NULL,
  published_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_id, version)
);
CREATE TABLE IF NOT EXISTS runs (
  id text PRIMARY KEY,
  workflow_id text NOT NULL REFERENCES workflows(id),
  workflow_version int NOT NULL,
  definition_snapshot jsonb NOT NULL,
  status text NOT NULL CHECK (status IN ('queued','running','waiting_approval','succeeded','failed','cancelled')),
  trigger_type text NOT NULL,
  input jsonb NOT NULL DEFAULT '{}'::jsonb,
  current_node_id text,
  steps_executed int NOT NULL DEFAULT 0,
  ai_tokens_used int NOT NULL DEFAULT 0,
  cancel_requested boolean NOT NULL DEFAULT false,
  error jsonb,
  ended_reason text,
  risk_flags jsonb NOT NULL DEFAULT '[]'::jsonb,
  waiting_seconds double precision NOT NULL DEFAULT 0,
  waiting_since timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  finished_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS runs_wf_idx ON runs (workflow_id, created_at DESC);
CREATE INDEX IF NOT EXISTS runs_created_idx ON runs (created_at DESC);
CREATE TABLE IF NOT EXISTS steps (
  id bigserial PRIMARY KEY,
  run_id text NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  sequence int NOT NULL,
  node_id text NOT NULL,
  node_type text NOT NULL,
  status text NOT NULL CHECK (status IN ('running','retrying','sleeping','waiting','succeeded','failed','cancelled')),
  attempt int NOT NULL DEFAULT 0,
  attempts jsonb NOT NULL DEFAULT '[]'::jsonb,
  resolved_input jsonb,
  output jsonb,
  error jsonb,
  tokens_prompt int,
  tokens_completion int,
  idempotency_key text,
  approval_id text,
  gate jsonb,
  wake_at timestamptz,
  next_attempt_at timestamptz,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  duration_ms int,
  UNIQUE (run_id, sequence)
);
CREATE TABLE IF NOT EXISTS approvals (
  id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  node_id text NOT NULL,
  step_sequence int NOT NULL,
  message text NOT NULL,
  status text NOT NULL CHECK (status IN ('pending','approved','rejected','closed')),
  decided_by text,
  decided_at timestamptz,
  decision_note text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS approvals_pending_idx ON approvals (created_at) WHERE status = 'pending';
CREATE TABLE IF NOT EXISTS jobs (
  run_id text PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_until timestamptz,
  claims int NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS jobs_available_idx ON jobs (available_at);
CREATE TABLE IF NOT EXISTS fault_marks (
  fault text NOT NULL,
  run_id text NOT NULL,
  PRIMARY KEY (fault, run_id)
);
