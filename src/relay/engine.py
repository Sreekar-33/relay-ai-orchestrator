"""The orchestration engine: mock-world client, node handlers, and the durable persist-then-acknowledge runner.

One tick executes one node in three phases:
  1. PREPARE (txn, run row locked, lease fenced): check cancel/caps, persist the step row + idempotency key BEFORE any effect.
  2. EXECUTE (no txn): call the node handler; every external call has a timeout and carries the stable key.
  3. COMMIT  (txn, run row locked, lease fenced): persist output/next pointer, or retry/backoff, or pause, or fail.
A crash anywhere resumes from persisted state; a crash between 2 and 3 re-sends the same key and the world replays it.
"""
from __future__ import annotations

import enum
import ipaddress
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlsplit

import httpx

from relay import ai
from relay.errors import PermanentError, StepError, TransientError
from relay.ids import new_id
from relay.templates import TemplateContext, TemplateError, resolve_params, stringify
from relay.validation import is_side_effect, node_index

log = logging.getLogger("relay.engine")
TERMINAL = frozenset({"succeeded", "failed", "cancelled"})
ALLOWED = {
    "queued": {"running", "cancelled", "failed"},
    "running": {"waiting_approval", "succeeded", "failed", "cancelled"},
    "waiting_approval": {"running", "cancelled", "failed"},
}
ORDER_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def publish_event(cur, run_id: str, kind: str) -> None:
    cur.execute("SELECT pg_notify('relay_events', %s)", (json.dumps({"run_id": run_id, "kind": kind}),))


# ------------------------------------------------------------------ outcomes
@dataclass(frozen=True)
class Completed:
    output: dict
    tokens: ai.TokenUsage | None = None
    meta: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Sleep:
    wake_at: datetime


@dataclass(frozen=True)
class AwaitApproval:
    message: str


# ------------------------------------------------------------------ mock world client
@dataclass(frozen=True)
class WorldResponse:
    status: int
    body: object
    replayed: bool


class WorldClient:
    def __init__(self, settings, client: httpx.Client | None = None):
        self.base = settings.mock_world_url.rstrip("/")
        self.aliases = settings.world_url_aliases
        self.timeout = settings.http_timeout_s
        self.client = client or httpx.Client()
        # SSRF guard: workflow authors (and the NL compiler) choose http_request URLs, so only allow-listed hosts are
        # reachable. The mock world (+ its aliases) is always allowed; RELAY_HTTP_ALLOWED_HOSTS adds more ("*" = any).
        own = {urlsplit(u).hostname for u in (self.base, *self.aliases)}
        self.allowed = {h for h in own if h} | set(getattr(settings, "http_allowed_hosts", ()) or ())

    def check_url(self, url: str) -> None:
        host = (urlsplit(url).hostname or "").lower()
        try:
            ip = ipaddress.ip_address(host)
            if ip.is_link_local or ip.is_multicast or ip.is_unspecified:   # e.g. 169.254.169.254 cloud metadata
                raise PermanentError("url_not_allowed", f"Requests to {host} are blocked (link-local/metadata address)")
        except ValueError:
            pass
        if "*" not in self.allowed and host not in self.allowed:
            raise PermanentError("url_not_allowed", f"Host '{host}' is not in the http_request allowlist (set RELAY_HTTP_ALLOWED_HOSTS)")

    def url(self, path: str) -> str:
        return self.base + path

    def rewrite(self, url: str) -> str:
        """Seeds hardcode http://localhost:9210; route it to the configured world (e.g. http://mock-world:9210 in Docker)."""
        for a in self.aliases:
            if url == a or url.startswith(a + "/"):
                return self.base + url[len(a):]
        return url

    def call(self, method, url, *, json_body=None, headers=None, idempotency_key=None) -> WorldResponse:
        method = method.upper()
        hdrs = {str(k): str(v) for k, v in (headers or {}).items() if str(k).lower() not in {"idempotency-key", "host", "content-length"}}
        if idempotency_key:
            hdrs["Idempotency-Key"] = idempotency_key
        target = self.rewrite(url)
        self.check_url(target)
        try:
            r = self.client.request(method, target, json=json_body if method != "GET" else None, headers=hdrs, timeout=self.timeout)
        except httpx.TimeoutException:
            raise TransientError("timeout", f"{method} {url} timed out after {self.timeout}s")
        except httpx.TransportError as exc:
            raise TransientError("connection_error", f"{method} {url} failed: {type(exc).__name__}")
        try:
            body = r.json()
        except ValueError:
            body = r.text[:2000]
        if r.status_code in (408, 429) or r.status_code >= 500:
            raise TransientError("upstream_unavailable", f"{method} {url} returned HTTP {r.status_code}", detail={"status": r.status_code})
        return WorldResponse(r.status_code, body, r.headers.get("x-mockworld-replayed", "").lower() == "true")


def _errmsg(body) -> str:
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        return str(body["error"].get("message"))
    return str(body)[:300]


# ------------------------------------------------------------------ node handlers
@dataclass
class NodeContext:
    run_id: str
    node_id: str
    params: dict
    idempotency_key: str | None
    trigger_body: object
    now: datetime
    world: WorldClient
    provider: object
    settings: object


def _num(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def h_condition(ctx):
    left, op, right = ctx.params.get("left"), ctx.params.get("op"), ctx.params.get("right")
    ln, rn = _num(left), _num(right)
    if op in ("equals", "not_equals"):
        same = (ln == rn) if ln is not None and rn is not None else stringify(left) == stringify(right)
        result = same if op == "equals" else not same
    elif op in ("greater_than", "less_than"):
        if ln is None or rn is None:
            raise PermanentError("non_numeric_comparison", f"Condition '{ctx.node_id}': {op} needs two numbers, got {left!r} and {right!r}")
        result = ln > rn if op == "greater_than" else ln < rn
    elif op == "contains":
        result = (right in left) if isinstance(left, list) else stringify(right) in stringify(left)
    else:
        raise PermanentError("invalid_param_value", f"Condition '{ctx.node_id}': unknown op {op!r}")
    return Completed({"result": bool(result), "left": left, "op": op, "right": right})


def h_delay(ctx):
    try:
        seconds = float(ctx.params["seconds"])
    except (TypeError, ValueError, KeyError):
        raise PermanentError("invalid_param_value", f"Delay '{ctx.node_id}': seconds must be a number")
    return Completed({"slept_seconds": 0}) if seconds <= 0 else Sleep(ctx.now + timedelta(seconds=seconds))


def h_notify(ctx):
    p = ctx.params
    if p.get("channel") == "email":
        path, body = "/email/send", {"to": stringify(p.get("to")), "message": stringify(p.get("message"))}
        if p.get("subject") is not None:
            body["subject"] = stringify(p["subject"])
    elif p.get("channel") == "chat":
        path, body = "/chat/message", {"channel": stringify(p.get("to")), "message": stringify(p.get("message"))}
    else:
        raise PermanentError("invalid_param_value", f"Notify '{ctx.node_id}': channel must be email or chat")
    r = ctx.world.call("POST", ctx.world.url(path), json_body=body, idempotency_key=ctx.idempotency_key)
    if r.status >= 400:
        raise PermanentError("side_effect_rejected", f"notify rejected: HTTP {r.status}: {_errmsg(r.body)}")
    b = r.body if isinstance(r.body, dict) else {}
    return Completed({"delivered": bool(b.get("delivered")), "notification_id": b.get("notification_id")},
                     meta={"replayed": r.replayed, "http_status": r.status})


def h_http_request(ctx):
    p = ctx.params
    method, url = str(p.get("method", "GET")).upper(), str(p.get("url", ""))
    if not url.startswith(("http://", "https://")):
        raise PermanentError("invalid_url", f"http_request '{ctx.node_id}': url must be http(s)")
    r = ctx.world.call(method, url, json_body=p.get("body"), headers=p.get("headers") if isinstance(p.get("headers"), dict) else None,
                       idempotency_key=ctx.idempotency_key if method != "GET" else None)
    if method != "GET" and 400 <= r.status < 500:
        raise PermanentError("side_effect_rejected", f"{method} {url} rejected: HTTP {r.status}: {_errmsg(r.body)}")
    return Completed({"status": r.status, "body": r.body}, meta={"replayed": r.replayed, "http_status": r.status})


def h_order_action(ctx):
    p = ctx.params
    order_id = str(p.get("order_id", ""))
    if not ORDER_ID_RE.match(order_id):
        raise PermanentError("invalid_order_id", f"order_action '{ctx.node_id}': {order_id!r} is not a valid order id")
    action = p.get("action")
    body = {"amount_usd": p["amount_usd"]} if action == "refund" and p.get("amount_usd") is not None else {}
    r = ctx.world.call("POST", ctx.world.url(f"/orders/{quote(order_id)}/{action}"), json_body=body, idempotency_key=ctx.idempotency_key)
    if r.status >= 400:
        raise PermanentError("side_effect_rejected", f"order {action} on {order_id} rejected: HTTP {r.status}: {_errmsg(r.body)}")
    b = r.body if isinstance(r.body, dict) else {}
    return Completed({k: b.get(k) for k in ("status", "reference_id", "amount_usd") if k in b} | {"reference_id": b.get("reference_id")},
                     meta={"replayed": r.replayed, "http_status": r.status})


def h_approval(ctx):
    return AwaitApproval(stringify(ctx.params.get("message", "")))


def h_ai(ctx):
    schema = ctx.params.get("output_schema") or {"type": "object"}
    res = ai.enforce(ctx.provider, stringify(ctx.params.get("prompt", "")), schema, timeout_s=ctx.settings.ai_timeout_s)
    meta = {"provider": ctx.provider.name, "model": res.calls[-1].model if res.calls else ctx.provider.model,
            "ai_calls": [asdict(c) for c in res.calls], "repaired": res.output is not None and len(res.calls) > 1}
    if res.output is None:
        raise PermanentError("ai_output_invalid", f"AI output failed schema validation after {len(res.calls)} attempts: {res.errors[0]}",
                             detail={"errors": res.errors}, tokens=res.tokens, meta=meta)
    output, report = ai.screen_output(res.output, schema)
    meta["guardrails"] = report
    return Completed(output, tokens=res.tokens, meta=meta)


HANDLERS = {"condition": h_condition, "delay": h_delay, "notify": h_notify, "http_request": h_http_request,
            "order_action": h_order_action, "approval": h_approval, "ai": h_ai}


# ------------------------------------------------------------------ fault injection (durability drills)
class Faults:
    """RELAY_FAULTS=crash_after_side_effect:<node_id> kills the worker (exit 137) right after the side effect returns and
    BEFORE the step is persisted — the exact crash window. One-shot per run (mark committed before dying)."""

    def __init__(self, db, specs, crash=os._exit):
        self.db, self.crash = db, crash
        self.specs = {tuple(s.split(":", 1)) for s in specs if ":" in s}

    def maybe_crash(self, point, node_id, run_id):
        if (point, node_id) not in self.specs:
            return
        with self.db.tx() as cur:
            cur.execute("INSERT INTO fault_marks(fault, run_id) VALUES (%s,%s) ON CONFLICT DO NOTHING RETURNING 1", (f"{point}:{node_id}", run_id))
            fired = cur.fetchone() is not None
        if fired:
            log.critical("FAULT INJECTION %s at %s (%s): killing worker now", point, node_id, run_id)
            for h in logging.getLogger().handlers:
                h.flush()
            self.crash(137)


# ------------------------------------------------------------------ queue (jobs table)
class LeaseLost(Exception):
    pass


def jobs_enqueue(cur, run_id, available_at=None):
    cur.execute("""INSERT INTO jobs(run_id, available_at) VALUES (%s, COALESCE(%s, now()))
                   ON CONFLICT (run_id) DO UPDATE SET available_at = LEAST(jobs.available_at, EXCLUDED.available_at)""", (run_id, available_at))
    cur.execute("SELECT pg_notify('relay_jobs', %s)", (run_id,))


def jobs_claim(cur, worker_id, lease_s):
    cur.execute("""UPDATE jobs SET locked_by=%s, locked_until=now() + make_interval(secs => %s), claims=claims+1
                   WHERE run_id = (SELECT run_id FROM jobs WHERE available_at <= now()
                                   AND (locked_until IS NULL OR locked_until < now())
                                   ORDER BY available_at FOR UPDATE SKIP LOCKED LIMIT 1)
                   RETURNING run_id""", (worker_id, lease_s))
    row = cur.fetchone()
    return row["run_id"] if row else None


def jobs_release(cur, run_id, worker_id, available_at):
    cur.execute("UPDATE jobs SET locked_by=NULL, locked_until=NULL, available_at=%s WHERE run_id=%s AND locked_by=%s",
                (available_at, run_id, worker_id))
    cur.execute("SELECT pg_notify('relay_jobs', %s)", (run_id,))


def jobs_delete(cur, run_id):
    cur.execute("DELETE FROM jobs WHERE run_id=%s", (run_id,))


def jobs_assert_lease(cur, run_id, worker_id):
    """Fencing: every engine write proves it still holds the lease, under a row lock. A zombie worker cannot write."""
    cur.execute("SELECT 1 FROM jobs WHERE run_id=%s AND locked_by=%s AND locked_until > now() FOR UPDATE", (run_id, worker_id))
    if cur.fetchone() is None:
        raise LeaseLost(run_id)


# ------------------------------------------------------------------ persistence helpers
_JSON_RUN = {"error", "risk_flags"}
_JSON_STEP = {"attempts", "resolved_input", "output", "error", "gate", "meta"}


def update_run(cur, run_id, **f):
    from relay.db import J
    sets = ", ".join(f"{k}=%s" for k in f) + ", updated_at=now()"
    cur.execute(f"UPDATE runs SET {sets} WHERE id=%s", (*[J(v) if k in _JSON_RUN and v is not None else v for k, v in f.items()], run_id))


def update_step(cur, step_id, **f):
    from relay.db import J
    sets = ", ".join(f"{k}=%s" for k in f)
    cur.execute(f"UPDATE steps SET {sets} WHERE id=%s RETURNING *", (*[J(v) if k in _JSON_STEP and v is not None else v for k, v in f.items()], step_id))
    return cur.fetchone()


def transition(cur, run, to, **fields):
    if to not in ALLOWED.get(run["status"], set()):
        raise RuntimeError(f"Illegal run transition {run['status']} -> {to}")
    if to in TERMINAL:
        fields.setdefault("finished_at", utcnow())
    update_run(cur, run["id"], status=to, **fields)
    run.update(fields)
    run["status"] = to


def outputs_for_run(cur, run_id):
    cur.execute("SELECT node_id, output FROM steps WHERE run_id=%s AND status='succeeded' ORDER BY sequence", (run_id,))
    return {r["node_id"]: r["output"] for r in cur.fetchall()}


def close_attempt(attempts, outcome, now, **extra):
    attempts = [dict(a) for a in (attempts or [])] or [{"n": 1, "started_at": now.isoformat()}]
    last = attempts[-1]
    started = datetime.fromisoformat(last["started_at"])
    last.update(outcome=outcome, finished_at=now.isoformat(), duration_ms=int((now - started).total_seconds() * 1000))
    last.update({k: v for k, v in extra.items() if v is not None})
    return attempts


def backoff_seconds(attempt, base, cap, rand=None):
    import random
    raw = min(base * (2 ** max(attempt - 1, 0)), cap)
    return round(raw * (0.8 + 0.4 * (rand() if rand else random.random())), 3)


# ------------------------------------------------------------------ the runner
class Tick(enum.Enum):
    CONTINUE = "continue"
    YIELD = "yield"
    DONE = "done"


@dataclass
class Prepared:
    run: dict
    step: dict
    node: dict
    spec: object
    immediate: object = None


class Engine:
    def __init__(self, db, catalog, settings, *, provider=None, world=None, faults=None):
        self.db, self.catalog, self.settings = db, catalog, settings
        self.world = world or WorldClient(settings)
        self.provider = provider or ai.build_provider(settings)
        self.faults = faults

    def process(self, run_id, worker_id):
        for _ in range(200):
            try:
                result = self.tick(run_id, worker_id)
            except LeaseLost:
                log.warning("lease lost on %s; another worker owns it", run_id)
                return
            if result is not Tick.CONTINUE:
                return
        with self.db.tx() as cur:
            jobs_release(cur, run_id, worker_id, utcnow())

    def tick(self, run_id, worker_id) -> Tick:
        p = self._prepare(run_id, worker_id)
        if isinstance(p, Tick):
            return p
        outcome = p.immediate if p.immediate is not None else self._execute(p)
        return self._commit(run_id, worker_id, p, outcome)

    # ---- helpers
    def _budget(self, limits):
        return int(limits.get("max_ai_tokens") or self.settings.default_max_ai_tokens)

    def _clamp(self, run, at):
        t = (run["definition_snapshot"].get("limits") or {}).get("timeout_seconds")
        if not t or not run.get("started_at"):
            return at
        return min(at, run["started_at"] + timedelta(seconds=float(t) + float(run["waiting_seconds"] or 0) + 0.05))

    def _fail(self, cur, run, step, now, code, message, node_id=None):
        if step is not None:
            update_step(cur, step["id"], status="failed", error={"code": code, "message": message}, finished_at=now,
                        duration_ms=int((now - step["started_at"]).total_seconds() * 1000))
        err = {"code": code, "message": message}
        if node_id:
            err["node_id"] = node_id
        transition(cur, run, "failed", error=err, ended_reason="failed")
        jobs_delete(cur, run["id"])
        publish_event(cur, run["id"], "run.failed")

    def _cancel(self, cur, run, step, now):
        if step is not None:
            update_step(cur, step["id"], status="cancelled", finished_at=now)
        cur.execute("UPDATE approvals SET status='closed', decided_by='system', decided_at=now(), decision_note='run cancelled' WHERE run_id=%s AND status='pending'", (run["id"],))
        transition(cur, run, "cancelled", ended_reason="cancelled_by_user", error={"code": "cancelled", "message": "Run cancelled by operator"})
        jobs_delete(cur, run["id"])
        publish_event(cur, run["id"], "run.cancelled")

    # ---- phase 1
    def _prepare(self, run_id, worker_id):
        with self.db.tx() as cur:
            cur.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE", (run_id,))
            run = cur.fetchone()
            if run is None:
                jobs_delete(cur, run_id)
                return Tick.DONE
            jobs_assert_lease(cur, run_id, worker_id)
            now = utcnow()
            if run["status"] in TERMINAL or run["status"] == "waiting_approval":
                jobs_delete(cur, run_id)
                return Tick.DONE
            cur.execute("SELECT * FROM steps WHERE run_id=%s AND status IN ('running','retrying','sleeping') ORDER BY sequence DESC LIMIT 1 FOR UPDATE", (run_id,))
            step = cur.fetchone()
            if run["cancel_requested"]:
                self._cancel(cur, run, step, now)
                return Tick.DONE
            if run["status"] == "queued":
                transition(cur, run, "running", started_at=now)
            snap = run["definition_snapshot"]
            limits = snap.get("limits") or {}
            if limits.get("timeout_seconds"):
                active = (now - run["started_at"]).total_seconds() - float(run["waiting_seconds"] or 0)  # approval waits excluded
                if active > float(limits["timeout_seconds"]):
                    self._fail(cur, run, step, now, "timeout_exceeded",
                               f"Run stopped by wall-clock cap: limits.timeout_seconds={limits['timeout_seconds']} exceeded after {active:.1f}s")
                    return Tick.DONE
            nodes = node_index(snap)
            if step is None:
                node_id = run["current_node_id"]
                if node_id is None:
                    transition(cur, run, "succeeded", ended_reason="completed")
                    jobs_delete(cur, run_id)
                    publish_event(cur, run_id, "run.succeeded")
                    return Tick.DONE
                max_steps = int(limits.get("max_steps", 0))
                if run["steps_executed"] >= max_steps:
                    self._fail(cur, run, None, now, "max_steps_exceeded",
                               f"Run stopped by step cap: limits.max_steps={max_steps} reached before executing node '{node_id}' "
                               f"(would be step {run['steps_executed'] + 1})", node_id)
                    return Tick.DONE
                node = nodes[node_id]
                spec = self.catalog.nodes[node["type"]]
                seq = run["steps_executed"] + 1
                try:
                    params = resolve_params(node.get("params") or {}, spec, TemplateContext(run["input"], outputs_for_run(cur, run_id), run_id))
                except TemplateError as exc:
                    from relay.db import J
                    cur.execute("""INSERT INTO steps(run_id, sequence, node_id, node_type, status, error, finished_at)
                                   VALUES (%s,%s,%s,%s,'failed',%s,now())""", (run_id, seq, node_id, spec.type, J({"code": "template_error", "message": str(exc)})))
                    update_run(cur, run_id, steps_executed=seq)
                    self._fail(cur, run, None, now, "template_error", str(exc), node_id)
                    return Tick.DONE
                key = f"{run_id}:{node_id}:{seq}" if is_side_effect(spec, params) else None
                from relay.db import J
                # Persist intent (incl. the idempotency key) BEFORE any side effect.
                cur.execute("""INSERT INTO steps(run_id, sequence, node_id, node_type, status, resolved_input, idempotency_key, started_at)
                               VALUES (%s,%s,%s,%s,'running',%s,%s,%s) RETURNING *""", (run_id, seq, node_id, spec.type, J(params), key, now))
                step = cur.fetchone()
                update_run(cur, run_id, steps_executed=seq)
            node = nodes[step["node_id"]]
            spec = self.catalog.nodes[node["type"]]
            if step["status"] == "sleeping":
                if now < step["wake_at"]:
                    jobs_release(cur, run_id, worker_id, self._clamp(run, step["wake_at"]))
                    return Tick.YIELD
                return Prepared(run, step, node, spec, Completed({"slept_seconds": round((now - step["started_at"]).total_seconds(), 2)}))
            if step["status"] == "retrying" and step["next_attempt_at"] and now < step["next_attempt_at"]:
                jobs_release(cur, run_id, worker_id, self._clamp(run, step["next_attempt_at"]))
                return Tick.YIELD
            if spec.requires_approval:
                # THE GATE: a database fact, not a prompt. Only an authenticated human decision writes 'approved'.
                cur.execute("""SELECT id FROM approvals WHERE run_id=%s AND status='approved' AND step_sequence < %s
                               ORDER BY step_sequence DESC LIMIT 1""", (run_id, step["sequence"]))
                row = cur.fetchone()
                update_step(cur, step["id"], gate={"requires_approval": True, "satisfied_by": row["id"] if row else None, "checked_at": now.isoformat()})
                if row is None:
                    self._fail(cur, run, step, now, "approval_required",
                               f"Node '{node['id']}' ({spec.type}) requires an approved approval earlier in this run; none exists, so the engine refused to execute it", node["id"])
                    return Tick.DONE
            if spec.type == "ai" and run["ai_tokens_used"] >= self._budget(limits):
                self._fail(cur, run, step, now, "token_budget_exceeded",
                           f"Run stopped by AI token budget: limit {self._budget(limits)}, used {run['ai_tokens_used']}", node["id"])
                return Tick.DONE
            attempts = [dict(a, outcome="interrupted", note="worker stopped before this attempt finished") if a.get("outcome") == "in_flight" else a
                        for a in (step["attempts"] or [])]
            attempts.append({"n": step["attempt"] + 1, "started_at": now.isoformat(), "outcome": "in_flight"})
            step = update_step(cur, step["id"], status="running", attempt=step["attempt"] + 1, attempts=attempts)
            publish_event(cur, run_id, "step.started")
            return Prepared(run, step, node, spec)

    # ---- phase 2
    def _execute(self, p):
        ctx = NodeContext(p.run["id"], p.node["id"], p.step["resolved_input"] or {}, p.step["idempotency_key"], p.run["input"],
                          utcnow(), self.world, self.provider, self.settings)
        try:
            outcome = HANDLERS[p.spec.type](ctx)
        except StepError as exc:
            outcome = exc
        except Exception as exc:  # a handler bug fails the step, never the engine
            log.exception("handler crashed")
            outcome = PermanentError("internal_error", f"{type(exc).__name__}: {exc}")
        if self.faults and p.step["idempotency_key"]:
            self.faults.maybe_crash("crash_after_side_effect", p.node["id"], p.run["id"])
        return outcome

    # ---- phase 3
    def _commit(self, run_id, worker_id, p, outcome):
        with self.db.tx() as cur:
            cur.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE", (run_id,))
            run = cur.fetchone()
            jobs_assert_lease(cur, run_id, worker_id)
            now = utcnow()
            step = p.step
            dur = int((now - step["started_at"]).total_seconds() * 1000)
            limits = run["definition_snapshot"].get("limits") or {}

            def tok(t):
                if not t:
                    return {}
                return {"tokens_prompt": (step["tokens_prompt"] or 0) + t.prompt, "tokens_completion": (step["tokens_completion"] or 0) + t.completion}

            def meta(new):
                old = step["meta"] or {}
                m = {**old, **(new or {})}
                if old.get("ai_calls") and (new or {}).get("ai_calls"):
                    m["ai_calls"] = old["ai_calls"] + new["ai_calls"]
                return m

            if isinstance(outcome, Completed):
                update_step(cur, step["id"], status="succeeded", output=outcome.output, error=None, finished_at=now, duration_ms=dur,
                            attempts=close_attempt(step["attempts"], "succeeded", now, replayed=outcome.meta.get("replayed")),
                            meta=meta(outcome.meta), **tok(outcome.tokens))
                node = p.node
                nxt = (node.get("on_true") if outcome.output.get("result") else node.get("on_false")) if node["type"] == "condition" else node.get("next")
                fields = {"current_node_id": nxt}
                if outcome.tokens:
                    fields["ai_tokens_used"] = run["ai_tokens_used"] + outcome.tokens.total
                update_run(cur, run_id, **fields)
                publish_event(cur, run_id, "step.succeeded")
                if outcome.tokens and fields["ai_tokens_used"] > self._budget(limits):
                    self._fail(cur, run, None, now, "token_budget_exceeded",
                               f"Run stopped by AI token budget: {fields['ai_tokens_used']} tokens used > limit {self._budget(limits)}", node["id"])
                    return Tick.DONE
                return Tick.CONTINUE
            if isinstance(outcome, Sleep):
                update_step(cur, step["id"], status="sleeping", wake_at=outcome.wake_at,
                            attempts=close_attempt(step["attempts"], "sleeping", now, wake_at=outcome.wake_at.isoformat()))
                jobs_release(cur, run_id, worker_id, now if run["cancel_requested"] else self._clamp(run, outcome.wake_at))
                publish_event(cur, run_id, "step.sleeping")
                return Tick.YIELD
            if isinstance(outcome, AwaitApproval):
                if run["cancel_requested"]:
                    self._cancel(cur, run, step, now)
                    return Tick.DONE
                apr = new_id("apr")
                cur.execute("INSERT INTO approvals(id, run_id, node_id, step_sequence, message, status) VALUES (%s,%s,%s,%s,%s,'pending')",
                            (apr, run_id, p.node["id"], step["sequence"], outcome.message))
                update_step(cur, step["id"], status="waiting", approval_id=apr, output={"approval_id": apr, "message": outcome.message},
                            attempts=close_attempt(step["attempts"], "waiting_approval", now))
                transition(cur, run, "waiting_approval", waiting_since=now)
                jobs_delete(cur, run_id)
                publish_event(cur, run_id, "run.waiting_approval")
                return Tick.DONE
            err = outcome
            if err.tokens:
                update_run(cur, run_id, ai_tokens_used=run["ai_tokens_used"] + err.tokens.total)
            if isinstance(err, TransientError) and step["attempt"] < self.settings.retry_max_attempts:
                delay = backoff_seconds(step["attempt"], self.settings.retry_base_s, self.settings.retry_max_backoff_s)
                nat = now + timedelta(seconds=delay)
                update_step(cur, step["id"], status="retrying", next_attempt_at=nat, error=err.to_dict(), meta=meta(err.meta), **tok(err.tokens),
                            attempts=close_attempt(step["attempts"], "transient_error", now, error=err.to_dict(), retry_in_s=delay))
                jobs_release(cur, run_id, worker_id, now if run["cancel_requested"] else self._clamp(run, nat))
                publish_event(cur, run_id, "step.retrying")
                return Tick.YIELD
            code, message = err.code, err.message
            if isinstance(err, TransientError):
                code, message = "retries_exhausted", f"Node '{p.node['id']}' failed after {step['attempt']} attempts: {err.message}"
            update_step(cur, step["id"], attempts=close_attempt(step["attempts"], "failed", now, error=err.to_dict()), meta=meta(err.meta), **tok(err.tokens))
            self._fail(cur, run, step, now, code, message, p.node["id"])
            return Tick.DONE
