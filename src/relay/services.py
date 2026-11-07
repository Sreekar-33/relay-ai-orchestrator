"""Business operations: workflows (draft -> publish, immutable versions), seeding, triggers, approvals, cancel, serializers."""
from __future__ import annotations

import copy
import hmac
import json
import logging

from relay import ai
from relay.db import J
from relay.engine import TERMINAL, jobs_delete, jobs_enqueue, node_index, publish_event, transition, update_run, utcnow
from relay.errors import RelayError
from relay.ids import new_id
from relay.validation import check_shape, validate_definition

log = logging.getLogger("relay.services")
MASK = "••••••"


def mask_definition(defn: dict) -> dict:
    out = copy.deepcopy(defn)
    trig = out.get("trigger")
    if isinstance(trig, dict) and "secret" in trig:
        trig["secret_set"] = bool(trig.get("secret"))
        trig["secret"] = MASK
    return out


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z") if dt else None


# ------------------------------------------------------------------ workflows
class WorkflowService:
    def __init__(self, db, catalog):
        self.db, self.catalog = db, catalog

    def _get(self, cur, wf_id, lock=False):
        cur.execute("SELECT * FROM workflows WHERE id=%s" + (" FOR UPDATE" if lock else ""), (wf_id,))
        wf = cur.fetchone()
        if wf is None:
            raise RelayError("workflow_not_found", f"Workflow '{wf_id}' not found", 404)
        return wf

    def create(self, defn) -> dict:
        defn = copy.deepcopy(defn)
        if isinstance(defn, dict) and "id" not in defn:
            defn["id"] = new_id("wf")
        issues = check_shape(defn)
        if issues:
            raise RelayError(issues[0].code, issues[0].message, 400, {"errors": [i.to_dict() for i in issues]})
        if isinstance(defn["trigger"], dict):
            defn["trigger"].pop("secret_set", None)
        with self.db.tx() as cur:
            cur.execute("SELECT 1 FROM workflows WHERE id=%s", (defn["id"],))
            if cur.fetchone():
                raise RelayError("workflow_exists", f"Workflow '{defn['id']}' already exists", 409)
            cur.execute("INSERT INTO workflows(id, name, description, status, definition) VALUES (%s,%s,%s,'draft',%s) RETURNING *",
                        (defn["id"], defn["name"], defn.get("description", ""), J(defn)))
            return cur.fetchone()

    def update(self, wf_id, defn) -> dict:
        defn = copy.deepcopy(defn)
        if isinstance(defn, dict):
            defn["id"] = wf_id
        issues = check_shape(defn)
        if issues:
            raise RelayError(issues[0].code, issues[0].message, 400, {"errors": [i.to_dict() for i in issues]})
        with self.db.tx() as cur:
            cur_wf = self._get(cur, wf_id, lock=True)
            trig = defn["trigger"]
            trig.pop("secret_set", None)
            if trig.get("secret") == MASK:  # masked secret round-trips to the stored one
                old = (cur_wf["definition"].get("trigger") or {}).get("secret")
                trig["secret"] = old
            cur.execute("""UPDATE workflows SET definition=%s, name=%s, description=%s, updated_at=now(),
                           has_unpublished_changes=(current_version IS NOT NULL) WHERE id=%s RETURNING *""",
                        (J(defn), defn["name"], defn.get("description", ""), wf_id))
            return cur.fetchone()

    def get(self, wf_id) -> dict:
        with self.db.tx() as cur:
            return self._get(cur, wf_id)

    def list(self) -> list:
        with self.db.tx() as cur:
            cur.execute("SELECT * FROM workflows ORDER BY created_at, id")
            return cur.fetchall()

    def versions(self, wf_id) -> list:
        with self.db.tx() as cur:
            cur.execute("SELECT version, published_at FROM workflow_versions WHERE workflow_id=%s ORDER BY version DESC", (wf_id,))
            return cur.fetchall()

    def validate(self, wf_id):
        return validate_definition(self.get(wf_id)["definition"], self.catalog)

    def publish(self, wf_id) -> dict:
        with self.db.tx() as cur:
            wf = self._get(cur, wf_id, lock=True)
            res = validate_definition(wf["definition"], self.catalog)
            if not res.ok:
                first = res.errors[0]
                raise RelayError(first.code, first.message, 422, res.to_dict())
            version = (wf["current_version"] or 0) + 1
            cur.execute("INSERT INTO workflow_versions(workflow_id, version, definition) VALUES (%s,%s,%s)", (wf_id, version, J(wf["definition"])))
            cur.execute("UPDATE workflows SET status='published', current_version=%s, has_unpublished_changes=false, updated_at=now() WHERE id=%s",
                        (version, wf_id))
        return {"id": wf_id, "status": "published", "version": version, "warnings": [w.to_dict() for w in res.warnings]}

    def published_definition(self, cur, wf_id):
        wf = self._get(cur, wf_id)
        if wf["current_version"] is None:
            raise RelayError("workflow_not_published", f"Workflow '{wf_id}' is a draft; publish it before triggering", 409)
        cur.execute("SELECT definition FROM workflow_versions WHERE workflow_id=%s AND version=%s", (wf_id, wf["current_version"]))
        return cur.fetchone()["definition"], wf["current_version"]


def seed_all(db, catalog, settings) -> list:
    svc, seeded = WorkflowService(db, catalog), []
    files = [settings.data_dir / "seed_workflows.json", settings.data_dir / "extra" / "demo_workflows.json"]
    for path in files:
        if not path.exists():
            continue
        for defn in json.loads(path.read_text())["workflows"]:
            with db.tx() as cur:
                cur.execute("SELECT 1 FROM workflows WHERE id=%s", (defn["id"],))
                if cur.fetchone():
                    continue
            try:
                svc.create(defn)
                svc.publish(defn["id"])
                seeded.append(defn["id"])
            except RelayError as exc:  # concurrent startup (api + worker) may race on the same seed
                log.info("seed %s skipped: %s", defn["id"], exc.message)
    return seeded


# ------------------------------------------------------------------ triggers
class TriggerService:
    def __init__(self, db, workflows, settings):
        self.db, self.workflows, self.settings = db, workflows, settings

    def _create_run(self, cur, wf_id, definition, version, trigger_type, body):
        run_id = new_id("run")
        cur.execute("""INSERT INTO runs(id, workflow_id, workflow_version, definition_snapshot, status, trigger_type, input, current_node_id, risk_flags)
                       VALUES (%s,%s,%s,%s,'queued',%s,%s,%s,%s) RETURNING *""",
                    (run_id, wf_id, version, J(copy.deepcopy(definition)), trigger_type, J(body), definition.get("entry"),
                     J(ai.scan_untrusted(body))))
        run = cur.fetchone()
        jobs_enqueue(cur, run_id)  # the worker executes; never the request handler
        publish_event(cur, run_id, "run.created")
        return run

    def manual(self, wf_id, body):
        if body is None:
            body = {}
        if not isinstance(body, dict):
            raise RelayError("invalid_input", "'input' must be a JSON object", 400)
        with self.db.tx() as cur:
            definition, version = self.workflows.published_definition(cur, wf_id)
            return self._create_run(cur, wf_id, definition, version, "manual", body)

    def webhook(self, wf_id, secret, raw: bytes):
        if len(raw) > self.settings.max_body_bytes:
            raise RelayError("payload_too_large", f"Webhook body exceeds {self.settings.max_body_bytes} bytes", 413)
        with self.db.tx() as cur:
            definition, version = self.workflows.published_definition(cur, wf_id)
            trig = definition.get("trigger") or {}
            if trig.get("type") != "webhook":
                raise RelayError("not_a_webhook_workflow", f"Workflow '{wf_id}' is not triggered by webhook", 409)
            expected = str(trig.get("secret") or "")
            if not secret or not expected or not hmac.compare_digest(secret.encode(), expected.encode()):
                raise RelayError("invalid_webhook_secret", "Missing or invalid X-Relay-Secret header", 401)
            try:
                body = json.loads(raw.decode("utf-8") or "{}")
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise RelayError("invalid_payload", "Webhook body must be valid UTF-8 JSON", 400)
            if not isinstance(body, dict):
                raise RelayError("invalid_payload", "Webhook body must be a JSON object", 400)
            return self._create_run(cur, wf_id, definition, version, "webhook", body)


# ------------------------------------------------------------------ approvals
def decide_approval(db, approval_id, decision, decided_by="demo-operator", note=None) -> dict:
    decided_by = (str(decided_by or "").strip() or "demo-operator")[:120]
    with db.tx() as cur:
        cur.execute("SELECT run_id FROM approvals WHERE id=%s", (approval_id,))
        peek = cur.fetchone()
        if peek is None:
            raise RelayError("approval_not_found", f"Approval '{approval_id}' not found", 404)
        cur.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE", (peek["run_id"],))  # lock order: run -> approval
        run = cur.fetchone()
        cur.execute("SELECT * FROM approvals WHERE id=%s FOR UPDATE", (approval_id,))
        apr = cur.fetchone()
        if apr["status"] != "pending":
            raise RelayError("approval_already_decided", f"Approval '{approval_id}' is already {apr['status']}", 409)
        if run["status"] != "waiting_approval":
            raise RelayError("run_not_waiting", f"Run '{run['id']}' is {run['status']}, not waiting_approval", 409)
        now = utcnow()
        cur.execute("UPDATE approvals SET status=%s, decided_by=%s, decided_at=%s, decision_note=%s WHERE id=%s RETURNING *",
                    (decision, decided_by, now, note, approval_id))
        decided = cur.fetchone()
        cur.execute("SELECT * FROM steps WHERE run_id=%s AND status='waiting' AND approval_id=%s FOR UPDATE", (run["id"], approval_id))
        step = cur.fetchone()
        cur.execute("UPDATE steps SET status=%s, output=%s, finished_at=%s, duration_ms=%s WHERE id=%s",
                    ("succeeded" if decision == "approved" else "cancelled",
                     J({"decision": decision, "decided_by": decided_by, "decided_at": now.isoformat(), "note": note, "approval_id": approval_id}),
                     now, int((now - step["started_at"]).total_seconds() * 1000), step["id"]))
        waited = float(run["waiting_seconds"] or 0) + ((now - run["waiting_since"]).total_seconds() if run["waiting_since"] else 0)
        if decision == "approved":
            nxt = node_index(run["definition_snapshot"])[step["node_id"]].get("next")
            transition(cur, run, "running", current_node_id=nxt, waiting_seconds=waited, waiting_since=None)
            jobs_enqueue(cur, run["id"])
        else:
            transition(cur, run, "cancelled", ended_reason="approval_rejected", waiting_seconds=waited, waiting_since=None,
                       error={"code": "approval_rejected", "message": f"Approval '{approval_id}' rejected by {decided_by}" + (f": {note}" if note else "")})
        publish_event(cur, run["id"], f"approval.{decision}")
        return {**serialize_approval({**decided, "workflow_id": run["workflow_id"]}), "run_status": run["status"]}


def list_approvals(db, status=None) -> list:
    with db.tx() as cur:
        q = "SELECT a.*, r.workflow_id FROM approvals a JOIN runs r ON r.id=a.run_id"
        if status:
            cur.execute(q + " WHERE a.status=%s ORDER BY a.created_at", (status,))
        else:
            cur.execute(q + " ORDER BY a.created_at DESC LIMIT 200")
        return cur.fetchall()


# ------------------------------------------------------------------ runs
def get_run(db, run_id) -> dict:
    with db.tx() as cur:
        cur.execute("SELECT * FROM runs WHERE id=%s", (run_id,))
        run = cur.fetchone()
        if run is None:
            raise RelayError("run_not_found", f"Run '{run_id}' not found", 404)
        cur.execute("SELECT * FROM steps WHERE run_id=%s ORDER BY sequence", (run_id,))
        steps = cur.fetchall()
        cur.execute("SELECT a.*, r.workflow_id FROM approvals a JOIN runs r ON r.id=a.run_id WHERE a.run_id=%s ORDER BY a.step_sequence", (run_id,))
        return serialize_run(run, steps, cur.fetchall())


def list_runs(db, workflow_id=None, status=None, limit=50) -> list:
    where, args = [], []
    if workflow_id:
        where.append("workflow_id=%s"); args.append(workflow_id)
    if status:
        where.append("status=%s"); args.append(status)
    sql = "SELECT * FROM runs" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY created_at DESC, id DESC LIMIT %s"
    with db.tx() as cur:
        cur.execute(sql, (*args, max(1, min(int(limit), 200))))
        return [run_summary(r) for r in cur.fetchall()]


def cancel_run(db, run_id) -> dict:
    with db.tx() as cur:
        cur.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE", (run_id,))
        run = cur.fetchone()
        if run is None:
            raise RelayError("run_not_found", f"Run '{run_id}' not found", 404)
        if run["status"] in TERMINAL:
            raise RelayError("run_not_cancellable", f"Run '{run_id}' is already {run['status']}", 409)
        err = {"code": "cancelled", "message": "Run cancelled by operator"}
        now = utcnow()
        if run["status"] == "queued":
            transition(cur, run, "cancelled", ended_reason="cancelled_by_user", error=err)
            jobs_delete(cur, run_id)
        elif run["status"] == "waiting_approval":
            cur.execute("UPDATE approvals SET status='closed', decided_by='system', decided_at=now(), decision_note='run cancelled' WHERE run_id=%s AND status='pending'", (run_id,))
            cur.execute("UPDATE steps SET status='cancelled', finished_at=now() WHERE run_id=%s AND status='waiting'", (run_id,))
            transition(cur, run, "cancelled", ended_reason="cancelled_by_user", error=err, waiting_since=None)
        else:  # running: cooperative, the worker stops between steps (wake it so a sleeping run cancels now)
            update_run(cur, run_id, cancel_requested=True)
            cur.execute("UPDATE jobs SET available_at=now() WHERE run_id=%s", (run_id,))
            cur.execute("SELECT pg_notify('relay_jobs', %s)", (run_id,))
            run["cancel_requested"] = True
        publish_event(cur, run_id, "run.cancel")
        return {"run_id": run_id, "status": run["status"], "cancel_requested": bool(run["cancel_requested"])}


# ------------------------------------------------------------------ serializers
def serialize_workflow(wf, full=False) -> dict:
    d = wf["definition"]
    out = {"id": wf["id"], "name": wf["name"], "description": wf["description"], "status": wf["status"],
           "current_version": wf["current_version"], "has_unpublished_changes": wf["has_unpublished_changes"],
           "trigger_type": (d.get("trigger") or {}).get("type"), "node_count": len(d.get("nodes") or []),
           "created_at": iso(wf["created_at"]), "updated_at": iso(wf["updated_at"])}
    if full:
        out["definition"] = mask_definition(d)
    return out


def serialize_approval(a) -> dict:
    return {"id": a["id"], "run_id": a["run_id"], "workflow_id": a.get("workflow_id"), "node_id": a["node_id"], "message": a["message"],
            "status": a["status"], "decided_by": a["decided_by"], "decided_at": iso(a["decided_at"]),
            "decision_note": a["decision_note"], "created_at": iso(a["created_at"])}


def run_summary(r) -> dict:
    return {"run_id": r["id"], "id": r["id"], "workflow_id": r["workflow_id"], "workflow_version": r["workflow_version"], "status": r["status"],
            "trigger_type": r["trigger_type"], "steps_executed": r["steps_executed"], "ai_tokens_used": r["ai_tokens_used"],
            "error": r["error"], "ended_reason": r["ended_reason"], "risk_flags": r["risk_flags"],
            "created_at": iso(r["created_at"]), "started_at": iso(r["started_at"]), "finished_at": iso(r["finished_at"])}


def serialize_step(s) -> dict:
    tokens = None
    if s["tokens_prompt"] is not None:
        tokens = {"prompt": s["tokens_prompt"], "completion": s["tokens_completion"] or 0, "total": s["tokens_prompt"] + (s["tokens_completion"] or 0)}
    return {"sequence": s["sequence"], "node_id": s["node_id"], "type": s["node_type"], "status": s["status"], "attempt": s["attempt"],
            "attempts": s["attempts"], "input": s["resolved_input"], "output": s["output"], "error": s["error"], "tokens": tokens,
            "idempotency_key": s["idempotency_key"], "approval_id": s["approval_id"], "gate": s["gate"], "wake_at": iso(s["wake_at"]),
            "next_attempt_at": iso(s["next_attempt_at"]), "meta": s["meta"], "started_at": iso(s["started_at"]),
            "finished_at": iso(s["finished_at"]), "duration_ms": s["duration_ms"]}


def serialize_run(r, steps, approvals) -> dict:
    out = run_summary(r)
    snap = r["definition_snapshot"]
    out.update({"input": r["input"], "current_node_id": r["current_node_id"], "cancel_requested": r["cancel_requested"],
                "limits": snap.get("limits"), "definition_snapshot": mask_definition(snap),
                "steps": [serialize_step(s) for s in steps], "approvals": [serialize_approval(a) for a in approvals]})
    return out
