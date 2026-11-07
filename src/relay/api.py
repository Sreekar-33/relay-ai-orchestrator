"""REST API + console. Triggers only enqueue; the worker executes runs."""
from __future__ import annotations

import hmac
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import uvicorn
from fastapi import Body, Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException

from relay import services as S
from relay.ai import build_provider
from relay.config import Settings
from relay.db import Database, migrate
from relay.errors import RelayError, StepError, TransientError
from relay.validation import load_catalog

log = logging.getLogger("relay.api")


def create_app(settings: Settings | None = None, db: Database | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    owns_db = db is None
    db = db or Database(settings.database_url, max_size=20)
    catalog = load_catalog(settings.data_dir / "node_catalog.json")
    workflows = S.WorkflowService(db, catalog)
    triggers = S.TriggerService(db, workflows, settings)
    provider = build_provider(settings)

    @asynccontextmanager
    async def lifespan(app):
        migrate(db)
        S.seed_all(db, catalog, settings)
        yield
        if owns_db:
            db.close()

    app = FastAPI(title="Relay", version="1.0.0", lifespan=lifespan,
                  description="AI workflow orchestrator. Auth: `Authorization: Bearer <RELAY_API_TOKEN>` (webhooks use `X-Relay-Secret`).")

    @app.exception_handler(RelayError)
    async def _relay_error(_, exc: RelayError):
        return JSONResponse(status_code=exc.status, content=exc.to_body())

    @app.exception_handler(StepError)
    async def _step_error(_, exc: StepError):
        t = isinstance(exc, TransientError)
        return JSONResponse(status_code=503 if t else 502, content={"error": {"code": exc.code, "message": exc.message, "retryable": t}})

    @app.exception_handler(RequestValidationError)
    async def _bad_request(_, exc):
        return JSONResponse(status_code=400, content={"error": {"code": "invalid_request", "message": "Request body is invalid", "details": exc.errors()}})

    @app.exception_handler(StarletteHTTPException)
    async def _http(_, exc):
        return JSONResponse(status_code=exc.status_code, content={"error": {"code": "not_found" if exc.status_code == 404 else "http_error", "message": str(exc.detail)}})

    @app.exception_handler(Exception)
    async def _unhandled(_, exc):
        log.exception("unhandled")
        return JSONResponse(status_code=500, content={"error": {"code": "internal_error", "message": "Internal server error"}})

    def auth(request: Request):
        h = request.headers.get("authorization", "")
        tok = h[7:] if h.lower().startswith("bearer ") else ""   # header only: tokens in URLs leak into access logs
        if not tok or not hmac.compare_digest(tok.encode(), settings.api_token.encode()):
            raise RelayError("unauthorized", "Missing or invalid bearer token", 401)

    A = [Depends(auth)]

    # ---------------- public
    @app.get("/health", tags=["meta"])
    def health():
        with db.tx() as cur:
            cur.execute("SELECT 1")
        return {"status": "ok", "ai_provider": provider.name, "ai_model": provider.model}

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/console/")

    @app.post("/hooks/{wf_id}", status_code=202, tags=["triggers"])
    async def webhook(wf_id: str, request: Request):
        raw = await request.body()
        run = await run_in_threadpool(triggers.webhook, wf_id, request.headers.get("x-relay-secret"), raw)
        return {"run_id": run["id"], "id": run["id"], "status": run["status"]}

    # ---------------- workflows
    @app.get("/workflows", dependencies=A, tags=["workflows"])
    def list_workflows():
        return [S.serialize_workflow(w) for w in workflows.list()]

    @app.post("/workflows", status_code=201, dependencies=A, tags=["workflows"])
    def create_workflow(definition=Body(...)):
        return S.serialize_workflow(workflows.create(definition), full=True)

    @app.get("/workflows/{wf_id}", dependencies=A, tags=["workflows"])
    def get_workflow(wf_id: str):
        out = S.serialize_workflow(workflows.get(wf_id), full=True)
        out["validation"] = workflows.validate(wf_id).to_dict()
        out["versions"] = [{"version": v["version"], "published_at": S.iso(v["published_at"])} for v in workflows.versions(wf_id)]
        return out

    @app.put("/workflows/{wf_id}", dependencies=A, tags=["workflows"])
    def update_workflow(wf_id: str, definition=Body(...)):
        return S.serialize_workflow(workflows.update(wf_id, definition), full=True)

    @app.post("/workflows/{wf_id}/validate", dependencies=A, tags=["workflows"])
    def validate_workflow(wf_id: str):
        return workflows.validate(wf_id).to_dict()

    @app.post("/workflows/{wf_id}/publish", dependencies=A, tags=["workflows"])
    def publish_workflow(wf_id: str):
        return workflows.publish(wf_id)

    @app.post("/workflows/{wf_id}/trigger", status_code=202, dependencies=A, tags=["triggers"])
    def trigger(wf_id: str, body=Body(default=None)):
        if body is not None and not isinstance(body, dict):
            raise RelayError("invalid_request", 'Body must be a JSON object like {"input": {...}}', 400)
        run = triggers.manual(wf_id, (body or {}).get("input", {}))
        return {"run_id": run["id"], "id": run["id"], "status": run["status"]}

    # ---------------- NL compiler (plain English -> draft workflow, validated)
    from relay.compiler import NLCompiler
    compiler = NLCompiler(provider, catalog, settings)

    @app.post("/workflows/compile", dependencies=A, tags=["compiler"])
    def compile_workflow(body=Body(...)):
        if not isinstance(body, dict) or not isinstance(body.get("description"), str):
            raise RelayError("invalid_request", 'Body must be {"description": "...", "save": true}', 400)
        r = compiler.compile(body["description"])
        meta = {"attempts": r.attempts, "tokens": {"prompt": r.tokens.prompt, "completion": r.tokens.completion}}
        if r.kind == "refusal":
            return {"refusal": r.refusal, "explanation": r.explanation, "compile": meta}
        if r.kind == "failed":
            raise RelayError("compile_failed", "The compiler could not produce a valid workflow", 422, {"issues": r.issues, "compile": meta})
        if body.get("save", True):
            workflows.create(r.workflow)
        trig = r.workflow["trigger"]
        webhook = {"url": f"/hooks/{r.workflow['id']}", "secret": trig["secret"], "note": "Shown once. Masked everywhere else."} if trig.get("type") == "webhook" else None
        return JSONResponse(status_code=201, content={"workflow": {"id": r.workflow["id"], "status": "draft", "definition": S.mask_definition(r.workflow)},
                                                      "webhook": webhook, "explanation": r.explanation, "warnings": r.warnings,
                                                      "notes": r.notes, "compile": meta})

    # ---------------- runs
    @app.get("/runs", dependencies=A, tags=["runs"])
    def runs(workflow_id: str | None = None, status: str | None = None, limit: int = Query(50, ge=1, le=200)):
        return S.list_runs(db, workflow_id, status, limit)

    @app.get("/runs/{run_id}", dependencies=A, tags=["runs"])
    def run(run_id: str):
        return S.get_run(db, run_id)

    @app.post("/runs/{run_id}/cancel", dependencies=A, tags=["runs"])
    def cancel(run_id: str):
        res = S.cancel_run(db, run_id)
        return JSONResponse(status_code=202 if res["status"] == "running" else 200, content=res)

    # ---------------- approvals
    @app.get("/approvals", dependencies=A, tags=["approvals"])
    def approvals(status: str | None = None):
        return [S.serialize_approval(a) for a in S.list_approvals(db, status)]

    def _decide(approval_id, decision, body):
        body = body if isinstance(body, dict) else {}
        return S.decide_approval(db, approval_id, decision, body.get("decided_by") or "demo-operator", body.get("note"))

    @app.post("/approvals/{approval_id}/approve", dependencies=A, tags=["approvals"])
    def approve(approval_id: str, body=Body(default=None)):
        return _decide(approval_id, "approved", body)

    @app.post("/approvals/{approval_id}/reject", dependencies=A, tags=["approvals"])
    def reject(approval_id: str, body=Body(default=None)):
        return _decide(approval_id, "rejected", body)

    # ---------------- meta
    @app.get("/catalog", dependencies=A, tags=["meta"])
    def get_catalog():
        return catalog.raw

    @app.get("/samples", dependencies=A, tags=["meta"])
    def samples():
        return [json.loads(l) for l in (settings.data_dir / "sample_payloads.jsonl").read_text().splitlines() if l.strip()]

    @app.get("/stats", dependencies=A, tags=["meta"])
    def stats():
        with db.tx() as cur:
            cur.execute("SELECT status, count(*) AS n FROM runs GROUP BY status")
            by = {r["status"]: r["n"] for r in cur.fetchall()}
            cur.execute("SELECT (SELECT count(*) FROM approvals WHERE status='pending') AS p, (SELECT count(*) FROM workflows) AS w, "
                        "(SELECT COALESCE(sum(ai_tokens_used),0) FROM runs) AS t")
            row = cur.fetchone()
        return {"runs_by_status": by, "pending_approvals": row["p"], "workflows": row["w"], "ai_tokens_total": int(row["t"])}

    # ---------------- mock world proxy (console demo convenience)
    def _world(method, path, body=None):
        try:
            return httpx.request(method, settings.mock_world_url + path, json=body, timeout=5).json()
        except (httpx.HTTPError, ValueError) as exc:
            raise RelayError("world_unreachable", f"Mock world unreachable: {type(exc).__name__}", 502)

    @app.get("/world/ledger", dependencies=A, tags=["mock world"])
    def world_ledger():
        return _world("GET", "/admin/ledger")

    @app.post("/world/config", dependencies=A, tags=["mock world"])
    def world_config(body=Body(default=None)):
        return _world("POST", "/admin/config", body or {})

    @app.post("/world/reset", dependencies=A, tags=["mock world"])
    def world_reset():
        return _world("POST", "/admin/reset", {})

    console = Path(__file__).resolve().parent / "console"
    if console.exists():
        app.mount("/console", StaticFiles(directory=console, html=True), name="console")
    return app


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    uvicorn.run(create_app(), host="0.0.0.0", port=8080)
