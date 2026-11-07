import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from dataclasses import replace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TEST_DB = os.environ.get("RELAY_TEST_DATABASE_URL", "postgresql://relay:relay@localhost:5433/relay_test")


class MockWorld:
    def __init__(self, url):
        self.url = url

    def _req(self, method, path, body=None):
        req = urllib.request.Request(self.url + path, data=json.dumps(body).encode() if body is not None else None, method=method,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read() or b"{}")

    def reset(self):
        self._req("POST", "/admin/reset", {})

    def configure(self, **cfg):
        self._req("POST", "/admin/config", cfg)

    def ledger(self):
        return self._req("GET", "/admin/ledger")["entries"]


@pytest.fixture(scope="session")
def world():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]
    proc = subprocess.Popen([sys.executable, str(ROOT / "scripts/mock_world.py"), "--port", str(port)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    mw = MockWorld(f"http://127.0.0.1:{port}")
    for _ in range(50):
        try:
            mw._req("GET", "/health"); break
        except Exception:
            time.sleep(0.1)
    yield mw
    proc.terminate()


@pytest.fixture(scope="session")
def settings(world):
    from relay.config import Settings
    return replace(Settings(), database_url=TEST_DB, mock_world_url=world.url, retry_max_attempts=3, retry_base_s=0.05,
                   retry_max_backoff_s=0.2, lease_seconds=5, http_timeout_s=3.0, ai_provider="fake")


@pytest.fixture(scope="session")
def db(settings):
    import psycopg
    from relay.db import Database, migrate
    base, name = settings.database_url.rsplit("/", 1)
    try:
        with psycopg.connect(base + "/postgres", autocommit=True, connect_timeout=3) as c:
            if not c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone():
                c.execute(f'CREATE DATABASE "{name}"')
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable ({exc}); run `make db`")
    d = Database(settings.database_url)
    migrate(d)
    yield d
    d.close()


@pytest.fixture
def env(db, settings, world):
    """Clean DB + world, seeded workflows, services and an engine factory."""
    from types import SimpleNamespace
    from relay import services as S
    from relay.engine import Engine
    from relay.validation import load_catalog
    with db.tx() as cur:
        cur.execute("TRUNCATE jobs, approvals, steps, runs, workflow_versions, workflows, fault_marks CASCADE")
    world.reset()
    world.configure(mode="ok", fail_rate=0, latency_ms=0)
    catalog = load_catalog(settings.data_dir / "node_catalog.json")
    S.seed_all(db, catalog, settings)
    wf = S.WorkflowService(db, catalog)
    return SimpleNamespace(db=db, world=world, settings=settings, catalog=catalog, workflows=wf,
                           triggers=S.TriggerService(db, wf, settings),
                           engine=lambda provider=None, faults=None: Engine(db, catalog, settings, provider=provider, faults=faults))


def drive(engine, db, run_id, until=("succeeded", "failed", "cancelled", "waiting_approval"), timeout=20, worker="w-test"):
    from relay.engine import jobs_claim
    deadline, status = time.time() + timeout, None
    while time.time() < deadline:
        with db.tx() as cur:
            claimed = jobs_claim(cur, worker, 5)
        if claimed:
            engine.process(claimed, worker)
        with db.tx() as cur:
            cur.execute("SELECT status FROM runs WHERE id=%s", (run_id,))
            status = cur.fetchone()["status"]
        if status in until:
            return status
        if not claimed:
            time.sleep(0.02)
    raise AssertionError(f"run {run_id} stuck in {status}")


def steps_of(db, run_id):
    with db.tx() as cur:
        cur.execute("SELECT * FROM steps WHERE run_id=%s ORDER BY sequence", (run_id,))
        return cur.fetchall()


def run_row(db, run_id):
    with db.tx() as cur:
        cur.execute("SELECT * FROM runs WHERE id=%s", (run_id,))
        return cur.fetchone()
