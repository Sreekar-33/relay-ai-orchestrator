"""HTTP-layer contract tests through the real FastAPI app (error codes, auth, cancel semantics, secret hygiene)."""
import pytest
from fastapi.testclient import TestClient

from relay.api import create_app
from tests.conftest import drive

H = {"Authorization": "Bearer relay-demo-token"}
LARGE = {"employee_email": "dev1@example.com", "amount_usd": 250, "description": "Conference ticket"}


@pytest.fixture
def client(env):
    with TestClient(create_app(env.settings, db=env.db)) as c:
        yield c


def test_auth_header_required_and_query_token_rejected(client):
    assert client.get("/workflows").status_code == 401
    assert client.get("/workflows?token=relay-demo-token").status_code == 401   # tokens in URLs leak into logs
    assert client.get("/workflows", headers=H).status_code == 200


def test_webhook_secret_and_payload_checks(client):
    hook = "/hooks/wf_expense_approval"
    assert client.post(hook, json=LARGE, headers={"X-Relay-Secret": "wrong"}).status_code == 401
    assert client.post(hook, json=LARGE).status_code == 401
    assert client.post(hook, content=b"[1,2]", headers={"X-Relay-Secret": "whsec_expense_774"}).status_code == 400
    r = client.post(hook, json=LARGE, headers={"X-Relay-Secret": "whsec_expense_774"})
    assert r.status_code == 202 and r.json()["run_id"].startswith("run_")
    assert "whsec_expense_774" not in client.get(f"/runs/{r.json()['run_id']}", headers=H).text


def test_publish_validation_and_unpublished_trigger(client):
    bad = {"id": "wf_bad", "name": "b", "trigger": {"type": "manual"}, "entry": "n1", "limits": {"max_steps": 5},
           "nodes": [{"id": "n1", "type": "teleport", "params": {}, "next": None}]}
    assert client.post("/workflows", json=bad, headers=H).status_code == 201
    r = client.post("/workflows/wf_bad/publish", headers=H)
    assert r.status_code == 422 and r.json()["error"]["code"] == "invalid_node_type"
    assert client.post("/workflows/wf_bad/trigger", json={"input": {}}, headers=H).status_code == 409
    assert client.get("/workflows/wf_nope", headers=H).status_code == 404
    assert "whsec_" not in client.get("/workflows/wf_support_triage", headers=H).text


def test_approval_twice_is_409_and_cancel_semantics(client, env):
    rid = client.post("/workflows/wf_expense_approval/trigger", json={"input": LARGE}, headers=H).json()["run_id"]
    drive(env.engine(), env.db, rid)
    apr = client.get("/approvals?status=pending", headers=H).json()[0]
    assert client.post(f"/approvals/{apr['id']}/approve", json={"decided_by": "maya"}, headers=H).status_code == 200
    r = client.post(f"/approvals/{apr['id']}/approve", headers=H)
    assert r.status_code == 409 and r.json()["error"]["code"] == "approval_already_decided"
    assert drive(env.engine(), env.db, rid) == "succeeded"
    r = client.post(f"/runs/{rid}/cancel", headers=H)
    assert r.status_code == 409 and r.json()["error"]["code"] == "run_not_cancellable"
    # waiting_approval -> cancelled, approval closed and no longer pending
    rid2 = client.post("/workflows/wf_expense_approval/trigger", json={"input": LARGE}, headers=H).json()["run_id"]
    drive(env.engine(), env.db, rid2)
    assert client.post(f"/runs/{rid2}/cancel", headers=H).json()["status"] == "cancelled"
    assert [a for a in client.get("/approvals?status=pending", headers=H).json() if a["run_id"] == rid2] == []
    # queued -> cancelled immediately, nothing executed
    rid3 = client.post("/workflows/wf_runaway/trigger", json={"input": {}}, headers=H).json()["run_id"]
    assert client.post(f"/runs/{rid3}/cancel", headers=H).json()["status"] == "cancelled"
    assert client.get(f"/runs/{rid3}", headers=H).json()["steps"] == []
