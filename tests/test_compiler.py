from relay.ai import FakeProvider
from relay.compiler import NLCompiler
from relay.config import Settings
from relay.validation import load_catalog, unguarded_sensitive_nodes, validate_definition

S = Settings()
CAT = load_catalog(S.data_dir / "node_catalog.json")


def wf(nodes, entry, trigger=None):
    return {"kind": "workflow", "explanation": "ok", "workflow": {"name": "Order check", "trigger": trigger or {"type": "webhook", "secret": "GENERATED"},
            "entry": entry, "limits": {"max_steps": 20, "timeout_seconds": 300, "max_ai_tokens": 0}, "nodes": nodes}}


DEMO = wf([
    {"id": "check", "type": "condition", "params": {"left": "{{trigger.body.total}}", "op": "greater_than", "right": "100"}, "on_true": "mgr", "on_false": "confirm"},
    {"id": "mgr", "type": "approval", "params": {"message": "Approve?"}, "next": "confirm"},
    {"id": "confirm", "type": "notify", "params": {"channel": "email", "to": "{{trigger.body.email}}", "message": "Confirmed"}, "next": None}], "check")


def test_valid_draft_gets_id_and_fresh_secret():
    r = NLCompiler(FakeProvider([DEMO]), CAT, S).compile("x")
    assert r.kind == "workflow" and r.workflow["id"].startswith("wf_nl_") and r.workflow["trigger"]["secret"].startswith("whsec_")
    assert validate_definition(r.workflow, CAT).ok


def test_invented_node_type_is_repaired_with_validator_feedback():
    fake = FakeProvider([wf([{"id": "sf", "type": "salesforce_update", "params": {}, "next": None}], "sf"), DEMO])
    r = NLCompiler(fake, CAT, S).compile("x")
    assert r.kind == "workflow" and r.attempts == 2 and "invalid_node_type" in fake.calls[1]["messages"][1]["content"]


def test_refusal_and_give_up():
    ref = {"kind": "refusal", "explanation": "no", "refusal": {"reason": "No database connector in the catalog", "missing_capabilities": ["postgres"]}}
    assert NLCompiler(FakeProvider([ref]), CAT, S).compile("x").kind == "refusal"
    bad = wf([{"id": "x", "type": "teleport", "params": {}, "next": None}], "x")
    r = NLCompiler(FakeProvider([bad, bad, bad]), CAT, S).compile("x")
    assert r.kind == "failed" and r.issues[0]["code"] == "invalid_node_type"


def test_unguarded_order_action_gets_an_approval_gate():
    unsafe = wf([{"id": "refund", "type": "order_action", "params": {"action": "refund", "order_id": "{{trigger.body.order_id}}"}, "next": None}], "refund")
    r = NLCompiler(FakeProvider([unsafe]), CAT, S).compile("refund with no human")
    assert r.workflow["entry"] == "approve_before_refund" and unguarded_sensitive_nodes(r.workflow, CAT) == []
    assert "approval" in r.explanation.lower()
