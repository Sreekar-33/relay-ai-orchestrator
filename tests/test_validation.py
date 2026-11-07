import json

import pytest

from relay.config import Settings
from relay.validation import load_catalog, unguarded_sensitive_nodes, validate_definition

DATA = Settings().data_dir
CAT = load_catalog(DATA / "node_catalog.json")
SEEDS = {w["id"]: w for w in json.loads((DATA / "seed_workflows.json").read_text())["workflows"]}


def wf(nodes, entry="n1", trigger=None):
    return {"id": "wf_t", "name": "t", "trigger": trigger or {"type": "manual"}, "entry": entry,
            "limits": {"max_steps": 5, "timeout_seconds": 60, "max_ai_tokens": 0}, "nodes": nodes}


def codes(r):
    return [e.code for e in r.errors]


@pytest.mark.parametrize("wid", sorted(SEEDS))
def test_seed_workflows_are_valid(wid):
    assert validate_definition(SEEDS[wid], CAT).ok


def test_unknown_node_type():
    r = validate_definition(wf([{"id": "n1", "type": "teleport", "params": {}, "next": None}]), CAT)
    assert codes(r) == ["invalid_node_type"] and r.errors[0].message == "Node 'n1' has unknown type 'teleport'"


def test_missing_required_params():
    r = validate_definition(wf([{"id": "n1", "type": "notify", "params": {"channel": "chat"}, "next": None}]), CAT)
    assert codes(r) == ["missing_required_param"] * 2


def test_reference_and_entry_to_nonexistent_node():
    assert codes(validate_definition(wf([{"id": "n1", "type": "delay", "params": {"seconds": 1}, "next": "ghost"}]), CAT)) == ["invalid_reference"]
    assert codes(validate_definition(wf([{"id": "n1", "type": "delay", "params": {"seconds": 1}, "next": None}], entry="x"), CAT)) == ["invalid_entry"]


def test_backward_jumps_are_legal():
    assert validate_definition(SEEDS["wf_runaway"], CAT).ok


def test_enum_type_branching_secret_and_template_errors():
    r = validate_definition(wf([
        {"id": "n1", "type": "condition", "params": {"left": "1", "op": "approx", "right": "2"}, "on_true": "n2"},
        {"id": "n2", "type": "notify", "params": {"channel": "chat", "to": "#x", "message": "{{nodes.ghost.output.a}}"}, "next": None},
    ]), CAT)
    assert set(codes(r)) == {"invalid_param_value", "invalid_branching", "invalid_template_reference"}
    n = [{"id": "n1", "type": "delay", "params": {"seconds": 1}, "next": None}]
    assert codes(validate_definition(wf(n, trigger={"type": "webhook"}), CAT)) == ["missing_webhook_secret"]


def test_unguarded_order_action_is_flagged_but_approval_on_other_branch_does_not_guard():
    d = {"entry": "a", "nodes": [
        {"id": "a", "type": "condition", "on_true": "gate", "on_false": "refund"},
        {"id": "gate", "type": "approval", "next": "refund"},
        {"id": "refund", "type": "order_action", "next": None}]}
    assert unguarded_sensitive_nodes(d, CAT) == ["refund"]
    assert unguarded_sensitive_nodes(SEEDS["wf_support_triage"], CAT) == []
