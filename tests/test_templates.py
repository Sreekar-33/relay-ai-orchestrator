import pytest

from relay.templates import TemplateContext, TemplateError, resolve

CTX = TemplateContext({"order_id": "ord_2001", "amount_usd": 250, "items": [{"sku": "A1"}], "flag": True},
                      {"classify": {"category": "complaint"}}, "run_123")


def test_single_expression_keeps_native_type():
    assert resolve("{{trigger.body.amount_usd}}", CTX) == 250
    assert resolve("{{ trigger.body.flag }}", CTX) is True


def test_embedded_and_multiple_expressions():
    assert resolve("Expense of ${{trigger.body.amount_usd}}", CTX) == "Expense of $250"
    assert resolve("{{trigger.body.order_id}}: {{run.id}}", CTX) == "ord_2001: run_123"   # not merged into one bogus expr
    assert resolve("{{nodes.classify.output.category}} / {{trigger.body.items.0.sku}}", CTX) == "complaint / A1"


def test_nested_structures():
    assert resolve({"order_id": "{{trigger.body.order_id}}", "x": ["{{run.id}}", 3]}, CTX) == {"order_id": "ord_2001", "x": ["run_123", 3]}


def test_unresolvable_paths_fail_clearly_never_render_none():
    with pytest.raises(TemplateError, match="'customer_email' not found under trigger.body"):
        resolve("Hi {{trigger.body.customer_email}}", CTX)
    with pytest.raises(TemplateError, match="node 'issue_refund' has not produced output in this run"):
        resolve("{{nodes.issue_refund.output.reference_id}}", CTX)
    with pytest.raises(TemplateError, match="Unknown template root"):
        resolve("{{env.SECRET}}", CTX)
