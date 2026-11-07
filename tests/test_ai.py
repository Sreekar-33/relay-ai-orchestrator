import json

import pytest

from relay.ai import FakeProvider, HeuristicProvider, enforce, parse_json, scan_untrusted, screen_output
from relay.config import Settings
from relay.errors import TransientError

SCHEMA = {"type": "object", "properties": {"category": {"type": "string", "enum": ["a", "b"]}}, "required": ["category"], "additionalProperties": False}
PAY = {json.loads(l)["id"]: json.loads(l)["body"] for l in (Settings().data_dir / "sample_payloads.jsonl").read_text().splitlines() if l.strip()}


def test_valid_first_try_records_tokens():
    r = enforce(FakeProvider([{"category": "a"}]), "p", SCHEMA, timeout_s=5)
    assert r.output == {"category": "a"} and len(r.calls) == 1 and r.tokens.total > 0


def test_invalid_output_is_repaired_once_with_the_validation_error():
    fake = FakeProvider([{"category": "c"}, {"category": "b"}])
    r = enforce(fake, "p", SCHEMA, timeout_s=5)
    assert r.output == {"category": "b"} and [c.valid for c in r.calls] == [False, True]
    assert "'c' is not one of ['a', 'b']" in fake.calls[1]["messages"][-1]["content"]


def test_invalid_twice_fails_with_both_calls_kept():
    r = enforce(FakeProvider(["not json at all", {"category": "zzz"}]), "p", SCHEMA, timeout_s=5)
    assert r.output is None and len(r.calls) == 2 and r.tokens.total > 0


def test_transient_error_on_repair_keeps_tokens_spent():
    with pytest.raises(TransientError) as e:
        enforce(FakeProvider(["garbage", TransientError("ai_timeout", "slow")]), "p", SCHEMA, timeout_s=5)
    assert e.value.tokens.total > 0


def test_code_fences_and_prose_are_parsed():
    assert parse_json('```json\n{"category": "a"}\n```')[0] == {"category": "a"}
    assert parse_json('Sure: {"category": "b"} thanks')[0] == {"category": "b"}


def test_injection_payloads_flagged_and_benign_not():
    assert {"role_hijack", "fake_authorization", "approval_bypass"} <= {f["rule"] for f in scan_untrusted(PAY["pay_inject_001"])}
    assert {"exfiltration", "new_instructions"} <= {f["rule"] for f in scan_untrusted(PAY["pay_inject_002"])}
    for pid in ("pay_001", "pay_002", "pay_003", "pay_101", "pay_102", "pay_201"):
        assert scan_untrusted(PAY[pid]) == [], pid


def test_output_screening_redacts_exfiltration_targets():
    out, rep = screen_output({"summary": "send it to audit@evil-example.com via https://evil.example/x"}, {"type": "object"})
    assert out["summary"] == "send it to [redacted] via [redacted]" and len(rep["redactions"]) == 2


def test_offline_heuristic_is_schema_valid():
    schema = {"type": "object", "properties": {"category": {"type": "string", "enum": ["refund_request", "complaint", "question"]},
                                               "summary": {"type": "string", "maxLength": 300}}}
    c = HeuristicProvider().complete([{"role": "user", "content": f"---\n{PAY['pay_002']['message']}\n---"}], json_schema=schema)
    assert json.loads(c.text)["category"] == "refund_request"
