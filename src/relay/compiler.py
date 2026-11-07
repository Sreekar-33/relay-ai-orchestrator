"""Natural-language workflow compiler: plain English -> a valid DRAFT workflow using only catalog node types, or a refusal.

Pipeline: catalog-grounded system prompt -> schema-enforced JSON (ai.enforce) -> deterministic post-processing
(ids, generated webhook secret, limits, approval gate inserted before any requires_approval node) -> the publish
validator is the referee -> up to 2 repair rounds fed with the validator's errors -> draft, refusal, or 422.
"""
from __future__ import annotations

import copy
import json
import re
import secrets
from dataclasses import dataclass, field

from relay.ai import TokenUsage, enforce
from relay.errors import RelayError
from relay.validation import unguarded_sensitive_nodes, validate_definition

COMPILER_SCHEMA = {
    "type": "object",
    "properties": {
        "kind": {"type": "string", "enum": ["workflow", "refusal"]},
        "explanation": {"type": "string"},
        "workflow": {"type": ["object", "null"]},
        "refusal": {"anyOf": [{"type": "null"}, {"type": "object", "properties": {
            "reason": {"type": "string", "minLength": 10},
            "missing_capabilities": {"type": "array", "items": {"type": "string"}, "minItems": 1}},
            "required": ["reason", "missing_capabilities"]}]},
    },
    "required": ["kind", "explanation"],
    "allOf": [
        {"if": {"properties": {"kind": {"const": "workflow"}}}, "then": {"required": ["workflow"], "properties": {"workflow": {"type": "object"}}}},
        {"if": {"properties": {"kind": {"const": "refusal"}}}, "then": {"required": ["refusal"], "properties": {"refusal": {"type": "object"}}}},
    ],
}


def _catalog_summary(catalog) -> str:
    lines = []
    for spec in catalog.nodes.values():
        params = ", ".join(f"{p.name}:{p.type}{'*' if p.required else ''}{'=' + '|'.join(p.enum) if p.enum else ''}"
                           for p in spec.params.values())
        flags = [f for f, on in (("side_effect", spec.side_effect), ("requires_approval", spec.requires_approval),
                                 ("branches on_true/on_false", bool(spec.branches))) if on]
        desc = next((n.get("description", "") for n in catalog.raw["nodes"] if n["type"] == spec.type), "")
        lines.append(f"- {spec.type}({params}) [{', '.join(flags) or 'pure'}]: {desc}")
    trig = "; ".join(f"{t}: {d['description']}" for t, d in catalog.triggers.items())
    return "NODE TYPES (* = required):\n" + "\n".join(lines) + f"\nTRIGGERS: {trig}"


def build_system_prompt(catalog, example: dict) -> str:
    return f"""You compile plain-English automation requests into Relay workflow definitions, or refuse.

{_catalog_summary(catalog)}

DEFINITION SHAPE (example):
{json.dumps(example, indent=1)}

RULES
1. Use ONLY the node types and triggers above. Never invent a type, trigger, or param.
2. Refuse (kind="refusal") when the request depends on any capability that is not one of the node types or triggers above
   (for example a datastore, a third-party SaaS connector, a messaging channel other than email/chat, a non-HTTP event
   source, or a publishing target). List each missing capability in missing_capabilities. Never approximate a missing
   capability with http_request; use http_request only for calling an HTTP API (placeholder URL like
   https://api.example.com/... when none is given).
3. Node types flagged requires_approval are refused by the engine unless an approval node precedes them on every path.
   Always place an approval node before them, even if the request says otherwise, and say so in the explanation.
4. Triggers: webhook -> {{"type":"webhook","secret":"GENERATED"}}; schedule -> {{"type":"schedule","cron":"<5-field cron, UTC>"}};
   manual -> {{"type":"manual"}}.
5. Data flow uses templates: {{{{trigger.body.<field>}}}} for the incoming payload (invent clear snake_case field names when the
   request does not name them) and {{{{nodes.<node_id>.output.<field>}}}} for earlier node outputs.
6. condition: params left/op/right with op in equals|not_equals|greater_than|less_than|contains; on_true/on_false are node
   ids or null (null ends that path). Only condition nodes branch; every other node uses "next" (null ends the run).
7. Order steps the way the request describes them: actions that always happen come before a condition guarding optional ones.
8. ai nodes need params.prompt and params.output_schema (a JSON Schema object). Use enum string fields for anything a
   condition branches on, and reference them as {{{{nodes.<ai_id>.output.<field>}}}}.
9. Repetition: a backward "next" jump through a delay node is allowed (the engine's step cap bounds it), as is a schedule trigger.
10. notify: channel email needs "to" (address or template); channel chat needs "to" like "#ops".
11. limits: {{"max_steps": 25, "timeout_seconds": 600, "max_ai_tokens": 4000 if there are ai nodes else 0}}.
12. Node ids: short snake_case. Keep the workflow minimal.
13. Capabilities you DO have: the ai node can classify, judge (e.g. sentiment, urgency, category), summarize, extract or write
    text from any input; never refuse a request because it needs that kind of judgement. order_action performs order refunds
    and replacements on the mock commerce system. notify sends email and chat messages.

OUTPUT: one JSON object matching this JSON Schema (no prose, no code fences):
{{schema}}
kind="workflow" -> include "workflow" and a one-paragraph "explanation".
kind="refusal"  -> include "refusal": {{"reason", "missing_capabilities"}} and an "explanation"."""


@dataclass
class CompileResult:
    kind: str
    workflow: dict | None = None
    explanation: str = ""
    refusal: dict | None = None
    warnings: list = field(default_factory=list)
    issues: list = field(default_factory=list)
    notes: list = field(default_factory=list)
    attempts: int = 0
    tokens: TokenUsage = field(default_factory=TokenUsage)


class NLCompiler:
    MAX_ROUNDS = 3

    def __init__(self, provider, catalog, settings):
        self.provider, self.catalog, self.settings = provider, catalog, settings
        seeds = json.loads((settings.data_dir / "seed_workflows.json").read_text())["workflows"]
        example = copy.deepcopy(next(w for w in seeds if w["id"] == "wf_slow_fulfillment"))  # neutral example: no eval answers
        example.pop("description", None)
        self.system = build_system_prompt(catalog, example)

    def postprocess(self, wf) -> tuple:
        wf = copy.deepcopy(wf) if isinstance(wf, dict) else {}
        notes = []
        slug = "_".join(re.findall(r"[a-z0-9]+", str(wf.get("name") or "workflow").lower())[:3]) or "workflow"
        wf["id"] = f"wf_nl_{slug}_{secrets.token_hex(3)}"
        wf["name"] = str(wf.get("name") or "Compiled workflow")
        trig = wf.get("trigger") if isinstance(wf.get("trigger"), dict) else {"type": "manual"}
        if trig.get("type") == "webhook":
            trig["secret"] = f"whsec_{secrets.token_hex(8)}"
            notes.append("Generated a new webhook secret (returned once).")
        wf["trigger"] = trig
        nodes = [n for n in (wf.get("nodes") or []) if isinstance(n, dict) and isinstance(n.get("id"), str)]
        for n in nodes:
            if not isinstance(n.get("params"), dict):
                n["params"] = {}
            if n.get("type") == "condition":
                n.setdefault("on_true", None); n.setdefault("on_false", None); n.pop("next", None)
            else:
                n.setdefault("next", None)
        wf["nodes"] = nodes
        limits = wf.get("limits") if isinstance(wf.get("limits"), dict) else {}
        limits.setdefault("max_steps", 25)
        delay = sum(float(n["params"].get("seconds") or 0) for n in nodes if n.get("type") == "delay" and isinstance(n["params"].get("seconds"), (int, float)))
        try:
            given = int(float(limits.get("timeout_seconds") or 600))
        except (TypeError, ValueError):
            given = 600
        limits["timeout_seconds"] = max(given, int(delay) + 600)
        if any(n.get("type") == "ai" for n in nodes) and not limits.get("max_ai_tokens"):
            limits["max_ai_tokens"] = 4000
        limits.setdefault("max_ai_tokens", 0)
        wf["limits"] = limits
        if wf.get("entry") in {n["id"] for n in nodes}:
            for target in unguarded_sensitive_nodes(wf, self.catalog):  # engine rule applied by construction
                gate_id = f"approve_before_{target}"
                t = next(n for n in nodes if n["id"] == target)
                gate = {"id": gate_id, "type": "approval", "next": target,
                        "params": {"message": f"Approve {t['params'].get('action', 'this action')} for order {t['params'].get('order_id', '')}?"}}
                for n in nodes:
                    for k in ("next", "on_true", "on_false"):
                        if n.get(k) == target:
                            n[k] = gate_id
                if wf["entry"] == target:
                    wf["entry"] = gate_id
                nodes.append(gate)
                notes.append(f"Inserted approval node '{gate_id}' before '{target}': it has requires_approval, so the engine would refuse it without one.")
        return wf, notes

    def compile(self, description: str) -> CompileResult:
        if getattr(self.provider, "name", "") == "heuristic":
            raise RelayError("compiler_requires_model", "The NL compiler needs a real model: set RELAY_AI_PROVIDER=openrouter and OPENROUTER_API_KEY", 503)
        description = (description or "").strip()
        if not description:
            raise RelayError("invalid_request", "'description' must be a non-empty string", 400)
        if len(description) > 4000:
            raise RelayError("invalid_request", "'description' is too long (max 4000 characters)", 400)
        tokens, feedback, issues = TokenUsage(), "", []
        for rnd in range(1, self.MAX_ROUNDS + 1):
            prompt = f"Request:\n{description}"
            if feedback:
                prompt += f"\n\nYour previous draft was rejected by the validator:\n{feedback}\nReturn a corrected draft (or a refusal)."
            res = enforce(self.provider, prompt, COMPILER_SCHEMA, timeout_s=max(self.settings.ai_timeout_s, 60), system=self.system)
            tokens = TokenUsage(tokens.prompt + res.tokens.prompt, tokens.completion + res.tokens.completion)
            if res.output is None:
                issues = [{"code": "compiler_output_invalid", "message": e} for e in res.errors]
                feedback = "\n".join(f"- {e}" for e in res.errors)
                continue
            out = res.output
            if out["kind"] == "refusal":
                return CompileResult("refusal", refusal=out["refusal"], explanation=out.get("explanation", ""), attempts=rnd, tokens=tokens)
            try:
                wf, notes = self.postprocess(out.get("workflow") or {})
            except (KeyError, TypeError, ValueError, AttributeError, StopIteration) as exc:
                issues = [{"code": "malformed_draft", "message": str(exc)}]
                feedback = f"- malformed draft: {exc}"
                continue
            v = validate_definition(wf, self.catalog)
            if v.ok:
                expl = out.get("explanation", "")
                if any("Inserted approval" in n for n in notes) and "approval" not in expl.lower():
                    expl += " An approval step was added before the order action because the engine requires it."
                return CompileResult("workflow", workflow=wf, explanation=expl, warnings=[w.to_dict() for w in v.warnings],
                                     notes=notes, attempts=rnd, tokens=tokens)
            issues = [e.to_dict() for e in v.errors]
            feedback = "\n".join(f"- {e['code']}: {e['message']}" for e in issues)
        return CompileResult("failed", issues=issues, attempts=self.MAX_ROUNDS, tokens=tokens)
