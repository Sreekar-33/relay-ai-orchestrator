"""{{trigger.body.x}} / {{nodes.<id>.output.y}} / {{run.id}} resolution. Path lookups only; unresolvable paths fail the step."""
from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

EXPR_RE = re.compile(r"\{\{\s*(.*?)\s*\}\}")
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


class TemplateError(Exception):
    """A template could not be parsed or resolved. Always fails the step (never retried)."""


@dataclass(frozen=True)
class ParsedRef:
    root: str
    node_id: str | None
    path: tuple


def parse_ref(expr: str) -> ParsedRef:
    parts = expr.strip().split(".")
    if not expr.strip() or any(not _SEGMENT_RE.match(p) for p in parts):
        raise TemplateError(f"Invalid template expression '{{{{{expr}}}}}'")
    if parts[:2] == ["trigger", "body"]:
        return ParsedRef("trigger", None, tuple(parts[2:]))
    if parts[0] == "nodes" and len(parts) >= 3 and parts[2] == "output":
        return ParsedRef("nodes", parts[1], tuple(parts[3:]))
    if parts == ["run", "id"]:
        return ParsedRef("run", None, ())
    raise TemplateError(f"Unknown template root in '{{{{{expr}}}}}': use trigger.body.*, nodes.<id>.output.* or run.id")


def references(value: Any) -> list[str]:
    if isinstance(value, str):
        return [m.group(1) for m in EXPR_RE.finditer(value)]
    if isinstance(value, dict):
        return [r for v in value.values() for r in references(v)]
    if isinstance(value, list):
        return [r for v in value for r in references(v)]
    return []


@dataclass(frozen=True)
class TemplateContext:
    trigger_body: Any
    node_outputs: Mapping[str, Any]
    run_id: str = ""


def _lookup(expr: str, ctx: TemplateContext) -> Any:
    ref = parse_ref(expr)
    if ref.root == "run":
        return ctx.run_id
    if ref.root == "trigger":
        cur, where = ctx.trigger_body, "trigger.body"
    else:
        if ref.node_id not in ctx.node_outputs:
            raise TemplateError(f"Template '{{{{{expr}}}}}' could not be resolved: node '{ref.node_id}' has not produced output in this run")
        cur, where = ctx.node_outputs[ref.node_id], f"nodes.{ref.node_id}.output"
    for seg in ref.path:
        if isinstance(cur, dict) and seg in cur:
            cur = cur[seg]
        elif isinstance(cur, list) and seg.isdigit() and int(seg) < len(cur):
            cur = cur[int(seg)]
        else:
            raise TemplateError(f"Template '{{{{{expr}}}}}' could not be resolved: '{seg}' not found under {where}")
        where = f"{where}.{seg}"
    return cur


def stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list, bool)) or value is None:
        return json.dumps(value, separators=(",", ":"))
    return str(value)


def resolve(value: Any, ctx: TemplateContext) -> Any:
    if isinstance(value, str):
        matches = list(EXPR_RE.finditer(value))
        # Native value only when ONE expression spans the whole string.
        if len(matches) == 1 and matches[0].span() == (0, len(value)):
            return copy.deepcopy(_lookup(matches[0].group(1), ctx))
        return EXPR_RE.sub(lambda m: stringify(_lookup(m.group(1), ctx)), value)
    if isinstance(value, dict):
        return {k: resolve(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, ctx) for v in value]
    return copy.deepcopy(value)


def resolve_params(params: dict, spec, ctx: TemplateContext) -> dict:
    out = {}
    for name, value in (params or {}).items():
        p = spec.params.get(name)
        out[name] = resolve(value, ctx) if p is not None and p.templatable else copy.deepcopy(value)
    return out
