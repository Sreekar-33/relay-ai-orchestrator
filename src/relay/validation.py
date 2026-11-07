"""Node catalog, publish-time validation, and graph analysis (approval gating, reachability)."""
from __future__ import annotations

import json
import re
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from relay import templates

ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")
CRON_FIELD_RE = re.compile(r"^[\d\*/,\-]+$")
TAINT_PARAMS = {"to", "url", "order_id", "channel", "method"}
_TYPES = {
    "string": lambda v: isinstance(v, str),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "object": lambda v: isinstance(v, dict),
}


# ------------------------------------------------------------------ catalog
@dataclass(frozen=True)
class ParamSpec:
    name: str
    type: str
    required: bool
    enum: tuple | None
    templatable: bool


@dataclass(frozen=True)
class NodeSpec:
    type: str
    side_effect: bool
    requires_approval: bool
    params: dict
    branches: tuple = ()


@dataclass(frozen=True)
class Catalog:
    nodes: dict
    triggers: dict
    raw: dict = field(repr=False, default_factory=dict)


def load_catalog(path: Path) -> Catalog:
    raw = json.loads(Path(path).read_text())
    nodes = {}
    for n in raw["nodes"]:
        params = {k: ParamSpec(k, p.get("type", "string"), bool(p.get("required")), tuple(p["enum"]) if "enum" in p else None,
                               bool(p.get("templatable"))) for k, p in n.get("params", {}).items()}
        nodes[n["type"]] = NodeSpec(n["type"], bool(n.get("side_effect")), bool(n.get("requires_approval")), params,
                                    tuple(n.get("branches", ())))
    return Catalog(nodes, {t["type"]: t for t in raw["triggers"]}, raw)


def is_side_effect(spec: NodeSpec, params: dict) -> bool:
    if not spec.side_effect:
        return False
    if spec.type == "http_request":
        return str(params.get("method", "")).upper() != "GET"
    return True


# ------------------------------------------------------------------ graph
def node_index(defn: dict) -> dict:
    return {n["id"]: n for n in defn.get("nodes", []) if isinstance(n, dict) and "id" in n}


def successors(node: dict) -> list:
    targets = [node.get("on_true"), node.get("on_false")] if node.get("type") == "condition" else [node.get("next")]
    return [t for t in targets if t]


def reachable(defn: dict) -> set:
    idx, seen, q = node_index(defn), set(), deque([defn.get("entry")])
    while q:
        nid = q.popleft()
        if nid in idx and nid not in seen:
            seen.add(nid)
            q.extend(successors(idx[nid]))
    return seen


def unguarded_sensitive_nodes(defn: dict, catalog: Catalog) -> list:
    """requires_approval nodes reachable on some path that has not passed an approval node (search over (node, approved))."""
    idx, out, seen, q = node_index(defn), [], set(), deque([(defn.get("entry"), False)])
    while q:
        nid, approved = q.popleft()
        if nid not in idx or (nid, approved) in seen:
            continue
        seen.add((nid, approved))
        node = idx[nid]
        spec = catalog.nodes.get(node.get("type"))
        if spec and spec.requires_approval and not approved and nid not in out:
            out.append(nid)
        for nxt in successors(node):
            q.append((nxt, approved or node.get("type") == "approval"))
    return out


# ------------------------------------------------------------------ validation
@dataclass
class Issue:
    code: str
    message: str
    node_id: str | None = None
    field: str | None = None

    def to_dict(self):
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class ValidationResult:
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    @property
    def ok(self):
        return not self.errors

    def to_dict(self):
        return {"ok": self.ok, "errors": [e.to_dict() for e in self.errors], "warnings": [w.to_dict() for w in self.warnings]}


def check_shape(defn) -> list:
    if not isinstance(defn, dict):
        return [Issue("invalid_definition", "Workflow definition must be a JSON object")]
    issues = []
    if "id" in defn and (not isinstance(defn["id"], str) or not ID_RE.match(defn["id"])):
        issues.append(Issue("invalid_definition", "Workflow 'id' must match ^[A-Za-z0-9_-]{1,64}$", field="id"))
    for key, typ in (("name", str), ("trigger", dict), ("entry", str), ("nodes", list), ("limits", dict)):
        if not isinstance(defn.get(key), typ):
            issues.append(Issue("invalid_definition", f"Workflow field '{key}' is missing or not a {typ.__name__}", field=key))
    for i, node in enumerate(defn.get("nodes") or []):
        if not isinstance(node, dict) or not isinstance(node.get("id"), str) or not isinstance(node.get("type"), str):
            issues.append(Issue("invalid_definition", f"nodes[{i}] must be an object with string 'id' and 'type'", field=f"nodes[{i}]"))
        elif not ID_RE.match(node["id"]):
            issues.append(Issue("invalid_definition", f"Node id '{node['id']}' is invalid", node_id=node["id"]))
        elif "params" in node and not isinstance(node["params"], dict):
            issues.append(Issue("invalid_definition", f"Node '{node['id']}' params must be an object", node_id=node["id"]))
    return issues


def validate_definition(defn, catalog: Catalog) -> ValidationResult:
    errors = check_shape(defn)
    if errors:
        return ValidationResult(errors)
    warnings = []
    trig = defn["trigger"]
    ttype = trig.get("type")
    if ttype not in catalog.triggers:
        errors.append(Issue("unknown_trigger_type", f"Trigger type '{ttype}' is not in the catalog", field="trigger.type"))
    elif ttype == "webhook" and not str(trig.get("secret") or "").strip():
        errors.append(Issue("missing_webhook_secret", "Webhook trigger requires a non-empty 'secret'", field="trigger.secret"))
    elif ttype == "schedule":
        cron = trig.get("cron")
        if not (isinstance(cron, str) and len(cron.split()) == 5 and all(CRON_FIELD_RE.match(f) for f in cron.split())):
            errors.append(Issue("invalid_cron", f"Schedule trigger needs a 5-field cron expression, got {cron!r}", field="trigger.cron"))
    limits = defn["limits"]
    ms = limits.get("max_steps")
    if not isinstance(ms, int) or isinstance(ms, bool) or not 1 <= ms <= 1000:
        errors.append(Issue("invalid_limits", f"limits.max_steps must be an integer between 1 and 1000, got {ms!r}", field="limits.max_steps"))
    for k in ("timeout_seconds", "max_ai_tokens"):
        v = limits.get(k)
        if k in limits and (isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0):
            errors.append(Issue("invalid_limits", f"limits.{k} must be a non-negative number, got {v!r}", field=f"limits.{k}"))

    nodes = defn["nodes"]
    ids = set()
    for n in nodes:
        if n["id"] in ids:
            errors.append(Issue("duplicate_node_id", f"Node id '{n['id']}' is used more than once", node_id=n["id"]))
        ids.add(n["id"])
    ai_nodes = {n["id"] for n in nodes if n["type"] == "ai"}

    for node in nodes:
        nid, ntype = node["id"], node["type"]
        spec = catalog.nodes.get(ntype)
        if spec is None:
            errors.append(Issue("invalid_node_type", f"Node '{nid}' has unknown type '{ntype}'", nid, "type"))
            continue
        params = node.get("params") or {}
        for pname, ps in spec.params.items():
            v = params.get(pname)
            if ps.required and (v is None or v == ""):
                errors.append(Issue("missing_required_param", f"Node '{nid}' ({ntype}) is missing required param '{pname}'", nid, f"params.{pname}"))
            elif pname in params and not (isinstance(v, str) and ps.templatable and "{{" in v):
                check = _TYPES.get(ps.type)
                if check and not check(v):
                    errors.append(Issue("invalid_param_value", f"Node '{nid}' ({ntype}) param '{pname}' must be a {ps.type}, got {v!r}", nid, f"params.{pname}"))
                elif ps.enum is not None and v not in ps.enum:
                    errors.append(Issue("invalid_param_value", f"Node '{nid}' ({ntype}) param '{pname}' must be one of {list(ps.enum)}, got {v!r}", nid, f"params.{pname}"))
        for pname in params:
            if pname not in spec.params:
                warnings.append(Issue("unknown_param", f"Node '{nid}' ({ntype}) has param '{pname}' not in the catalog (ignored)", nid, f"params.{pname}"))
        if spec.branches:
            for b in spec.branches:
                if b not in node:
                    errors.append(Issue("invalid_branching", f"Condition node '{nid}' must define '{b}' (use null to end the run)", nid, b))
                elif node[b] is not None and node[b] not in ids:
                    errors.append(Issue("invalid_reference", f"Node '{nid}' {b} -> '{node[b]}' does not exist", nid, b))
            if node.get("next") is not None:
                errors.append(Issue("invalid_branching", f"Condition node '{nid}' routes with on_true/on_false, not 'next'", nid, "next"))
        else:
            if "on_true" in node or "on_false" in node:
                errors.append(Issue("invalid_branching", f"Node '{nid}' ({ntype}) cannot have on_true/on_false; only condition nodes branch", nid))
            nxt = node.get("next")
            if nxt is not None and nxt not in ids:
                errors.append(Issue("invalid_reference", f"Node '{nid}' next -> '{nxt}' does not exist", nid, "next"))
        if ntype == "ai" and isinstance(params.get("output_schema"), dict):
            try:
                Draft202012Validator.check_schema(params["output_schema"])
                if params["output_schema"].get("type") != "object":
                    raise SchemaError("output_schema must describe a JSON object (type: object)")
            except SchemaError as exc:
                errors.append(Issue("invalid_output_schema", f"Node '{nid}' output_schema is not a valid JSON Schema: {exc.message}", nid, "params.output_schema"))
        for pname, ps in spec.params.items():
            if not ps.templatable or pname not in params:
                continue
            for expr in templates.references(params[pname]):
                try:
                    ref = templates.parse_ref(expr)
                except templates.TemplateError as exc:
                    errors.append(Issue("invalid_template", f"Node '{nid}' param '{pname}': {exc}", nid, f"params.{pname}"))
                    continue
                if ref.root == "nodes" and ref.node_id not in ids:
                    errors.append(Issue("invalid_template_reference", f"Node '{nid}' param '{pname}' references unknown node '{ref.node_id}'", nid, f"params.{pname}"))
                elif ref.root == "nodes" and ref.node_id in ai_nodes and pname in TAINT_PARAMS and spec.side_effect:
                    warnings.append(Issue("ai_controls_side_effect_target", f"Node '{nid}' param '{pname}' is controlled by AI node '{ref.node_id}'; side-effect targets should be fixed in the definition", nid, f"params.{pname}"))
    if defn["entry"] not in ids:
        errors.append(Issue("invalid_entry", f"Entry node '{defn['entry']}' does not exist", field="entry"))
    if errors:
        return ValidationResult(errors, warnings)
    reach = reachable(defn)
    for n in nodes:
        if n["id"] not in reach:
            warnings.append(Issue("unreachable_node", f"Node '{n['id']}' is not reachable from entry '{defn['entry']}'", n["id"]))
    for nid in unguarded_sensitive_nodes(defn, catalog):
        warnings.append(Issue("unguarded_sensitive_node", f"Node '{nid}' requires approval but is reachable without an approval node before it; the engine will refuse to execute it at runtime", nid))
    return ValidationResult(errors, warnings)
