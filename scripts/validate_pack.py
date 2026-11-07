#!/usr/bin/env python3
"""Validates that the Relay capstone pack data is parseable and internally consistent."""
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

TEMPLATE_RE = re.compile(r"\{\{\s*(trigger\.body(?:\.[A-Za-z0-9_]+)*|nodes\.([A-Za-z0-9_]+)\.output(?:\.[A-Za-z0-9_]+)*)\s*\}\}")


def read_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_jsonl(path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}:{line_number}: invalid JSONL: {exc}") from exc
    return rows


def iter_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from iter_strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from iter_strings(v)


def validate_workflow(workflow, catalog_nodes, trigger_types):
    wid = workflow.get("id", "<missing id>")
    for field in ("id", "name", "trigger", "entry", "nodes", "limits"):
        if field not in workflow:
            raise ValueError(f"Workflow {wid} missing '{field}'")
    trigger = workflow["trigger"]
    if trigger["type"] not in trigger_types:
        raise ValueError(f"Workflow {wid} has unknown trigger type '{trigger['type']}'")
    if trigger["type"] == "webhook" and not trigger.get("secret"):
        raise ValueError(f"Workflow {wid} webhook trigger missing secret")
    for field in ("max_steps", "timeout_seconds", "max_ai_tokens"):
        if not isinstance(workflow["limits"].get(field), (int, float)):
            raise ValueError(f"Workflow {wid} limits missing numeric '{field}'")

    node_ids = set()
    for node in workflow["nodes"]:
        if node["id"] in node_ids:
            raise ValueError(f"Workflow {wid} has duplicate node id '{node['id']}'")
        node_ids.add(node["id"])

    if workflow["entry"] not in node_ids:
        raise ValueError(f"Workflow {wid} entry '{workflow['entry']}' is not a node")

    for node in workflow["nodes"]:
        nid = node["id"]
        spec = catalog_nodes.get(node["type"])
        if spec is None:
            raise ValueError(f"Workflow {wid} node '{nid}' has unknown type '{node['type']}'")
        for pname, pspec in spec["params"].items():
            if isinstance(pspec, dict) and pspec.get("required") and pname not in node.get("params", {}):
                raise ValueError(f"Workflow {wid} node '{nid}' ({node['type']}) missing required param '{pname}'")
        edges = ["on_true", "on_false"] if node["type"] == "condition" else ["next"]
        for edge in edges:
            if edge not in node:
                raise ValueError(f"Workflow {wid} node '{nid}' missing edge '{edge}'")
            target = node[edge]
            if target is not None and target not in node_ids:
                raise ValueError(f"Workflow {wid} node '{nid}' edge '{edge}' points to unknown node '{target}'")
        for text in iter_strings(node.get("params", {})):
            for match in TEMPLATE_RE.finditer(text):
                ref_node = match.group(2)
                if ref_node and ref_node not in node_ids:
                    raise ValueError(f"Workflow {wid} node '{nid}' references unknown node '{ref_node}' in template")

    # every order_action must have an approval node somewhere in the workflow
    types_used = {n["type"] for n in workflow["nodes"]}
    for node in workflow["nodes"]:
        if catalog_nodes[node["type"]].get("requires_approval") and "approval" not in types_used:
            raise ValueError(f"Workflow {wid} uses sensitive node '{node['id']}' with no approval node in the workflow")


def main():
    catalog = read_json(DATA_DIR / "node_catalog.json")
    seeds = read_json(DATA_DIR / "seed_workflows.json")["workflows"]
    payloads = read_jsonl(DATA_DIR / "sample_payloads.jsonl")
    nl_cases = read_jsonl(DATA_DIR / "nl_eval.jsonl")

    catalog_nodes = {n["type"]: n for n in catalog["nodes"]}
    trigger_types = {t["type"] for t in catalog["triggers"]}
    if not {"http_request", "condition", "delay", "notify", "ai", "approval", "order_action"} <= set(catalog_nodes):
        raise ValueError("Catalog is missing core node types")

    seed_ids = set()
    for workflow in seeds:
        if workflow["id"] in seed_ids:
            raise ValueError(f"Duplicate workflow id: {workflow['id']}")
        seed_ids.add(workflow["id"])
        validate_workflow(workflow, catalog_nodes, trigger_types)
    for required in ("wf_support_triage", "wf_expense_approval", "wf_slow_fulfillment", "wf_runaway"):
        if required not in seed_ids:
            raise ValueError(f"Missing required seed workflow '{required}'")

    payload_ids = set()
    for row in payloads:
        for field in ("id", "workflow", "note", "expected", "body"):
            if field not in row:
                raise ValueError(f"Payload missing '{field}': {row}")
        if row["id"] in payload_ids:
            raise ValueError(f"Duplicate payload id: {row['id']}")
        payload_ids.add(row["id"])
        if row["workflow"] not in seed_ids:
            raise ValueError(f"Payload {row['id']} references unknown workflow '{row['workflow']}'")
    for required in ("pay_001", "pay_inject_001", "pay_inject_002", "pay_101", "pay_201"):
        if required not in payload_ids:
            raise ValueError(f"Missing required payload '{required}'")

    nl_ids = set()
    trap_count = 0
    for case in nl_cases:
        for field in ("id", "expect", "description", "note"):
            if not case.get(field):
                raise ValueError(f"NL case missing '{field}': {case}")
        if case["id"] in nl_ids:
            raise ValueError(f"Duplicate NL case id: {case['id']}")
        nl_ids.add(case["id"])
        if case["expect"] == "refusal":
            trap_count += 1
            if not case.get("assertions", {}).get("must_refuse"):
                raise ValueError(f"NL trap {case['id']} missing must_refuse assertion")
        elif case["expect"] == "workflow":
            assertions = case.get("assertions", {})
            for node_type in assertions.get("required_node_types", []):
                if node_type not in catalog_nodes:
                    raise ValueError(f"NL case {case['id']} asserts unknown node type '{node_type}'")
        else:
            raise ValueError(f"NL case {case['id']} has unknown expect '{case['expect']}'")
    if trap_count < 3:
        raise ValueError(f"Expected at least 3 trap cases, found {trap_count}")
    if "nl_demo_001" not in nl_ids or "nl_trap_001" not in nl_ids:
        raise ValueError("Missing nl_demo_001 or nl_trap_001 (referenced by the demo flow)")

    print("Pack OK:")
    print(f"  {len(catalog_nodes)} node types, {len(trigger_types)} trigger types")
    print(f"  {len(seeds)} seed workflows, {len(payloads)} sample payloads")
    print(f"  {len(nl_cases)} NL eval cases ({trap_count} traps)")


if __name__ == "__main__":
    main()
