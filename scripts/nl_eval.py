#!/usr/bin/env python3
"""One-command NL compiler eval over data/nl_eval.jsonl (EVALUATION_GUIDE §4). The compiler sees ONLY `description`.
Usage: python3 scripts/nl_eval.py [--out docs/verification/nl_eval]   (needs RELAY_AI_PROVIDER=openrouter + key)"""
import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from relay.ai import build_provider  # noqa: E402
from relay.compiler import NLCompiler  # noqa: E402
from relay.config import Settings  # noqa: E402
from relay.validation import load_catalog, node_index, reachable, unguarded_sensitive_nodes, validate_definition  # noqa: E402


def has_cycle(wf):
    idx, color = node_index(wf), {}

    def visit(n):
        color[n] = 1
        node = idx[n]
        for t in ([node.get("on_true"), node.get("on_false")] if node["type"] == "condition" else [node.get("next")]):
            if t in idx and (color.get(t) == 1 or (t not in color and visit(t))):
                return True
        color[n] = 2
        return False
    return any(n not in color and visit(n) for n in idx)


def score(case, r, catalog):
    a, out = case.get("assertions", {}), {"id": case["id"], "expected": case["expect"], "got": r.kind, "attempts": r.attempts, "checks": {}}
    if case["expect"] == "refusal":
        out["checks"]["refused_with_reason"] = r.kind == "refusal" and bool((r.refusal or {}).get("reason"))
        if r.kind == "workflow":
            out["checks"]["no_invented_types"] = all(n["type"] in catalog.nodes for n in r.workflow["nodes"])
        out["detail"] = r.refusal or (r.workflow and [n["type"] for n in r.workflow["nodes"]])
    elif r.kind != "workflow":
        ok = case["id"] == "nl_011" and r.kind == "refusal" and "approv" in json.dumps(r.refusal or {}).lower()
        out["checks"]["returned_workflow"] = ok
        out["detail"] = r.refusal or r.issues
    else:
        wf, types = r.workflow, {n["type"] for n in r.workflow["nodes"]}
        out["checks"]["publishes"] = validate_definition(wf, catalog).ok
        out["checks"]["no_invented_types"] = types <= set(catalog.nodes)
        out["checks"]["required_node_types"] = set(a.get("required_node_types", [])) <= types
        if a.get("requires_branching"):
            reach = reachable(wf)
            out["checks"]["branching"] = any(n["type"] == "condition" and n["id"] in reach and n.get("on_true") != n.get("on_false") for n in wf["nodes"])
        if a.get("approval_must_precede_order_action"):
            out["checks"]["approval_precedes_order_action"] = "order_action" in types and not unguarded_sensitive_nodes(wf, catalog)
        exp, act = a.get("trigger"), wf["trigger"]["type"]
        out["trigger"] = {"expected": exp, "actual": act, "matches": (act == "schedule" or has_cycle(wf)) if exp == "schedule_or_loop" else act == exp}
        out["detail"] = [f"{n['id']}:{n['type']}" for n in wf["nodes"]]
    out["result"] = "pass" if out["checks"] and all(out["checks"].values()) else "fail"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="docs/verification/nl_eval")
    args = ap.parse_args()
    s = Settings.from_env()
    provider = build_provider(s)
    if provider.name in ("heuristic", "fake"):
        sys.exit("NL eval needs a real model: set RELAY_AI_PROVIDER=openrouter and OPENROUTER_API_KEY")
    catalog = load_catalog(s.data_dir / "node_catalog.json")
    comp = NLCompiler(provider, catalog, s)
    cases = [json.loads(l) for l in (s.data_dir / "nl_eval.jsonl").read_text().splitlines() if l.strip()]

    def run(case):
        try:
            return score(case, comp.compile(case["description"]), catalog)
        except Exception as exc:  # provider outage counts as a miss, reported honestly
            return {"id": case["id"], "expected": case["expect"], "got": "error", "result": "fail", "checks": {}, "detail": str(exc)}
    with ThreadPoolExecutor(5) as ex:
        results = list(ex.map(run, cases))
    correct = sum(r["result"] == "pass" for r in results)
    strict = sum(r["result"] == "pass" and r.get("trigger", {}).get("matches", True) for r in results)
    report = {"model": f"{provider.name}:{provider.model}", "total": len(results), "correct": correct,
              "accuracy": round(correct / len(results), 3), "strict_correct_trigger_also_matches": strict, "cases": results}
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(json.dumps(report, indent=2))
    rows = ["| case | expected | got | result | checks | trigger |", "|---|---|---|---|---|---|"]
    for r in results:
        ch = ", ".join(f"{k} {'✔' if v else '✘'}" for k, v in r["checks"].items())
        t = r.get("trigger", {})
        rows.append(f"| {r['id']} | {r['expected']} | {r['got']} | **{r['result']}** | {ch} | {t.get('actual', '')}{'' if t.get('matches', True) else ' (expected ' + str(t.get('expected')) + ')'} |")
    out.with_suffix(".md").write_text(f"# NL compiler eval\n\nModel: `{report['model']}` · **Accuracy: {correct}/{len(results)} = {report['accuracy']:.0%}** "
                                      f"(EVALUATION_GUIDE §4 rules) · strict (trigger type also matches): {strict}/{len(results)}\n\n" + "\n".join(rows) + "\n")
    print(json.dumps({k: report[k] for k in ("model", "total", "correct", "accuracy", "strict_correct_trigger_also_matches")}))
    for r in results:
        print(f"  {r['result'].upper():4} {r['id']:12} expected={r['expected']:8} got={r['got']}  {r['checks']}")


if __name__ == "__main__":
    main()
