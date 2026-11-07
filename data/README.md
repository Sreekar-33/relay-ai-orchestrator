# Data

Files directly in this folder come verbatim from the Relay capstone pack (https://github.com/airtribe-projects/relay-capstone) and are unmodified.

Additions (loaded at startup in addition to the pack seeds; they do not affect the smoke test, which only checks the four pack seeds):

| File | Workflows | Purpose |
|---|---|---|
| `extra/demo_workflows.json` | `wf_unguarded_refund` | `order_action` with no approval node: publishing warns, the engine refuses it at runtime (`approval_required`) |
| | `wf_ai_repair_demo` | strict AI output schema to demonstrate the one-shot schema repair retry |
