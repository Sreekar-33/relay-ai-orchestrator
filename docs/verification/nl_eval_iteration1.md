# NL compiler eval

Model: `openrouter:openai/gpt-4o-mini` · **Accuracy: 11/15 = 73%** (EVALUATION_GUIDE §4 rules) · strict (trigger type also matches): 11/15

| case | expected | got | result | checks | trigger |
|---|---|---|---|---|---|
| nl_demo_001 | workflow | workflow | **pass** | publishes ✔, no_invented_types ✔, required_node_types ✔, branching ✔ | webhook |
| nl_002 | workflow | workflow | **pass** | publishes ✔, no_invented_types ✔, required_node_types ✔, branching ✔ | schedule |
| nl_003 | workflow | refusal | **fail** | returned_workflow ✘ |  |
| nl_004 | workflow | workflow | **pass** | publishes ✔, no_invented_types ✔, required_node_types ✔, approval_precedes_order_action ✔ | webhook |
| nl_005 | workflow | workflow | **pass** | publishes ✔, no_invented_types ✔, required_node_types ✔ | manual |
| nl_006 | workflow | workflow | **pass** | publishes ✔, no_invented_types ✔, required_node_types ✔, branching ✔ | webhook |
| nl_007 | workflow | workflow | **pass** | publishes ✔, no_invented_types ✔, required_node_types ✔, branching ✔ | schedule |
| nl_008 | workflow | refusal | **fail** | returned_workflow ✘ |  |
| nl_009 | workflow | workflow | **fail** | publishes ✔, no_invented_types ✔, required_node_types ✘, approval_precedes_order_action ✘ | webhook |
| nl_010 | workflow | workflow | **pass** | publishes ✔, no_invented_types ✔, required_node_types ✔ | webhook |
| nl_011 | workflow | refusal | **pass** | returned_workflow ✔ |  |
| nl_012 | workflow | refusal | **fail** | returned_workflow ✘ |  |
| nl_trap_001 | refusal | refusal | **pass** | refused_with_reason ✔ |  |
| nl_trap_002 | refusal | refusal | **pass** | refused_with_reason ✔ |  |
| nl_trap_003 | refusal | refusal | **pass** | refused_with_reason ✔ |  |
