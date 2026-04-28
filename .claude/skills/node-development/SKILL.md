---
name: node-development
description: "Authoring contract for adding a node to the graph runtime. Use when registering a new NodeProviderDefinition, deciding between node-provider vs model-provider vs new node kind, wiring backend/editor/validation/tests for a node end to end, touching src/graph_agent/runtime/node_providers.py or core.py, frontend/src/lib/editor.ts, or anything matching the pattern `provider_id: core.*` / `start.*` / `end.*` / `provider.*`."
---

# Node development contract

Full walkthrough lives in `docs/node-development-guide.md`. This skill is the
operational summary — load the full doc when you actually need to write code.

## The core rule (do not skip)

Prefer the smallest extension that matches the behavior:

1. Add a new `NodeProviderDefinition` when an existing category + node kind
   already fits. **This is almost always the right answer.**
2. Add a new model provider under `src/graph_agent/providers/` when only the
   vendor changes.
3. Add a new runtime node subclass **only** when execution semantics genuinely
   differ from existing kinds (`input`, `data`, `control_flow_unit`, `model`,
   `tool`, `provider`, `mcp_context_provider`, `mcp_tool_executor`, `output`).

A node that works in only one of the four layers (registry / runtime / editor /
tests) is half-built — pay attention to all four.

## Layer map

| Concern | File |
|---|---|
| Provider metadata + categories | `src/graph_agent/runtime/node_providers.py` |
| Provider registration | `src/graph_agent/examples/tool_schema_repair.py` |
| Catalog exposure | `src/graph_agent/api/graph_store.py`, `api/manager.py` |
| Frontend types | `frontend/src/lib/types.ts` |
| Default node creation | `frontend/src/lib/editor.ts` (`createNodeFromProvider`) |
| Runtime deserialization | `src/graph_agent/runtime/core.py` (`_node_from_dict`) |
| Runtime execution | `src/graph_agent/runtime/core.py` (`execute`, `runtime_input_preview`) |
| Validation | `src/graph_agent/runtime/core.py` (`GraphDefinition.validate`, `validate_against_services`) |
| Inspector / modals | `frontend/src/components/GraphInspector.tsx`, `ProviderDetailsModal.tsx`, `ToolDetailsModal.tsx`, `NodeDetailsForm.tsx` |
| Canvas + tooltip | `frontend/src/components/GraphCanvasNode.tsx`, `frontend/src/lib/nodeTooltip.ts` |
| Derived synthetic outputs | `frontend/src/lib/runtimeNodeOutputs.ts` |

## Spec-first template

Before writing any code, fill this in:

```yaml
provider_id: core.example_node
display_name: Example Node
category: data
node_kind: data
description: One-sentence human description shown in the editor.
capabilities: [example capability]
produces_side_effects: false
preserves_input_payload: false
model_provider_name: null
default_config:
  mode: example_node
  input_binding: { type: input_payload }
config_fields:
  - { key: mode, label: Mode, input_type: text }
accepted_inputs: [data_envelope]
produced_outputs: [data_envelope]
binding_edges_allowed: false
requires_custom_inspector: false
requires_custom_canvas_rendering: false
requires_custom_tooltip: false
requires_service_validation: false
test_files: [tests/test_example_node.py]
```

If the spec is fuzzy, the implementation will drift. Resolve the spec before
touching code.

## Validation philosophy

- Allow partial config while editing.
- Enforce hard rules statically when a bad graph is detectable before runtime.
- Error messages must name the node id and the exact failing rule.
- Use `validate()` for graph-structure rules; `validate_against_services()`
  for tool/provider/connection existence checks.

## Anti-patterns

- New node kind when a `provider_id` branch in an existing class would do.
- Frontend behavior without a clean provider registration.
- Runtime work without editor defaults — node is impossible to configure.
- Editor UI without validator support — broken graphs slip through.
- Envelopes that don't match the category contract.
- Required-at-edit-time config that only needs to be required at run time.
- Forgetting `runtime_input_preview()` when the resolved input matters.
- Forgetting to grep for `provider_id` special cases across the frontend.

## Provider-id sweep before declaring done

```bash
rg -n "core\.example_node|provider_id ===|provider_id ==" frontend/src src/graph_agent tests
```

Provider-specific branches are a normal part of this codebase — the sweep is
a workflow step, not a sign something went wrong.

## Definition of done

Another agent should be able to: discover the node in the catalog, drag it
onto the canvas, understand its purpose from the UI, configure it without
hidden knowledge, run it, get useful validation errors when misconfigured,
and trust the tests to catch regressions. If any of those fail, it is not
fully integrated yet.

## When in doubt

Read `docs/node-development-guide.md` for the full step-by-step. Use existing
node families in `tests/` (e.g. `test_supabase_data.py`,
`test_structured_payload_builder.py`, `test_parallel_splitter.py`) as test
templates.
