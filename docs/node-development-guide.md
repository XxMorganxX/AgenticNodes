# Node Development Guide

This guide is the repo-specific playbook for adding a new node with the least guesswork possible.

Use it when you are:

- adding a brand-new node provider
- adding a new node variant that shares an existing category
- deciding whether a change belongs in a node provider, a model provider, or a new node kind
- wiring the backend, editor, validation, and tests for a new node end to end

## The Core Rule

Prefer the smallest extension that matches the behavior you need:

1. Add a new `NodeProviderDefinition` when the node can reuse an existing category and node kind.
2. Add a new model provider under `src/graph_agent/providers/` when the graph behavior is the same and only the vendor changes.
3. Add a new runtime node subclass only when execution semantics truly differ from the existing `input`, `data`, `control_flow_unit`, `model`, `tool`, `provider`, `mcp_context_provider`, `mcp_tool_executor`, or `output` kinds.

This is consistent with the architecture described in [CLAUDE.md](CLAUDE.md) and with the node provider system in [src/graph_agent/runtime/node_providers.py](src/graph_agent/runtime/node_providers.py).

## Mental Model

A node exists in four layers at once:

1. Registry metadata
   The node is registered as a `NodeProviderDefinition`.
2. Runtime behavior
   The graph loader and runtime know how to deserialize, validate, execute, and preview it.
3. Editor behavior
   The frontend knows how to create it, edit it, render it, and explain it.
4. Tests
   The repo has direct coverage for the node's catalog shape, validation, and runtime behavior.

If one of those layers is missing, the node will feel half-built even if the code compiles.

## Architecture Map

These are the main files to check when adding any node:

| Concern | Primary file(s) | Why it matters |
| --- | --- | --- |
| Node metadata shape | `src/graph_agent/runtime/node_providers.py` | Defines categories, contracts, and `NodeProviderDefinition`. |
| Node registration | `src/graph_agent/examples/tool_schema_repair.py` | The default services register all built-in node providers here. |
| Catalog exposure to frontend | `src/graph_agent/api/graph_store.py`, `src/graph_agent/api/manager.py` | The editor gets providers, contracts, and connection rules from the catalog. |
| Graph/node types in frontend | `frontend/src/lib/types.ts` | Shared document shape used by the editor. |
| Default node creation | `frontend/src/lib/editor.ts` | New nodes need sensible initial config when dropped onto the canvas. |
| Runtime deserialization | `src/graph_agent/runtime/core.py` | `_node_from_dict()` maps saved graph JSON into runtime classes. |
| Runtime execution | `src/graph_agent/runtime/core.py` | `execute()` and `runtime_input_preview()` implement node behavior. |
| Validation | `src/graph_agent/runtime/core.py` | `GraphDefinition.validate()` and `validate_against_services()` enforce graph rules. |
| Inspector and editing UI | `frontend/src/components/GraphInspector.tsx`, `frontend/src/components/ProviderDetailsModal.tsx`, `frontend/src/components/ToolDetailsModal.tsx` | Special-case editor UI often lives here. |
| Canvas rendering | `frontend/src/components/GraphCanvasNode.tsx`, `frontend/src/lib/nodeTooltip.ts` | Labels, handles, badges, and tooltip summaries live here. |
| Derived runtime output in editor | `frontend/src/lib/runtimeNodeOutputs.ts` | Needed for binding-only or synthetic output nodes. |
| Test coverage | `tests/` | Existing node families each have focused tests. |

## Step 0: Decide What You Are Actually Adding

Before touching code, answer these questions.

### A. Is this a new node provider or a new model provider?

Add a new model provider when:

- the graph node is still just a generic API/model node
- the only change is the underlying vendor or API transport
- the same node kind and editor behavior still make sense

Add a new node provider when:

- users need a new card on the canvas
- the node has its own description, config, capabilities, or routing rules
- the node does something graph-visible that is not just a vendor swap

### B. Can this reuse an existing node kind?

Usually yes.

Reuse an existing node kind when the node can behave like one of these:

- `input`
- `data`
- `control_flow_unit`
- `model`
- `tool`
- `provider`
- `mcp_context_provider`
- `mcp_tool_executor`
- `output`

Only create a new runtime subclass when the existing execution path would become confusing, unsafe, or full of hacks.

### C. Which category contract should it use?

Pick the category first. The category determines:

- which nodes it can connect to
- what envelope types it should accept
- what envelope type it should emit
- how the graph validator will reason about it

The canonical category contracts and connection rules live in [src/graph_agent/runtime/node_providers.py](src/graph_agent/runtime/node_providers.py).

## Step 1: Write The Node Spec First

Before coding, fill in this spec.

```yaml
provider_id: core.example_node
display_name: Example Node
category: data
node_kind: data
description: One-sentence human description shown in the editor.
capabilities:
  - example capability
produces_side_effects: false
preserves_input_payload: false
model_provider_name: null
default_config:
  mode: example_node
  input_binding:
    type: input_payload
config_fields:
  - key: mode
    label: Mode
    input_type: text
    help_text: ""
    placeholder: ""
accepted_inputs:
  - data_envelope
produced_outputs:
  - data_envelope
special_handles: []
binding_edges_allowed: false
requires_custom_inspector: false
requires_custom_canvas_rendering: false
requires_custom_tooltip: false
requires_custom_preview_output: false
requires_service_validation: false
test_files:
  - tests/test_example_node.py
```

If you cannot fill out this spec clearly, the implementation will likely drift.

## Step 2: Register The Node Provider

Add a `NodeProviderDefinition` in [src/graph_agent/examples/tool_schema_repair.py](src/graph_agent/examples/tool_schema_repair.py).

Every provider registration should include:

- `provider_id`
- `display_name`
- `category`
- `node_kind`
- `description`
- `capabilities`
- `default_config`
- `config_fields`
- `produces_side_effects` when relevant
- `preserves_input_payload` when relevant
- `model_provider_name` only when the node is tied to a model provider identity

Use existing nodes in that file as patterns:

- start and model nodes near the top
- data nodes in the middle
- control-flow and end nodes near the bottom

### Provider registration checklist

- Use a stable, namespaced `provider_id` like `core.*`, `start.*`, `end.*`, or `provider.*`.
- Keep `display_name` human and concise because it shows up all over the UI.
- Make `description` explain user-visible behavior, not implementation details.
- Make `capabilities` searchable and concrete.
- Put every editor-facing default in `default_config`.
- Put every editable field in `config_fields` unless the field is intentionally hidden.

## Step 3: Make Sure The Catalog Will Expose It

The frontend catalog is assembled in:

- [src/graph_agent/api/graph_store.py](src/graph_agent/api/graph_store.py)
- [src/graph_agent/api/manager.py](src/graph_agent/api/manager.py)

In most cases you do not need extra code here. If the node provider is registered correctly, it will flow into the catalog automatically.

You only need extra work if the new node depends on:

- provider status metadata
- Microsoft auth state
- MCP server state
- extra diagnostic payloads

## Step 4: Decide Whether Default Node Creation Needs Special Handling

When users drag a provider card into the canvas, the frontend constructs a node in [frontend/src/lib/editor.ts](frontend/src/lib/editor.ts).

Update `createNodeFromProvider()` when your node needs any special default initialization beyond the raw `default_config`.

Typical reasons to add a branch there:

- the node needs a generated prompt name
- the node needs a default tool name
- the node should inherit the graph's default Supabase connection
- the node needs initial handles, branches, or structured config
- the node has editing-time defaults that are more dynamic than `default_config`

### Good default creation behavior

- New node can be dropped onto the canvas without crashing.
- The inspector opens without requiring hidden fields.
- Validation may allow incomplete editing-time config where appropriate.
- The defaults produce a usable shape, even if the node is not fully configured yet.

## Step 5: Wire Runtime Deserialization

Graph JSON is converted into runtime node objects in `_node_from_dict()` in [src/graph_agent/runtime/core.py](src/graph_agent/runtime/core.py).

Update that function when:

- the new node can reuse an existing runtime class but needs provider-specific mapping
- the new node requires a special subclass
- the new node has a special kind-to-class mapping

Questions to answer:

- Which runtime class should represent the node?
- Does it need extra constructor arguments like `provider_name`, `prompt_name`, or `tool_name`?
- Is there legacy compatibility behavior to preserve?

If the node does not deserialize correctly, saved graphs and tests will fail even if the frontend can create the node.

## Step 6: Implement Runtime Behavior

Most backend node work happens in [src/graph_agent/runtime/core.py](src/graph_agent/runtime/core.py).

There are two main patterns.

### Pattern A: Reuse an existing runtime class and branch by `provider_id`

This is the normal path for many `data`, `control_flow_unit`, and `output` nodes.

Use this when:

- the node belongs naturally inside an existing class family
- the node emits the category's usual contract
- the execution flow is a variant of an existing pattern

You will usually update:

- `execute()`
- `runtime_input_preview()`
- helper functions near the class

### Pattern B: Add a dedicated runtime subclass

Use this when:

- the node has unique semantics that would make the shared class unreadable
- the node needs custom constructor state
- the node needs fundamentally different execution or output behavior

If you add a subclass, make sure:

- it derives from the right base class
- it sets the correct `kind`
- it returns `NodeExecutionResult`
- it resolves config through the normal config/env var path
- it is wired into `_node_from_dict()`

### Runtime behavior checklist

- Accept the right input contract for the node category.
- Emit the right output contract for the node category.
- Include helpful `metadata` on the output envelope.
- Respect graph env var resolution through `self.config`.
- Keep editing-time tolerance separate from execution-time strictness.
- Implement `runtime_input_preview()` when the editor should show the resolved runtime input.
- Keep side effects explicit and deterministic where possible.

## Step 7: Add Validation Rules

Validation lives in `GraphDefinition.validate()` and `GraphDefinition.validate_against_services()` in [src/graph_agent/runtime/core.py](src/graph_agent/runtime/core.py).

Add validation in the narrowest place possible.

### Use `validate()` for graph-structure rules

Examples:

- allowed incoming or outgoing edge kinds
- source-only or sink-only node restrictions
- allowed categories for connections
- special handle requirements
- branching constraints

### Use `validate_against_services()` for runtime/service rules

Examples:

- referenced tool names must exist
- provider names must exist
- Supabase connection ids must exist
- MCP tools must come from MCP-backed tool definitions
- certain config combinations are forbidden

### Validation philosophy

- Allow partial config while a user is still editing, when possible.
- Enforce hard requirements before runtime when a bad graph can be detected statically.
- Make error messages name the node id and the exact failing rule.

## Step 8: Add Or Update Editor UI

Some nodes work automatically from the catalog-driven editor UI. Others need explicit frontend branches.

Check these files:

- [frontend/src/components/GraphInspector.tsx](frontend/src/components/GraphInspector.tsx)
- [frontend/src/components/ProviderDetailsModal.tsx](frontend/src/components/ProviderDetailsModal.tsx)
- [frontend/src/components/ToolDetailsModal.tsx](frontend/src/components/ToolDetailsModal.tsx)
- [frontend/src/components/NodeDetailsForm.tsx](frontend/src/components/NodeDetailsForm.tsx)

You may need custom UI if the node has:

- structured config editing
- special auth flows
- preview actions
- dynamic form behavior
- custom tabs
- specialized help text

### Frontend editor checklist

- Can the node be selected and edited?
- Are all important config fields visible somewhere?
- Are defaults shown clearly?
- Are validation constraints understandable from the UI?
- Does the node need a tailored preview or diagnostics panel?

## Step 9: Update Canvas Rendering And Tooltips If Needed

Node cards and tooltips often have provider-specific branches.

Check:

- [frontend/src/components/GraphCanvasNode.tsx](frontend/src/components/GraphCanvasNode.tsx)
- [frontend/src/lib/nodeTooltip.ts](frontend/src/lib/nodeTooltip.ts)

Update these when the new node needs:

- custom badges or visual treatment
- extra handles
- provider-specific summary text
- special button affordances
- a unique card size or layout

If the node is a control-flow node, pay extra attention to handles and outgoing-edge behavior.

## Step 10: Add Derived Runtime Output Support If The Node Is Binding-Only Or Synthetic

Some nodes do not need to execute in the usual way for the editor to reason about them. For those nodes, the frontend derives runtime outputs in [frontend/src/lib/runtimeNodeOutputs.ts](frontend/src/lib/runtimeNodeOutputs.ts).

Touch that file when the node:

- emits a synthetic envelope without runtime execution
- is binding-only
- should preview output in the UI before a run reaches it directly

If your node emits ordinary runtime output from the backend, you usually do not need changes here.

## Step 11: Search For Provider ID Special Cases

Before calling the node done, search for its `provider_id` and look for places that branch by provider.

At minimum, search these directories:

```bash
rg -n "core.example_node|provider_id ===|provider_id ==" frontend/src src/graph_agent tests
```

Common places that need follow-up work:

- `frontend/src/lib/editor.ts`
- `frontend/src/components/GraphInspector.tsx`
- `frontend/src/components/GraphCanvas.tsx`
- `frontend/src/components/GraphCanvasNode.tsx`
- `frontend/src/lib/nodeTooltip.ts`
- `frontend/src/lib/runtimeNodeOutputs.ts`
- `src/graph_agent/runtime/core.py`

This repo has a meaningful amount of provider-specific behavior. A global search is part of the normal workflow, not a sign that the change went wrong.

## Step 12: Add Tests

Every new node should have tests in the style already used by the repo.

### Minimum expected coverage

1. Catalog registration test
   Assert the provider appears in the catalog with the expected `default_config` and metadata.
2. Graph validation test
   Assert valid graphs pass and invalid graph shapes fail with useful errors.
3. Runtime success test
   Execute a minimal graph and assert the node output contract and downstream traversal.
4. Runtime error test
   Assert failures are surfaced clearly and do not silently produce bad envelopes.
5. Editing-time tolerance test
   If the node can exist partially configured in the editor, assert that behavior.

### Optional but often valuable

- event payload assertions
- run-state assertions
- loop/iterator behavior
- env var resolution
- binding edge behavior
- service integration behavior
- special handle routing
- frontend type/build coverage if UI logic changed materially

### Good test file placement

Prefer one of these:

- add to the most relevant existing node-family test file
- create a focused new test file if the node has distinct behavior

Use existing tests as models:

- `tests/test_structured_payload_builder.py`
- `tests/test_supabase_data.py`
- `tests/test_parallel_splitter.py`
- `tests/test_runtime_normalizer.py`
- `tests/test_outlook_draft_node.py`
- `tests/test_spreadsheet_rows.py`

## Step 13: Verify End To End

A node is only done when all of these are true:

- it appears in the catalog
- it can be created from the editor
- its initial config is sane
- the graph can serialize and deserialize it
- validation errors are accurate
- runtime execution produces the expected envelope or terminal output
- any custom UI behaves correctly
- tests cover the intended behavior

## Generic Build Checklist

Copy this into the PR or task notes for a new node:

```md
- Wrote node spec
- Chose category and node kind intentionally
- Registered `NodeProviderDefinition`
- Confirmed catalog exposure
- Added default creation logic in `frontend/src/lib/editor.ts` if needed
- Wired `_node_from_dict()` if needed
- Implemented runtime behavior
- Implemented `runtime_input_preview()` if useful
- Added graph validation rules
- Added service validation rules
- Updated inspector/modal UI if needed
- Updated canvas rendering or tooltip UI if needed
- Updated derived runtime output logic if needed
- Searched for provider-id special cases across backend and frontend
- Added catalog tests
- Added validation tests
- Added runtime success/error tests
- Ran focused tests
```

## Anti-Patterns To Avoid

- Adding a brand-new node kind when a provider-specific branch in an existing kind would be clearer.
- Hard-coding frontend behavior without also registering the provider cleanly.
- Adding runtime behavior without editor defaults, leaving the node impossible to configure.
- Adding editor UI without validator support, leaving broken graphs undetected.
- Returning envelopes that do not match the node category contract.
- Making config required at edit time when it only needs to be required at run time.
- Forgetting `runtime_input_preview()` for nodes whose resolved input is important to inspect.
- Forgetting to search the frontend for provider-specific branches.

## Definition Of Done

A generic new node is complete when another agent can:

1. discover it in the catalog,
2. drag it onto the canvas,
3. understand what it does from the UI,
4. configure it without hidden knowledge,
5. run it in a graph successfully,
6. get useful validation when configured incorrectly, and
7. trust the tests to catch regressions.

If any of those fail, the node is not fully integrated yet.
