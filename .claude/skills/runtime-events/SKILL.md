---
name: runtime-events
description: "Event, state, and control-loop contract (schema_version `runtime.v1`). Use when adding or modifying an event_type, changing run state shape, working in src/graph_agent/runtime/engine.py or core.py, touching api/run_state_reducer.py, adjusting SSE streaming or replay, modifying safety guards (DEFAULT_RUN_MAX_STEPS, DEFAULT_MAX_VISITS_PER_NODE, DEFAULT_MAX_REPAIR_ATTEMPTS), or introducing new node lifecycle / termination behavior."
---

# Runtime event + state contract

Source docs: `docs/control-loop.md`, `docs/state.md`, `docs/logging.md`.
This skill consolidates them because they share one schema version
(`runtime.v1`) and one set of compatibility rules.

## Single source of truth

Events are the contract. They drive: SSE live streaming, run replay,
reducer-based state projection, and UI rendering. Defined in
`src/graph_agent/runtime/event_contract.py`.

State is derived from events via `api/run_state_reducer.py`. **Nodes never
mutate shared state directly** — they return `NodeExecutionResult`, the
runtime emits events, and the reducer projects state.

## Control loop (`engine.py`)

1. Start at the declared start node.
2. Execute current node with current `RunContext`.
3. Persist node output / errors / timing into run state.
4. Evaluate outgoing edges in priority.
5. Take first matching conditional edge; otherwise the first standard edge.
6. Stop on output-node completion, guard limit, or unrecoverable error.

Safety guards live in `config.py` and produce **structured terminal events**,
not exceptions:

- `DEFAULT_RUN_MAX_STEPS`
- `DEFAULT_MAX_VISITS_PER_NODE`
- `DEFAULT_MAX_REPAIR_ATTEMPTS`

Tool validation errors are routable results, not thrown exceptions —
conditional edges can branch on them.

## `runtime.v1` envelope

Every event has:

- `schema_version`
- `event_type`
- `summary` (human readable)
- `payload` (machine readable)
- `run_id`
- `agent_id`
- `parent_run_id`
- `timestamp`

## Base event types

```
run.started
node.started
node.completed
edge.selected
condition.evaluated
retry.triggered
run.completed
run.failed
run.cancelled
run.interrupted
```

Multi-agent parent streams wrap these with an `agent.` prefix
(`agent.node.completed`, etc.).

## Reducer-critical payload keys (stable within `runtime.v1`)

| event_type | required keys |
|---|---|
| `node.started` | `node_id`, `visit_count`, `received_input` |
| `node.completed` | `node_id`, `output`, `route_outputs`, `error` |
| `edge.selected` | `id`, `source_id`, `target_id`, `source_handle_id` |
| `run.completed` | `final_output` |
| `run.failed`, `run.cancelled` | `error`, optional `final_output` |
| `run.interrupted` | `reason`, `error`, optional `final_output` |

## Compatibility rules (do not violate without bumping schema)

Inside `runtime.v1`:

- **Allowed:** add optional payload fields.
- **Stable:** every `event_type` name above.
- **Stable:** every reducer-critical payload key above.
- **Requires new schema version or adapter:** renaming an event, removing a
  reducer key, or changing an event's meaning.

If you need to change the contract: bump to `runtime.v2`, write an adapter
that translates v2 → v1 for stored runs, and update `event_history`
projection accordingly. `state.md` calls this out specifically because
persisted runs depend on event names being decodable across upgrades.

## Run state shape

Owned by `RunState` (see `docs/state.md`):

- run identifier, graph identifier
- initial input payload
- current node identifier
- ordered `event_history` (versioned events with `schema_version`)
- per-node outputs and errors
- transition history
- visit counts
- final output payload
- terminal status

In-memory only in v1 — runs do not survive process restarts unless the
configured run store (`GRAPH_AGENT_RUN_STORE`) persists events.

## Adding a new event safely

1. Decide if an existing `event_type` + new optional payload field works
   first (preferred — no compatibility cost).
2. If a new event type is genuinely needed:
   - Add it to `event_contract.py`.
   - Wire emission in the right control-loop hook in `engine.py`.
   - Update the reducer in `api/run_state_reducer.py` (and document new
     reducer-critical keys).
   - Update SSE/UI projections.
   - Add coverage in `tests/` — both event payload assertions and
     reducer projection assertions.
3. Document in `docs/logging.md` and bump this skill's reducer-critical
   keys table if relevant.

## Iterator row capture

Iterator nodes (`core.spreadsheet_rows`, `core.supabase_table_rows`) pop downstream `node_outputs` between rows, so per-iteration resolved prompts are gone by run termination. The engine emits `node.iterator.row_completed` at each iteration boundary (after the sub-graph drain, before the next reset) with payload `{node_id, iterator_node_id, iteration_index, total_rows, downstream_node_ids, prompt_map}`. `prompt_map` is keyed by node id with `{system_prompt, user_prompt, messages, prompt_name}` per downstream model node.

The manager handler routes that event to `RunStore.write_iteration_snapshot`, which writes an extra `runs` row with `phase='iteration'`, the same `run_id`, and the prompt map nested under `metadata.iteration`. `initialize_run` now writes `phase='started'` and terminal flushes write `phase='ended'`. Skipped when the iteration triggers run termination (terminal flush covers it). Filesystem store appends to `.logs/runs/<run_id>/iterations.jsonl`.

## Lean Supabase mirror

When `GRAPH_AGENT_RUN_STORE_SUPABASE_LEAN=1` (default in mirror mode), the
async Supabase mirror only forwards `run.*` and `agent.run.*` lifecycle events
to `run_events`. Per-node events (`node.started`, `node.completed`,
`edge.selected`, `condition.evaluated`, `retry.triggered`) are dropped at the
mirror boundary — the filesystem run store under `.logs/runs/` keeps the full
event log for replay and SSE.

`runs.state_snapshot` is also slimmed: only the keys that
`_merge_snapshot_metadata` consumes during recovery survive (status fields,
`node_statuses`, `iterator_states`, `loop_regions`, `documents`, plus run
identifiers). `node_outputs`, `node_inputs`, `event_history`,
`transition_history`, `node_errors`, and `edge_outputs` are stripped before
upload.

Prompt traces are unaffected: `_run_row_metadata` still extracts system+user
prompts per api/provider node into `runs.metadata.prompt_traces` *before*
slimming, so per-run prompt audit data persists.

Forced off when `GRAPH_AGENT_RUN_STORE_SUPABASE_PRIMARY=1` because Supabase is
the only recovery target in that mode and needs the full event stream + state.

## Common pitfalls

- Mutating state from inside a node — breaks replay.
- Throwing where a structured failure event was expected — breaks routing
  and observability.
- Renaming a reducer-critical key without an adapter — breaks stored runs.
- Adding an event that's emitted but never reduced — invisible to UI/replay.
