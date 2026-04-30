# Logging Component

## Purpose

The logging layer provides observability for every meaningful graph execution step so branching, retries, and failures can be debugged without guessing.

## First-Version Decision

- Logging is event-based and structured.
- The same event model powers both debugging output and the live visualization UI.
- Events are stored in memory with the run and exposed through the API.

## Required Events

The first version records:

- run lifecycle events
- node lifecycle events
- condition evaluation results
- selected edge transitions
- model provider metadata
- tool validation and execution outcomes
- retry and loop-guard failures
- final output or terminal error

## Event Shape

Each event should contain:

- schema version
- event type
- timestamp
- run identifier
- node or edge identifier when relevant
- a human-readable summary
- a machine-readable payload for the UI

The current runtime contract is `runtime.v1`.

`runtime.v1` uses this top-level envelope:

- `schema_version`
- `event_type`
- `summary`
- `payload`
- `run_id`
- `agent_id`
- `parent_run_id`
- `timestamp`

Supported base event types in `runtime.v1`:

- `run.started`
- `node.started`
- `node.completed`
- `edge.selected`
- `condition.evaluated`
- `retry.triggered`
- `run.completed`
- `run.failed`
- `run.cancelled`
- `run.interrupted`

Multi-agent parent streams may wrap those same event types with the `agent.` prefix, such as `agent.node.completed` and `agent.run.completed`.

Reducer-critical payload fields that are stable within `runtime.v1`:

- `node.started`: `node_id`, `visit_count`, `received_input`
- `node.completed`: `node_id`, `output`, `route_outputs`, `error`
- `edge.selected`: `id`, `source_id`, `target_id`, `source_handle_id`
- `run.completed`: `final_output`
- `run.failed`, `run.cancelled`: `error`, optional `final_output`
- `run.interrupted`: `reason`, `error`, optional `final_output`

Compatibility rules for `runtime.v1`:

- adding optional payload fields is allowed
- existing `event_type` names stay stable within v1
- reducer-critical payload keys stay stable within v1
- renaming an event, removing a payload key, or changing event meaning requires a new schema version or a compatibility adapter

## Iterator row capture

Iterator nodes (`core.spreadsheet_rows`, `core.supabase_table_rows`, etc.) reset the `node_outputs` of every downstream node between rows. Without a checkpoint, the resolved `system_prompt`/`user_prompt`/`messages` for each prior iteration are lost by the time the run terminates. To preserve them:

- The engine emits `node.iterator.row_completed` at the end of each iteration body, before the next iteration's reset. Payload: `node_id`, `iterator_node_id`, `iteration_index` (1-based), `total_rows`, `downstream_node_ids`, and a `prompt_map` keyed by downstream node id with `{system_prompt, user_prompt, messages, prompt_name}` per node.
- Skipped when the iteration triggers run termination (failed/cancelled/output-node completion), since the existing terminal flush covers that snapshot.
- The manager translates each `node.iterator.row_completed` into a separate `runs` row via `RunStore.write_iteration_snapshot`. The new row has `phase='iteration'` and the same `run_id` as the parent run. The schema already supports multiple rows per `run_id` (see `supabase/run_events_schema.sql`).
- For symmetry, `initialize_run` now writes `phase='started'` and terminal flushes write `phase='ended'`.
- The filesystem run store appends per-iteration captures to `.logs/runs/<run_id>/iterations.jsonl`.

## Lean Supabase mirror

When `GRAPH_AGENT_RUN_STORE_SUPABASE_LEAN=1` (the default whenever Supabase is the async mirror, not the primary), the mirror writes a slimmer payload to keep storage costs bounded:

- `run_events` only receives `run.*` and `agent.run.*` lifecycle events. Per-node events (`node.started`, `node.completed`, `edge.selected`, `condition.evaluated`, `retry.triggered`) and their multi-agent `agent.node.*` variants are dropped at the mirror boundary.
- `runs.state_snapshot` is filtered down to the keys `_merge_snapshot_metadata` reads back during recovery (status fields, `node_statuses`, `iterator_states`, `loop_regions`, `documents`, plus run identifiers). Heavy fields — `node_outputs`, `node_inputs`, `event_history`, `transition_history`, `node_errors`, `edge_outputs` — are stripped.
- `runs.metadata.prompt_traces` is preserved verbatim. The system + user prompts captured per api/provider node are extracted from the full state *before* slimming, so per-run prompt audit data still lands on the run row.

The filesystem run store under `.logs/runs/` keeps the full event log and full `RunState` regardless of the lean flag, so local replay and SSE are unaffected. Lean mode is forced off when `GRAPH_AGENT_RUN_STORE_SUPABASE_PRIMARY=1` (Supabase becomes the sole recovery target and needs the full stream).

## Why This Matters

Graph systems can be non-deterministic even when the graph structure is static. A stable event model makes it possible to reason about the exact path a run took and why it took it.

## Extensibility

Future versions can fan out the same structured events to:

- stdout for local debugging
- file sinks
- external observability services
- persisted run history stores
