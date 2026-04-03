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

## Why This Matters

Graph systems can be non-deterministic even when the graph structure is static. A stable event model makes it possible to reason about the exact path a run took and why it took it.

## Extensibility

Future versions can fan out the same structured events to:

- stdout for local debugging
- file sinks
- external observability services
- persisted run history stores
