from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from graph_agent.runtime.run_documents import normalize_run_documents


RUNTIME_EVENT_SCHEMA_VERSION = "runtime.v1"

BASE_RUNTIME_EVENT_TYPES = (
    "run.started",
    "node.started",
    "node.completed",
    "node.iterator.updated",
    "node.iterator.config_patch",
    "edge.selected",
    "condition.evaluated",
    "retry.triggered",
    "run.completed",
    "run.failed",
    "run.cancelled",
    "run.interrupted",
)
AGENT_WRAPPED_RUNTIME_EVENT_TYPES = tuple(f"agent.{event_type}" for event_type in BASE_RUNTIME_EVENT_TYPES)
SUPPORTED_RUNTIME_EVENT_TYPES = (*BASE_RUNTIME_EVENT_TYPES, *AGENT_WRAPPED_RUNTIME_EVENT_TYPES)
TERMINAL_RUNTIME_EVENT_TYPES = {"run.completed", "run.failed", "run.cancelled", "run.interrupted"}

REDUCER_CRITICAL_PAYLOAD_FIELDS: dict[str, tuple[str, ...]] = {
    "run.started": ("graph_id", "graph_name"),
    "node.started": (
        "node_id",
        "visit_count",
        "received_input",
        "iterator_node_id",
        "iterator_row_index",
        "iterator_total_rows",
        "iteration_id",
    ),
    "node.completed": (
        "node_id",
        "output",
        "route_outputs",
        "error",
        "iterator_node_id",
        "iterator_row_index",
        "iterator_total_rows",
        "iteration_id",
    ),
    "node.iterator.updated": (
        "node_id",
        "status",
        "current_row_index",
        "total_rows",
        "iterator_node_id",
        "iterator_row_index",
        "iterator_total_rows",
        "iteration_id",
    ),
    "edge.selected": ("id", "source_id", "target_id", "source_handle_id"),
    "run.completed": ("final_output", "terminal_node_id"),
    "run.failed": ("error", "final_output"),
    "run.cancelled": ("error", "final_output"),
    "run.interrupted": ("reason", "error", "final_output"),
}


def _normalize_nullable_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def normalize_runtime_event_dict(event: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(event)
    payload = event.get("payload")
    normalized["schema_version"] = str(event.get("schema_version") or RUNTIME_EVENT_SCHEMA_VERSION)
    normalized["event_type"] = str(event.get("event_type") or "")
    normalized["summary"] = str(event.get("summary") or "")
    normalized["payload"] = dict(payload) if isinstance(payload, Mapping) else {}
    normalized["run_id"] = str(event.get("run_id") or "")
    normalized["agent_id"] = _normalize_nullable_string(event.get("agent_id"))
    normalized["parent_run_id"] = _normalize_nullable_string(event.get("parent_run_id"))
    normalized["timestamp"] = str(event.get("timestamp") or "")
    return normalized


def normalize_runtime_event_history(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [normalize_runtime_event_dict(event) for event in events]


def normalize_runtime_state_snapshot(state: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(state, Mapping):
        return None
    normalized = dict(state)
    event_history = state.get("event_history")
    normalized["event_history"] = (
        normalize_runtime_event_history(event_history)
        if isinstance(event_history, list)
        else []
    )
    transition_history = state.get("transition_history")
    normalized["transition_history"] = (
        [dict(transition) for transition in transition_history if isinstance(transition, Mapping)]
        if isinstance(transition_history, list)
        else []
    )
    event_count = state.get("event_count")
    normalized["event_count"] = (
        int(event_count)
        if isinstance(event_count, int) and event_count >= 0
        else len(normalized["event_history"])
    )
    transition_count = state.get("transition_count")
    normalized["transition_count"] = (
        int(transition_count)
        if isinstance(transition_count, int) and transition_count >= 0
        else len(normalized["transition_history"])
    )
    node_statuses = state.get("node_statuses")
    normalized["node_statuses"] = (
        {str(node_id): str(status or "") for node_id, status in node_statuses.items()}
        if isinstance(node_statuses, Mapping)
        else {}
    )
    iterator_states = state.get("iterator_states")
    normalized["iterator_states"] = (
        {str(node_id): dict(iterator_state) for node_id, iterator_state in iterator_states.items() if isinstance(iterator_state, Mapping)}
        if isinstance(iterator_states, Mapping)
        else {}
    )
    loop_regions = state.get("loop_regions")
    normalized["loop_regions"] = (
        {str(node_id): dict(loop_region) for node_id, loop_region in loop_regions.items() if isinstance(loop_region, Mapping)}
        if isinstance(loop_regions, Mapping)
        else {}
    )
    normalized["documents"] = normalize_run_documents(state.get("documents"))
    agent_runs = state.get("agent_runs")
    if isinstance(agent_runs, Mapping):
        normalized["agent_runs"] = {
            str(agent_id): normalize_runtime_state_snapshot(agent_state) or {}
            for agent_id, agent_state in agent_runs.items()
        }
    return normalized
