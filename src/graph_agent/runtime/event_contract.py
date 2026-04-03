from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


RUNTIME_EVENT_SCHEMA_VERSION = "runtime.v1"

BASE_RUNTIME_EVENT_TYPES = (
    "run.started",
    "node.started",
    "node.completed",
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
    "node.started": ("node_id", "visit_count", "received_input"),
    "node.completed": ("node_id", "output", "route_outputs", "error"),
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
    agent_runs = state.get("agent_runs")
    if isinstance(agent_runs, Mapping):
        normalized["agent_runs"] = {
            str(agent_id): normalize_runtime_state_snapshot(agent_state) or {}
            for agent_id, agent_state in agent_runs.items()
        }
    return normalized
