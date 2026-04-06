from __future__ import annotations

from typing import Any
from uuid import uuid4

from graph_agent.runtime.event_contract import normalize_runtime_event_dict


_MISSING = object()


def build_run_state(
    run_id: str,
    graph_id: str,
    input_payload: Any,
    *,
    documents: list[dict[str, Any]] | None = None,
    execution_node_ids: list[str] | None = None,
    agent_id: str | None = None,
    parent_run_id: str | None = None,
    agent_name: str | None = None,
) -> dict[str, Any]:
    normalized_execution_node_ids = [node_id for node_id in dict.fromkeys(execution_node_ids or []) if isinstance(node_id, str) and node_id]
    return {
        "run_id": run_id,
        "graph_id": graph_id,
        "agent_id": agent_id,
        "agent_name": agent_name,
        "parent_run_id": parent_run_id,
        "status": "queued",
        "status_reason": None,
        "input_payload": input_payload,
        "documents": list(documents or []),
        "current_node_id": None,
        "current_edge_id": None,
        "started_at": None,
        "ended_at": None,
        "runtime_instance_id": None,
        "last_heartbeat_at": None,
        "node_inputs": {},
        "node_outputs": {},
        "edge_outputs": {},
        "node_errors": {},
        "node_statuses": {node_id: "idle" for node_id in normalized_execution_node_ids},
        "iterator_states": {},
        "loop_regions": {},
        "visit_counts": {},
        "transition_history": [],
        "event_history": [],
        "final_output": None,
        "terminal_error": None,
        "agent_runs": {},
    }


def _omit_run_state_entry(record: dict[str, Any] | None, key: str) -> dict[str, Any]:
    if not record or key not in record:
        return record or {}
    next_record = dict(record)
    next_record.pop(key, None)
    return next_record


def _resolve_edge_output_from_event_history(
    event_history: list[dict[str, Any]],
    edge_payload: dict[str, Any],
) -> Any:
    source_node_id = edge_payload.get("source_id")
    source_handle_id = edge_payload.get("source_handle_id")
    if not isinstance(source_node_id, str) or not source_node_id:
        return _MISSING
    for candidate in reversed(event_history):
        if candidate.get("event_type") != "node.completed":
            continue
        candidate_payload = candidate.get("payload")
        if not isinstance(candidate_payload, dict):
            continue
        if candidate_payload.get("node_id") != source_node_id:
            continue
        if isinstance(source_handle_id, str) and source_handle_id:
            route_outputs = candidate_payload.get("route_outputs")
            if isinstance(route_outputs, dict) and source_handle_id in route_outputs:
                return route_outputs[source_handle_id]
        if "output" in candidate_payload:
            return candidate_payload["output"]
        return _MISSING
    return _MISSING


def _mark_terminal_node_statuses(
    node_statuses: dict[str, Any] | None,
    visit_counts: dict[str, Any] | None,
    node_errors: dict[str, Any] | None,
    *,
    terminal_status: str,
) -> dict[str, Any]:
    next_statuses = dict(node_statuses or {})
    for node_id, status in list(next_statuses.items()):
        normalized_status = str(status or "").strip() or "idle"
        if normalized_status == "active":
            if isinstance(node_errors, dict) and node_id in node_errors:
                normalized_status = "failed"
            elif terminal_status == "failed":
                normalized_status = "failed"
            elif isinstance(visit_counts, dict) and int(visit_counts.get(node_id, 0) or 0) > 0:
                normalized_status = "success"
            else:
                normalized_status = "idle"
        if normalized_status == "idle":
            normalized_status = "unreached"
        next_statuses[node_id] = normalized_status
    return next_statuses


def _build_iteration_id(iterator_node_id: Any, iterator_row_index: Any) -> str | None:
    if not isinstance(iterator_node_id, str) or not iterator_node_id:
        return None
    if not isinstance(iterator_row_index, int) or iterator_row_index <= 0:
        return None
    return f"{iterator_node_id}:row:{iterator_row_index}"


def _append_unique_string(values: list[Any], candidate: Any) -> list[str]:
    normalized_values = [value for value in values if isinstance(value, str) and value]
    if isinstance(candidate, str) and candidate and candidate not in normalized_values:
        normalized_values.append(candidate)
    return normalized_values


def _update_loop_region_state(
    previous_regions: dict[str, Any] | None,
    payload: dict[str, Any],
    *,
    include_status: bool = False,
) -> dict[str, Any] | None:
    iterator_node_id = payload.get("iterator_node_id")
    if (
        (not isinstance(iterator_node_id, str) or not iterator_node_id)
        and isinstance(payload.get("node_id"), str)
        and (
            payload.get("iterator_type") is not None
            or payload.get("current_row_index") is not None
            or payload.get("total_rows") is not None
        )
    ):
        iterator_node_id = payload.get("node_id")
    if not isinstance(iterator_node_id, str) or not iterator_node_id:
        return previous_regions
    next_regions = dict(previous_regions or {})
    existing_region = next_regions.get(iterator_node_id)
    current_region = dict(existing_region) if isinstance(existing_region, dict) else {}
    member_node_ids = [value for value in current_region.get("member_node_ids", []) if isinstance(value, str) and value]
    iteration_ids = [value for value in current_region.get("iteration_ids", []) if isinstance(value, str) and value]

    node_id = payload.get("node_id")
    if isinstance(node_id, str) and node_id and node_id != iterator_node_id:
        member_node_ids = _append_unique_string(member_node_ids, node_id)

    iteration_row_index = payload.get("iterator_row_index")
    if not isinstance(iteration_row_index, int):
        iteration_row_index = payload.get("current_row_index")
    iteration_id = payload.get("iteration_id")
    if not isinstance(iteration_id, str) or not iteration_id:
        iteration_id = _build_iteration_id(iterator_node_id, iteration_row_index)
    iteration_ids = _append_unique_string(iteration_ids, iteration_id)

    current_row_index = payload.get("current_row_index")
    if not isinstance(current_row_index, int):
        current_row_index = iteration_row_index if isinstance(iteration_row_index, int) else current_region.get("current_row_index")
    total_rows = payload.get("total_rows")
    if not isinstance(total_rows, int):
        total_rows = payload.get("iterator_total_rows") if isinstance(payload.get("iterator_total_rows"), int) else current_region.get("total_rows")
    status = current_region.get("status")
    if include_status and isinstance(payload.get("status"), str) and payload.get("status"):
        status = payload.get("status")

    next_regions[iterator_node_id] = {
        "iterator_node_id": iterator_node_id,
        "iterator_type": payload.get("iterator_type") if payload.get("iterator_type") is not None else current_region.get("iterator_type"),
        "status": status,
        "current_row_index": current_row_index,
        "total_rows": total_rows,
        "active_iteration_id": iteration_id if isinstance(iteration_id, str) and iteration_id else current_region.get("active_iteration_id"),
        "member_node_ids": member_node_ids,
        "iteration_ids": iteration_ids,
        "sheet_name": payload.get("sheet_name") if payload.get("sheet_name") is not None else current_region.get("sheet_name"),
        "source_file": payload.get("source_file") if payload.get("source_file") is not None else current_region.get("source_file"),
        "file_format": payload.get("file_format") if payload.get("file_format") is not None else current_region.get("file_format"),
    }
    return next_regions


def apply_single_run_event(previous: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    event = normalize_runtime_event_dict(event)
    next_state = {
        **previous,
        "event_history": [*previous.get("event_history", []), event],
    }
    event_type = event.get("event_type")
    payload = event.get("payload")
    if not isinstance(payload, dict):
        payload = {}

    if event_type == "run.started":
        next_state["status"] = "running"
        next_state["status_reason"] = None
        next_state["started_at"] = event.get("timestamp")

    if event_type == "node.started":
        node_id = payload.get("node_id")
        visit_count = payload.get("visit_count")
        next_state["current_node_id"] = node_id if isinstance(node_id, str) else None
        next_state["current_edge_id"] = None
        if isinstance(node_id, str):
            next_state["visit_counts"] = {
                **next_state.get("visit_counts", {}),
                node_id: int(visit_count) if isinstance(visit_count, int) else 0,
            }
            next_state["node_inputs"] = {
                **next_state.get("node_inputs", {}),
                node_id: payload.get("received_input"),
            }
            next_state["node_errors"] = _omit_run_state_entry(next_state.get("node_errors"), node_id)
            next_state["node_statuses"] = {
                **next_state.get("node_statuses", {}),
                node_id: "active",
            }
        next_loop_regions = _update_loop_region_state(next_state.get("loop_regions"), payload)
        if next_loop_regions is not None:
            next_state["loop_regions"] = next_loop_regions

    if event_type == "node.completed":
        node_id = payload.get("node_id")
        if next_state.get("current_node_id") == node_id:
            next_state["current_node_id"] = None
        if isinstance(node_id, str) and "output" in payload:
            next_state["node_outputs"] = {
                **next_state.get("node_outputs", {}),
                node_id: payload.get("output"),
            }
        if isinstance(node_id, str) and payload.get("error") is not None:
            next_state["node_errors"] = {
                **next_state.get("node_errors", {}),
                node_id: payload.get("error"),
            }
        elif isinstance(node_id, str):
            next_state["node_errors"] = _omit_run_state_entry(next_state.get("node_errors"), node_id)
        if isinstance(node_id, str):
            next_state["node_statuses"] = {
                **next_state.get("node_statuses", {}),
                node_id: "failed" if payload.get("error") is not None else "success",
            }
        next_loop_regions = _update_loop_region_state(next_state.get("loop_regions"), payload)
        if next_loop_regions is not None:
            next_state["loop_regions"] = next_loop_regions

    if event_type == "node.iterator.updated":
        node_id = payload.get("node_id")
        if isinstance(node_id, str):
            next_state["iterator_states"] = {
                **next_state.get("iterator_states", {}),
                node_id: {
                    "iterator_type": payload.get("iterator_type"),
                    "status": payload.get("status"),
                    "current_row_index": payload.get("current_row_index"),
                    "total_rows": payload.get("total_rows"),
                    "headers": payload.get("headers"),
                    "sheet_name": payload.get("sheet_name"),
                    "source_file": payload.get("source_file"),
                    "file_format": payload.get("file_format"),
                },
            }
        next_loop_regions = _update_loop_region_state(next_state.get("loop_regions"), payload, include_status=True)
        if next_loop_regions is not None:
            next_state["loop_regions"] = next_loop_regions

    if event_type == "edge.selected":
        selected_edge_id = payload.get("id")
        next_state["current_edge_id"] = selected_edge_id if isinstance(selected_edge_id, str) else None
        selected_edge_output = _resolve_edge_output_from_event_history(previous.get("event_history", []), payload)
        if isinstance(selected_edge_id, str) and selected_edge_output is not _MISSING:
            next_state["edge_outputs"] = {
                **next_state.get("edge_outputs", {}),
                selected_edge_id: selected_edge_output,
            }
        next_state["transition_history"] = [
            *next_state.get("transition_history", []),
            {
                "edge_id": payload.get("id"),
                "source_id": payload.get("source_id"),
                "target_id": payload.get("target_id"),
                "timestamp": event.get("timestamp"),
            },
        ]

    if event_type == "run.completed":
        next_state["status"] = "completed"
        next_state["status_reason"] = None
        next_state["current_node_id"] = None
        next_state["current_edge_id"] = None
        next_state["ended_at"] = event.get("timestamp")
        next_state["final_output"] = payload.get("final_output")
        next_state["node_statuses"] = _mark_terminal_node_statuses(
            next_state.get("node_statuses"),
            next_state.get("visit_counts"),
            next_state.get("node_errors"),
            terminal_status="completed",
        )

    if event_type == "run.failed":
        next_state["status"] = "failed"
        next_state["status_reason"] = None
        next_state["current_node_id"] = None
        next_state["current_edge_id"] = None
        next_state["ended_at"] = event.get("timestamp")
        next_state["terminal_error"] = payload.get("error")
        if "final_output" in payload:
            next_state["final_output"] = payload.get("final_output")
        next_state["node_statuses"] = _mark_terminal_node_statuses(
            next_state.get("node_statuses"),
            next_state.get("visit_counts"),
            next_state.get("node_errors"),
            terminal_status="failed",
        )

    if event_type == "run.cancelled":
        next_state["status"] = "cancelled"
        next_state["status_reason"] = None
        next_state["current_node_id"] = None
        next_state["current_edge_id"] = None
        next_state["ended_at"] = event.get("timestamp")
        next_state["terminal_error"] = payload.get("error")
        if "final_output" in payload:
            next_state["final_output"] = payload.get("final_output")
        next_state["node_statuses"] = _mark_terminal_node_statuses(
            next_state.get("node_statuses"),
            next_state.get("visit_counts"),
            next_state.get("node_errors"),
            terminal_status="cancelled",
        )

    if event_type == "run.interrupted":
        next_state["status"] = "interrupted"
        next_state["status_reason"] = payload.get("reason")
        next_state["current_node_id"] = None
        next_state["current_edge_id"] = None
        next_state["ended_at"] = event.get("timestamp")
        next_state["terminal_error"] = payload.get("error")
        if "final_output" in payload:
            next_state["final_output"] = payload.get("final_output")
        next_state["node_statuses"] = _mark_terminal_node_statuses(
            next_state.get("node_statuses"),
            next_state.get("visit_counts"),
            next_state.get("node_errors"),
            terminal_status="interrupted",
        )

    return next_state


def apply_event(previous: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    event = normalize_runtime_event_dict(event)
    event_type = str(event.get("event_type") or "")
    if not event_type.startswith("agent."):
        return apply_single_run_event(previous, event)

    payload = event.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    next_state = {
        **previous,
        "event_history": [*previous.get("event_history", []), event],
        "agent_runs": dict(previous.get("agent_runs", {})),
    }
    agent_id = str(event.get("agent_id") or payload.get("agent_id") or "")
    if not agent_id:
        return next_state
    agent_state = next_state["agent_runs"].get(agent_id)
    if agent_state is None:
        agent_state = build_run_state(
            str(payload.get("child_run_id") or uuid4()),
            str(previous.get("graph_id") or ""),
            previous.get("input_payload"),
            documents=previous.get("documents"),
            agent_id=agent_id,
            parent_run_id=str(previous.get("run_id") or ""),
            agent_name=str(payload.get("agent_name") or agent_id),
        )
    normalized_event = {
        **event,
        "event_type": event_type.removeprefix("agent."),
        "run_id": agent_state["run_id"],
    }
    next_state["agent_runs"][agent_id] = apply_single_run_event(agent_state, normalized_event)
    return next_state


def replay_events(initial_state: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    state = initial_state
    for event in events:
        state = apply_event(state, normalize_runtime_event_dict(event))
    return state
