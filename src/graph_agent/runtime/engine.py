from __future__ import annotations

from collections import deque
from typing import Any
from collections.abc import Callable
from uuid import uuid4

from graph_agent.runtime.core import (
    API_MESSAGE_HANDLE_ID,
    API_TOOL_CALL_HANDLE_ID,
    CONTROL_FLOW_LOOP_BODY_HANDLE_ID,
    Edge,
    GraphDefinition,
    MCP_TERMINAL_OUTPUT_HANDLE_ID,
    NodeContext,
    NodeExecutionResult,
    RunState,
    RuntimeEvent,
    RuntimeServices,
    TransitionRecord,
)


def _base_node_label(node: Any) -> str:
    explicit_label = str(getattr(node, "label", "") or "").strip()
    if explicit_label:
        return explicit_label
    provider_label = str(getattr(node, "provider_label", "") or "").strip()
    if provider_label:
        return provider_label
    return str(getattr(node, "id", "node"))


def _node_instance_labels(graph: GraphDefinition) -> dict[str, str]:
    groups: dict[str, list[Any]] = {}
    for node in graph.nodes.values():
        base_label = _base_node_label(node)
        groups.setdefault(base_label, []).append(node)
    labels: dict[str, str] = {}
    for base_label, nodes in groups.items():
        if len(nodes) == 1:
            labels[str(nodes[0].id)] = base_label
            continue
        for index, node in enumerate(nodes, start=1):
            labels[str(node.id)] = f"{base_label} {index}"
    return labels


class GraphRuntime:
    def __init__(
        self,
        services: RuntimeServices,
        *,
        max_steps: int,
        max_visits_per_node: int,
        event_listeners: list[Callable[[RuntimeEvent], None]] | None = None,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> None:
        self.services = services
        self.max_steps = max_steps
        self.max_visits_per_node = max_visits_per_node
        self.event_listeners = event_listeners or []
        self.cancel_requested = cancel_requested or (lambda: False)

    def add_event_listener(self, listener: Callable[[RuntimeEvent], None]) -> None:
        self.event_listeners.append(listener)

    def emit(self, state: RunState, event_type: str, summary: str, payload: dict[str, Any]) -> RuntimeEvent:
        event = RuntimeEvent(
            event_type=event_type,
            summary=summary,
            payload=payload,
            run_id=state.run_id,
        )
        state.event_history.append(event)
        for listener in self.event_listeners:
            listener(event)
        return event

    def _describe_edge(self, edge: Edge, graph: GraphDefinition) -> dict[str, Any]:
        source_node = graph.nodes.get(edge.source_id)
        target_node = graph.nodes.get(edge.target_id)
        instance_labels = _node_instance_labels(graph)
        condition = edge.condition
        return {
            "edge_id": edge.id,
            "kind": edge.kind,
            "label": edge.label or None,
            "source_handle_id": edge.source_handle_id,
            "target_handle_id": edge.target_handle_id,
            "target_node_id": edge.target_id,
            "target_node_label": instance_labels.get(edge.target_id, target_node.label if target_node is not None else edge.target_id),
            "target_node_kind": target_node.kind if target_node is not None else None,
            "source_node_label": instance_labels.get(edge.source_id, source_node.label if source_node is not None else edge.source_id),
            "condition_label": condition.label if condition is not None else None,
            "condition_type": condition.condition_type if condition is not None else None,
            "condition_path": condition.path if condition is not None else None,
            "condition_value": condition.value if condition is not None else None,
        }

    def _no_matching_edge_error(
        self,
        graph: GraphDefinition,
        node_id: str,
        result: NodeExecutionResult,
    ) -> tuple[str, dict[str, Any]]:
        node = graph.nodes.get(node_id)
        instance_labels = _node_instance_labels(graph)
        node_label = instance_labels.get(node_id, node.label if node is not None else node_id)
        outgoing_edges = graph.get_outgoing_edges(node_id)
        available_routes = [self._describe_edge(edge, graph) for edge in outgoing_edges]
        result_contract = self._result_contract(result)
        route_count = len(outgoing_edges)
        if route_count == 0:
            message = f"Node '{node_label}' completed, but it has no outgoing execution edges."
        else:
            route_target_labels = ", ".join(route["target_node_label"] for route in available_routes) or "none"
            contract_clause = f" Output contract was '{result_contract}'." if result_contract else ""
            message = (
                f"Node '{node_label}' completed, but no outgoing edge matched its result."
                f"{contract_clause} Available routes: {route_target_labels}."
            )
        error = {
            "type": "no_matching_edge",
            "node_id": node_id,
            "node_label": node_label,
            "node_kind": node.kind if node is not None else None,
            "node_provider_id": node.provider_id if node is not None else None,
            "node_provider_label": node.provider_label if node is not None else None,
            "result_status": result.status,
            "result_contract": result_contract,
            "available_routes": available_routes,
            "message": message,
        }
        summary = f"No outgoing edge matched after node '{node_label}'."
        return summary, error

    def _frame_iteration_context(self, frame: dict[str, Any]) -> dict[str, Any]:
        context: dict[str, Any] = {}
        iterator_node_id = frame.get("iterator_node_id")
        iterator_row_index = frame.get("iterator_row_index")
        iterator_total_rows = frame.get("iterator_total_rows")
        if isinstance(iterator_node_id, str) and iterator_node_id:
            context["iterator_node_id"] = iterator_node_id
        if isinstance(iterator_row_index, int):
            context["iterator_row_index"] = iterator_row_index
        if isinstance(iterator_total_rows, int):
            context["iterator_total_rows"] = iterator_total_rows
        iteration_id = self._build_iteration_id(
            context.get("iterator_node_id"),
            context.get("iterator_row_index"),
        )
        if iteration_id is not None:
            context["iteration_id"] = iteration_id
        return context

    def _build_iteration_id(self, iterator_node_id: Any, iterator_row_index: Any) -> str | None:
        if not isinstance(iterator_node_id, str) or not iterator_node_id:
            return None
        if not isinstance(iterator_row_index, int) or iterator_row_index <= 0:
            return None
        return f"{iterator_node_id}:row:{iterator_row_index}"

    def _iterator_update_context(self, node_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        context: dict[str, Any] = {
            "iterator_node_id": node_id,
        }
        current_row_index = payload.get("current_row_index")
        total_rows = payload.get("total_rows")
        if isinstance(current_row_index, int):
            context["iterator_row_index"] = current_row_index
        if isinstance(total_rows, int):
            context["iterator_total_rows"] = total_rows
        iteration_id = self._build_iteration_id(node_id, current_row_index)
        if iteration_id is not None:
            context["iteration_id"] = iteration_id
        return context

    def _scoped_visit_key(self, node_id: str, frame: dict[str, Any]) -> tuple[str, str | None, int | None]:
        return (
            node_id,
            str(frame.get("iterator_node_id", "") or "").strip() or None,
            int(frame["iterator_row_index"]) if isinstance(frame.get("iterator_row_index"), int) else None,
        )

    def _binding_edges_for_node(self, graph: GraphDefinition, node_id: str) -> list[Edge]:
        return [
            edge
            for edge in graph.get_outgoing_edges(node_id)
            if edge.kind == "binding"
            and (target := graph.nodes.get(edge.target_id)) is not None
            and getattr(target, "provider_id", None) == "core.context_builder"
        ]

    def _extract_internal_metadata(self, result: NodeExecutionResult) -> dict[str, Any]:
        metadata = dict(result.metadata or {})
        internal = metadata.pop("_internal", {})
        result.metadata = metadata
        return dict(internal) if isinstance(internal, dict) else {}

    def _update_iterator_state(self, state: RunState, node_id: str, payload: dict[str, Any]) -> None:
        state.iterator_states[node_id] = {
            "iterator_type": payload.get("iterator_type"),
            "status": payload.get("status"),
            "current_row_index": payload.get("current_row_index"),
            "total_rows": payload.get("total_rows"),
            "headers": payload.get("headers"),
            "sheet_name": payload.get("sheet_name"),
            "source_file": payload.get("source_file"),
            "file_format": payload.get("file_format"),
        }

    def _emit_iterator_update(self, state: RunState, node_id: str, payload: dict[str, Any]) -> None:
        iterator_payload = {
            "node_id": node_id,
            **self._iterator_update_context(node_id, payload),
            **payload,
        }
        self._update_iterator_state(state, node_id, iterator_payload)
        self.emit(
            state,
            "node.iterator.updated",
            f"Updated iterator progress for node '{node_id}'.",
            iterator_payload,
        )

    def _enqueue_selected_edges(
        self,
        graph: GraphDefinition,
        state: RunState,
        pending_nodes: deque[dict[str, Any]],
        next_edges: list[tuple[Edge, NodeExecutionResult]],
        binding_edges: list[Edge],
        *,
        iteration_context: dict[str, Any] | None = None,
    ) -> None:
        inherited_context = dict(iteration_context or {})
        for next_edge, edge_result in next_edges:
            if edge_result.output is not None:
                state.edge_outputs[next_edge.id] = edge_result.output
            state.transition_history.append(
                TransitionRecord(
                    edge_id=next_edge.id,
                    source_id=next_edge.source_id,
                    target_id=next_edge.target_id,
                )
            )
            self.emit(
                state,
                "edge.selected",
                f"Transitioning from '{next_edge.source_id}' to '{next_edge.target_id}'.",
                next_edge.to_dict(),
            )
            frame = {"node_id": next_edge.target_id, "incoming_edge_id": next_edge.id, **inherited_context}
            target = graph.nodes.get(next_edge.target_id)
            if target is not None and getattr(target, "provider_id", None) == "core.context_builder":
                pending_nodes.appendleft(frame)
            else:
                pending_nodes.append(frame)

        for edge in binding_edges:
            state.transition_history.append(
                TransitionRecord(
                    edge_id=edge.id,
                    source_id=edge.source_id,
                    target_id=edge.target_id,
                )
            )
            self.emit(
                state,
                "edge.selected",
                f"Transitioning from '{edge.source_id}' to '{edge.target_id}'.",
                edge.to_dict(),
            )
            pending_nodes.appendleft(
                {"node_id": edge.target_id, "incoming_edge_id": edge.id, **inherited_context},
            )

    def _run_spreadsheet_row_iterator(
        self,
        graph: GraphDefinition,
        state: RunState,
        node: Any,
        result: NodeExecutionResult,
        row_envelopes: list[dict[str, Any]],
        step_state: dict[str, int],
        scoped_visit_counts: dict[tuple[str, str | None, int | None], int],
    ) -> RunState | None:
        iterator_state = dict(result.metadata.get("iterator_state", {})) if isinstance(result.metadata.get("iterator_state"), dict) else {}
        total_rows = len(row_envelopes)
        initial_state = {
            **iterator_state,
            "status": "completed" if total_rows == 0 else "running",
            "current_row_index": 0 if total_rows == 0 else 1,
            "total_rows": total_rows,
        }
        self._emit_iterator_update(state, node.id, initial_state)
        if total_rows == 0:
            return None

        for row_index, row_envelope in enumerate(row_envelopes, start=1):
            if self.cancel_requested():
                return self.cancel_run(state, summary=f"Run cancelled during spreadsheet iteration for node '{node.label}'.")
            state.node_outputs[node.id] = row_envelope
            self._emit_iterator_update(
                state,
                node.id,
                {
                    **iterator_state,
                    "status": "running",
                    "current_row_index": row_index,
                    "total_rows": total_rows,
                },
            )
            row_result = NodeExecutionResult(
                status="success",
                output=row_envelope,
                summary=f"Prepared spreadsheet row {row_index} of {total_rows}.",
                metadata={
                    "control_flow_handle_id": CONTROL_FLOW_LOOP_BODY_HANDLE_ID,
                    "iterator_type": "spreadsheet_rows",
                    "current_row_index": row_index,
                    "total_rows": total_rows,
                },
                route_outputs={CONTROL_FLOW_LOOP_BODY_HANDLE_ID: row_envelope},
            )
            next_edges = self.select_edges(graph, state, node.id, row_result)
            binding_edges = self._binding_edges_for_node(graph, node.id)
            if not next_edges and not binding_edges:
                failure_summary, failure_error = self._no_matching_edge_error(graph, node.id, row_result)
                return self.fail_run(
                    state,
                    summary=failure_summary,
                    error=failure_error,
                )
            row_pending_nodes: deque[dict[str, Any]] = deque()
            self._enqueue_selected_edges(
                graph,
                state,
                row_pending_nodes,
                next_edges,
                binding_edges,
                iteration_context={
                    "iterator_node_id": node.id,
                    "iterator_row_index": row_index,
                    "iterator_total_rows": total_rows,
                },
            )
            terminal_state = self._drain_pending_nodes(
                graph,
                state,
                row_pending_nodes,
                step_state=step_state,
                scoped_visit_counts=scoped_visit_counts,
                complete_run_on_output=False,
            )
            if terminal_state is not None and terminal_state.status in {"failed", "cancelled"}:
                return terminal_state

        self._emit_iterator_update(
            state,
            node.id,
            {
                **iterator_state,
                "status": "completed",
                "current_row_index": total_rows,
                "total_rows": total_rows,
            },
        )
        return None

    def _drain_pending_nodes(
        self,
        graph: GraphDefinition,
        state: RunState,
        pending_nodes: deque[dict[str, Any]],
        *,
        step_state: dict[str, int],
        scoped_visit_counts: dict[tuple[str, str | None, int | None], int],
        complete_run_on_output: bool,
    ) -> RunState | None:
        while pending_nodes and step_state["count"] < self.max_steps:
            if self.cancel_requested():
                return self.cancel_run(state, summary="Run cancelled before starting the next node.")
            frame = pending_nodes.popleft()
            current_node_id = str(frame["node_id"])
            state.current_node_id = current_node_id
            incoming_edge_id = frame.get("incoming_edge_id")
            state.current_edge_id = str(incoming_edge_id) if incoming_edge_id is not None else None
            node = graph.get_node(current_node_id)
            context = NodeContext(graph=graph, state=state, services=self.services, node_id=node.id)
            if not node.is_ready(context):
                pending_nodes.append(frame)
                step_state["count"] += 1
                continue

            scope_key = self._scoped_visit_key(current_node_id, frame)
            scoped_visit_count = scoped_visit_counts.get(scope_key, 0) + 1
            scoped_visit_counts[scope_key] = scoped_visit_count
            visit_count = state.visit_counts.get(current_node_id, 0) + 1
            state.visit_counts[current_node_id] = visit_count
            if scoped_visit_count > self.max_visits_per_node:
                return self.fail_run(
                    state,
                    summary=f"Node '{current_node_id}' exceeded the visit limit.",
                    error={
                        "type": "loop_guard",
                        "node_id": current_node_id,
                        "max_visits_per_node": self.max_visits_per_node,
                    },
                )

            try:
                received_input = node.runtime_input_preview(context)
            except Exception:  # noqa: BLE001
                received_input = None
            self.emit(
                state,
                "node.started",
                f"Started node '{node.label}'.",
                {
                    "node_id": node.id,
                    "node_kind": node.kind,
                    "node_category": node.category.value,
                    "node_provider_id": node.provider_id,
                    "node_provider_label": node.provider_label,
                    "visit_count": visit_count,
                    "received_input": received_input,
                    **self._frame_iteration_context(frame),
                },
            )

            if self.cancel_requested():
                return self.cancel_run(state, summary=f"Run cancelled before executing node '{node.label}'.")
            try:
                result = node.execute(context)
            except Exception as exc:  # noqa: BLE001
                return self.fail_run(
                    state,
                    summary=f"Node '{node.label}' raised an exception.",
                    error={"type": "node_exception", "node_id": node.id, "message": str(exc)},
                )

            if self.cancel_requested():
                return self.cancel_run(state, summary=f"Run cancelled while node '{node.label}' was executing.")
            internal_metadata = self._extract_internal_metadata(result)
            if result.error is not None:
                state.node_errors[node.id] = result.error
            if result.output is not None:
                state.node_outputs[node.id] = result.output

            self.emit(
                state,
                "node.completed",
                result.summary or f"Completed node '{node.label}'.",
                {
                    "node_id": node.id,
                    "node_kind": node.kind,
                    "node_category": node.category.value,
                    "node_provider_id": node.provider_id,
                    "node_provider_label": node.provider_label,
                    "status": result.status,
                    "output": result.output,
                    "route_outputs": result.route_outputs,
                    "error": result.error,
                    "metadata": result.metadata,
                    **self._frame_iteration_context(frame),
                },
            )

            row_envelopes = internal_metadata.get("spreadsheet_row_envelopes")
            if isinstance(row_envelopes, list):
                terminal_state = self._run_spreadsheet_row_iterator(
                    graph,
                    state,
                    node,
                    result,
                    [row for row in row_envelopes if isinstance(row, dict)],
                    step_state,
                    scoped_visit_counts,
                )
                if terminal_state is not None:
                    return terminal_state
                step_state["count"] += 1
                continue

            if node.kind == "output":
                if self._should_promote_output_result(graph, state, node.id, result):
                    state.final_output = result.output
                if complete_run_on_output and not pending_nodes:
                    state.status = "completed"
                    completion_event = self.emit(
                        state,
                        "run.completed",
                        "Run completed successfully.",
                        {"final_output": state.final_output, "terminal_node_id": node.id},
                    )
                    state.ended_at = completion_event.timestamp
                    return state
                step_state["count"] += 1
                continue

            next_edges = self.select_edges(graph, state, node.id, result)
            binding_edges = self._binding_edges_for_node(graph, node.id)
            hold_outgoing = bool(result.metadata.get("hold_outgoing_edges"))
            if hold_outgoing:
                next_edges = []
                binding_edges = []
            if not next_edges and not binding_edges:
                if hold_outgoing:
                    state.current_node_id = None
                    state.current_edge_id = None
                    step_state["count"] += 1
                    continue
                if result.status != "success" and result.error is not None:
                    return self.fail_run(
                        state,
                        summary=result.summary or f"Node '{node.label}' failed.",
                        error=result.error,
                    )
                failure_summary, failure_error = self._no_matching_edge_error(graph, node.id, result)
                return self.fail_run(
                    state,
                    summary=failure_summary,
                    error=failure_error,
                )

            self._enqueue_selected_edges(
                graph,
                state,
                pending_nodes,
                next_edges,
                binding_edges,
                iteration_context=self._frame_iteration_context(frame),
            )
            step_state["count"] += 1

        if step_state["count"] >= self.max_steps:
            return self.fail_run(
                state,
                summary="Run exceeded the maximum number of steps.",
                error={"type": "max_steps_exceeded", "max_steps": self.max_steps},
            )
        if complete_run_on_output and not pending_nodes and state.status == "running":
            state.status = "completed"
            completion_event = self.emit(
                state,
                "run.completed",
                "Run completed successfully.",
                {"final_output": state.final_output, "terminal_node_id": state.current_node_id},
            )
            state.ended_at = completion_event.timestamp
            return state
        return None

    def run(
        self,
        graph: GraphDefinition,
        input_payload: Any,
        *,
        run_id: str | None = None,
        agent_id: str | None = None,
        parent_run_id: str | None = None,
        documents: list[dict[str, Any]] | None = None,
    ) -> RunState:
        state = RunState(
            graph_id=graph.graph_id,
            input_payload=input_payload,
            documents=list(documents or []),
            run_id=run_id or str(uuid4()),
            agent_id=agent_id,
            parent_run_id=parent_run_id,
            status="running",
        )
        pending_nodes = deque([{"node_id": graph.start_node_id, "incoming_edge_id": None}])

        self.emit(
            state,
            "run.started",
            f"Run started for graph '{graph.name}'.",
            {"graph_id": graph.graph_id, "graph_name": graph.name},
        )
        step_state = {"count": 0}
        terminal_state = self._drain_pending_nodes(
            graph,
            state,
            pending_nodes,
            step_state=step_state,
            scoped_visit_counts={},
            complete_run_on_output=True,
        )
        if terminal_state is not None:
            return terminal_state
        if self.cancel_requested():
            return self.cancel_run(state, summary="Run cancelled before completion.")
        return state

    def select_edges(
        self,
        graph: GraphDefinition,
        state: RunState,
        node_id: str,
        result: NodeExecutionResult,
    ) -> list[tuple[Edge, NodeExecutionResult]]:
        outgoing = graph.get_outgoing_edges(node_id)
        source_node = graph.get_node(node_id)
        if source_node.kind == "mcp_tool_executor":
            remaining_outgoing = [
                edge for edge in outgoing if edge.source_handle_id != MCP_TERMINAL_OUTPUT_HANDLE_ID
            ]
            selected_edges = self._select_matching_edges(
                state,
                node_id,
                remaining_outgoing,
                result,
                allow_parallel=self._should_fan_out_to_multiple_end_nodes(graph, remaining_outgoing),
            )
            route_result = self._route_result(result, MCP_TERMINAL_OUTPUT_HANDLE_ID)
            if route_result is not None:
                handle_edges = [edge for edge in outgoing if edge.source_handle_id == MCP_TERMINAL_OUTPUT_HANDLE_ID]
                selected_edges.extend(
                    self._select_matching_edges(
                        state,
                        node_id,
                        handle_edges,
                        route_result,
                        allow_parallel=self._should_fan_out_to_multiple_end_nodes(graph, handle_edges),
                    )
                )
            return selected_edges
        has_explicit_api_outputs = source_node.kind == "model" and any(
            edge.source_handle_id in {API_TOOL_CALL_HANDLE_ID, API_MESSAGE_HANDLE_ID} for edge in outgoing
        )
        if has_explicit_api_outputs:
            selected_edges: list[tuple[Edge, NodeExecutionResult]] = []
            for handle_id in (API_TOOL_CALL_HANDLE_ID, API_MESSAGE_HANDLE_ID):
                route_result = self._route_result(result, handle_id)
                if route_result is None:
                    continue
                handle_edges = [edge for edge in outgoing if edge.source_handle_id == handle_id]
                selected_edges.extend(
                    self._select_matching_edges(
                        state,
                        node_id,
                        handle_edges,
                        route_result,
                        allow_parallel=True,
                    )
                )
            if selected_edges:
                return selected_edges
            outgoing = [edge for edge in outgoing if edge.source_handle_id not in {API_TOOL_CALL_HANDLE_ID, API_MESSAGE_HANDLE_ID}]
        explicit_handle_ids = sorted(
            {
                edge.source_handle_id
                for edge in outgoing
                if isinstance(edge.source_handle_id, str) and edge.source_handle_id.strip()
            }
        )
        if explicit_handle_ids and result.route_outputs:
            selected_edges: list[tuple[Edge, NodeExecutionResult]] = []
            for handle_id in explicit_handle_ids:
                route_result = self._route_result(result, handle_id)
                if route_result is None:
                    continue
                handle_edges = [edge for edge in outgoing if edge.source_handle_id == handle_id]
                selected_edges.extend(
                    self._select_matching_edges(
                        state,
                        node_id,
                        handle_edges,
                        route_result,
                        allow_parallel=self._should_fan_out_to_multiple_end_nodes(graph, handle_edges),
                    )
                )
            if selected_edges:
                return selected_edges
            outgoing = [edge for edge in outgoing if edge.source_handle_id not in explicit_handle_ids]
        return self._select_matching_edges(
            state,
            node_id,
            outgoing,
            result,
            allow_parallel=self._should_fan_out_to_multiple_end_nodes(graph, outgoing),
        )

    def select_edge(
        self,
        graph: GraphDefinition,
        state: RunState,
        node_id: str,
        result: NodeExecutionResult,
    ) -> Edge | None:
        selected = self.select_edges(graph, state, node_id, result)
        return selected[0][0] if selected else None

    def _should_promote_output_result(
        self,
        graph: GraphDefinition,
        state: RunState,
        node_id: str,
        result: NodeExecutionResult,
    ) -> bool:
        if result.output is None:
            return False
        if result.metadata.get("skip_final_output_promotion") is True:
            return False
        current_edge_id = state.current_edge_id
        if not current_edge_id:
            return True
        incoming_edge = next((edge for edge in graph.get_incoming_edges(node_id) if edge.id == current_edge_id), None)
        if incoming_edge is None:
            return True
        if incoming_edge.source_handle_id == MCP_TERMINAL_OUTPUT_HANDLE_ID:
            return state.final_output is None
        if incoming_edge.source_handle_id == API_MESSAGE_HANDLE_ID:
            edge_output = state.edge_outputs.get(incoming_edge.id)
            if isinstance(edge_output, dict):
                metadata = edge_output.get("metadata")
                if isinstance(metadata, dict) and (
                    metadata.get("should_call_tools") is True or metadata.get("need_tool") is True
                ):
                    return state.final_output is None
        return True

    def _should_fan_out_to_multiple_end_nodes(self, graph: GraphDefinition, outgoing: list[Edge]) -> bool:
        standard_edges = [edge for edge in outgoing if edge.kind == "standard"]
        if len(standard_edges) < 2:
            return False
        for edge in standard_edges:
            target = graph.nodes.get(edge.target_id)
            if target is None or target.kind != "output":
                return False
        return True

    def _select_matching_edges(
        self,
        state: RunState,
        node_id: str,
        outgoing: list[Edge],
        result: NodeExecutionResult,
        *,
        allow_parallel: bool = False,
    ) -> list[tuple[Edge, NodeExecutionResult]]:
        conditional_edges = [edge for edge in outgoing if edge.kind == "conditional"]
        standard_edges = [edge for edge in outgoing if edge.kind == "standard"]

        matched_conditional_edges: list[tuple[Edge, NodeExecutionResult]] = []
        for edge in conditional_edges:
            matched = edge.is_match(state, result)
            self.emit(
                state,
                "condition.evaluated",
                f"Condition '{edge.condition.label}' evaluated to {matched}.",
                {
                    "edge_id": edge.id,
                    "condition_id": edge.condition.id if edge.condition else None,
                    "matched": matched,
                },
            )
            if matched:
                if result.status != "success":
                    self.emit(
                        state,
                        "retry.triggered",
                        f"Retry path selected through edge '{edge.id}'.",
                        {"edge_id": edge.id, "node_id": node_id, "result_status": result.status},
                    )
                matched_conditional_edges.append((edge, result))
                if not allow_parallel:
                    return matched_conditional_edges

        if matched_conditional_edges:
            return matched_conditional_edges

        if result.status != "success":
            return []

        if standard_edges:
            if allow_parallel:
                return [(edge, result) for edge in standard_edges]
            return [(standard_edges[0], result)]

        return []

    def _route_result(self, result: NodeExecutionResult, handle_id: str) -> NodeExecutionResult | None:
        if handle_id not in result.route_outputs:
            return None
        route_output = result.route_outputs[handle_id]
        route_metadata = result.metadata
        if isinstance(route_output, dict):
            output_metadata = route_output.get("metadata")
            if isinstance(output_metadata, dict):
                route_metadata = dict(output_metadata)
        return NodeExecutionResult(
            status=result.status,
            output=route_output,
            error=result.error,
            summary=result.summary,
            metadata=dict(route_metadata),
        )

    def _result_contract(self, result: NodeExecutionResult) -> str | None:
        output = result.output
        if not isinstance(output, dict):
            return None
        metadata = output.get("metadata")
        if not isinstance(metadata, dict):
            return None
        contract = metadata.get("contract")
        return contract if isinstance(contract, str) and contract else None

    def fail_run(self, state: RunState, summary: str, error: dict[str, Any]) -> RunState:
        state.status = "failed"
        state.terminal_error = error
        failure_event = self.emit(state, "run.failed", summary, {"error": error})
        state.ended_at = failure_event.timestamp
        return state

    def cancel_run(self, state: RunState, summary: str) -> RunState:
        state.status = "cancelled"
        state.current_node_id = None
        state.current_edge_id = None
        state.terminal_error = {"type": "run_cancelled", "message": summary}
        cancelled_event = self.emit(state, "run.cancelled", summary, {"error": state.terminal_error})
        state.ended_at = cancelled_event.timestamp
        return state
