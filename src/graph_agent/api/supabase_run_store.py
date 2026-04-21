from __future__ import annotations

import json
import os
from threading import Lock
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from graph_agent.api.run_state_reducer import build_run_state, replay_events
from graph_agent.runtime.event_contract import normalize_runtime_event_dict, normalize_runtime_state_snapshot
from graph_agent.runtime.core import (
    generation_prompt_capture_from_value,
    generation_prompt_captures_from_node_outputs,
    utc_now_iso,
)
from graph_agent.runtime.supabase_data import build_supabase_rest_auth_headers


def _process_env_by_selector(selector_name: str, fallback_name: str) -> str:
    env_var_name = os.environ.get(selector_name, "").strip() or fallback_name
    return os.environ.get(env_var_name, "").strip()


def _merge_snapshot_metadata(recovered: dict[str, Any], snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if snapshot is None:
        return recovered
    for key in ("status_reason", "runtime_instance_id", "last_heartbeat_at", "node_statuses", "iterator_states", "loop_regions", "documents"):
        if key in snapshot:
            recovered[key] = snapshot.get(key)
    snapshot_agent_runs = snapshot.get("agent_runs")
    recovered_agent_runs = recovered.get("agent_runs")
    if isinstance(snapshot_agent_runs, dict) and isinstance(recovered_agent_runs, dict):
        for agent_id, snapshot_agent_state in snapshot_agent_runs.items():
            recovered_agent_state = recovered_agent_runs.get(agent_id)
            if not isinstance(snapshot_agent_state, dict) or not isinstance(recovered_agent_state, dict):
                continue
            for key in ("status_reason", "runtime_instance_id", "last_heartbeat_at", "node_statuses", "iterator_states", "loop_regions", "documents"):
                if key in snapshot_agent_state:
                    recovered_agent_state[key] = snapshot_agent_state.get(key)
    return recovered


def _event_row_metadata(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return {}
    metadata: dict[str, Any] = {}
    for key in (
        "node_id",
        "node_kind",
        "node_category",
        "node_provider_id",
        "node_provider_label",
        "status",
        "session_id",
        "iterator_node_id",
        "iterator_row_index",
        "iterator_total_rows",
        "iteration_id",
    ):
        value = payload.get(key)
        if value is not None:
            metadata[key] = value
    payload_metadata = payload.get("metadata")
    if isinstance(payload_metadata, dict):
        for key in (
            "contract",
            "prompt_name",
            "response_mode",
            "provider",
            "should_call_tools",
            "tool_call_count",
            "tool_name",
            "tool_status",
            "no_tool_call",
        ):
            value = payload_metadata.get(key)
            if value is not None:
                metadata[key] = value
    generation_prompt = generation_prompt_capture_from_value(payload.get("output"))
    if generation_prompt is None:
        route_outputs = payload.get("route_outputs")
        if isinstance(route_outputs, dict):
            for route_output in route_outputs.values():
                generation_prompt = generation_prompt_capture_from_value(route_output)
                if generation_prompt is not None:
                    break
    if generation_prompt is not None:
        metadata["generation_prompt"] = generation_prompt
        metadata["generation_prompt_name"] = generation_prompt.get("prompt_name", "")
        metadata["generation_source_node_id"] = generation_prompt.get("source_node_id", "")
        metadata["generation_system_prompt"] = generation_prompt.get("system_prompt", "")
        metadata["generation_user_prompt"] = generation_prompt.get("user_prompt", "")
    return metadata


def _run_row_metadata(state: dict[str, Any]) -> dict[str, Any]:
    prompt_traces = generation_prompt_captures_from_node_outputs(state.get("node_outputs"))
    if not prompt_traces:
        return {}
    latest_prompt_trace = prompt_traces[-1]
    return {
        "prompt_traces": prompt_traces,
        "prompt_trace_count": len(prompt_traces),
        "latest_prompt_trace": latest_prompt_trace,
        "latest_prompt_name": latest_prompt_trace.get("prompt_name", ""),
        "latest_prompt_source_node_id": latest_prompt_trace.get("source_node_id", ""),
        "latest_system_prompt": latest_prompt_trace.get("system_prompt", ""),
        "latest_user_prompt": latest_prompt_trace.get("user_prompt", ""),
    }


class SupabaseRunStore:
    def __init__(
        self,
        *,
        url: str,
        service_role_key: str,
        schema: str = "public",
        runs_table: str = "runs",
        events_table: str = "run_events",
    ) -> None:
        self.url = url.rstrip("/")
        self.service_role_key = service_role_key.strip()
        self.schema = schema
        self.runs_table = runs_table
        self.events_table = events_table
        self._sequence_lock = Lock()
        self._sequence_cache: dict[str, int] = {}

    @classmethod
    def from_env(cls) -> "SupabaseRunStore":
        url = (
            _process_env_by_selector("GRAPH_AGENT_RUN_STORE_SUPABASE_URL_ENV_VAR", "GRAPH_AGENT_SUPABASE_URL")
            or os.environ.get("SUPABASE_URL", "").strip()
        )
        service_role_key = (
            _process_env_by_selector("GRAPH_AGENT_RUN_STORE_SUPABASE_SECRET_KEY_ENV_VAR", "GRAPH_AGENT_SUPABASE_SECRET_KEY")
            or os.environ.get("GRAPH_AGENT_SUPABASE_SERVICE_ROLE_KEY", "").strip()
            or os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
            or os.environ.get("SUPABASE_SECRET_KEY", "").strip()
        )
        schema = os.environ.get("GRAPH_AGENT_SUPABASE_SCHEMA", "").strip() or "public"
        if not url or not service_role_key:
            raise RuntimeError(
                "Supabase run store requires GRAPH_AGENT_SUPABASE_URL and GRAPH_AGENT_SUPABASE_SECRET_KEY."
            )
        return cls(url=url, service_role_key=service_role_key, schema=schema)

    def initialize_run(self, state: dict[str, Any]) -> None:
        run_id = str(state["run_id"])
        created_at = utc_now_iso()
        row = {
            "run_id": run_id,
            "graph_id": state.get("graph_id"),
            "agent_id": state.get("agent_id"),
            "agent_name": state.get("agent_name"),
            "parent_run_id": state.get("parent_run_id"),
            "status": state.get("status"),
            "status_reason": state.get("status_reason"),
            "started_at": state.get("started_at"),
            "ended_at": state.get("ended_at"),
            "runtime_instance_id": state.get("runtime_instance_id"),
            "last_heartbeat_at": state.get("last_heartbeat_at"),
            "input_payload": state.get("input_payload"),
            "final_output": state.get("final_output"),
            "terminal_error": state.get("terminal_error"),
            "current_node_id": state.get("current_node_id"),
            "current_edge_id": state.get("current_edge_id"),
            "state_snapshot": state,
            "metadata": _run_row_metadata(state),
            "created_at": created_at,
        }
        self._upsert_runs([row])
        with self._sequence_lock:
            self._sequence_cache.setdefault(run_id, 0)

    def append_event(self, run_id: str, event: dict[str, Any]) -> None:
        event = normalize_runtime_event_dict(event)
        sequence_number = self._next_sequence_number(run_id)
        row = {
            "run_id": run_id,
            "sequence_number": sequence_number,
            "event_type": event.get("event_type"),
            "timestamp": event.get("timestamp"),
            "agent_id": event.get("agent_id"),
            "parent_run_id": event.get("parent_run_id"),
            "summary": event.get("summary"),
            "payload": event.get("payload", {}),
            "metadata": _event_row_metadata(event),
        }
        self._request_json("POST", self._table_path(self.events_table), payload=[row], prefer="return=minimal")

    def write_state(self, run_id: str, state: dict[str, Any]) -> None:
        normalized_state = normalize_runtime_state_snapshot(state) or state
        row = {
            "run_id": run_id,
            "graph_id": normalized_state.get("graph_id"),
            "agent_id": normalized_state.get("agent_id"),
            "agent_name": normalized_state.get("agent_name"),
            "parent_run_id": normalized_state.get("parent_run_id"),
            "status": normalized_state.get("status"),
            "status_reason": normalized_state.get("status_reason"),
            "started_at": normalized_state.get("started_at"),
            "ended_at": normalized_state.get("ended_at"),
            "runtime_instance_id": normalized_state.get("runtime_instance_id"),
            "last_heartbeat_at": normalized_state.get("last_heartbeat_at"),
            "input_payload": normalized_state.get("input_payload"),
            "final_output": normalized_state.get("final_output"),
            "terminal_error": normalized_state.get("terminal_error"),
            "current_node_id": normalized_state.get("current_node_id"),
            "current_edge_id": normalized_state.get("current_edge_id"),
            "state_snapshot": normalized_state,
            "metadata": _run_row_metadata(normalized_state),
            "created_at": normalized_state.get("started_at") or utc_now_iso(),
        }
        self._upsert_runs([row])

    def load_manifest(self, run_id: str) -> dict[str, Any] | None:
        row = self._load_run_row(run_id)
        if row is None:
            return None
        return {
            "run_id": row.get("run_id"),
            "graph_id": row.get("graph_id"),
            "agent_id": row.get("agent_id"),
            "agent_name": row.get("agent_name"),
            "parent_run_id": row.get("parent_run_id"),
            "input_payload": row.get("input_payload"),
            "metadata": row.get("metadata"),
            "created_at": row.get("created_at"),
        }

    def load_events(self, run_id: str) -> list[dict[str, Any]]:
        rows = self._request_json(
            "GET",
            self._table_path(self.events_table),
            query={
                "run_id": f"eq.{run_id}",
                "select": "run_id,event_type,timestamp,agent_id,parent_run_id,summary,payload",
                "order": "sequence_number.asc",
            },
        )
        return [
            normalize_runtime_event_dict({
                "run_id": row.get("run_id"),
                "event_type": row.get("event_type"),
                "timestamp": row.get("timestamp"),
                "agent_id": row.get("agent_id"),
                "parent_run_id": row.get("parent_run_id"),
                "summary": row.get("summary"),
                "payload": row.get("payload", {}),
            })
            for row in rows
        ]

    def load_state(self, run_id: str) -> dict[str, Any] | None:
        row = self._load_run_row(run_id)
        if row is None:
            return None
        snapshot = row.get("state_snapshot")
        return normalize_runtime_state_snapshot(snapshot if isinstance(snapshot, dict) else None)

    def recover_run_state(self, run_id: str) -> dict[str, Any] | None:
        manifest = self.load_manifest(run_id) or {}
        snapshot = self.load_state(run_id)
        events = self.load_events(run_id)
        if not manifest and snapshot is None and not events:
            return None
        if not events:
            return snapshot
        graph_id = str(manifest.get("graph_id") or (snapshot or {}).get("graph_id") or run_id)
        input_payload = manifest.get("input_payload", (snapshot or {}).get("input_payload"))
        documents = manifest.get("documents", (snapshot or {}).get("documents"))
        initial_state = build_run_state(
            run_id,
            graph_id,
            input_payload,
            documents=documents,
            agent_id=manifest.get("agent_id", (snapshot or {}).get("agent_id")),
            parent_run_id=manifest.get("parent_run_id", (snapshot or {}).get("parent_run_id")),
            agent_name=manifest.get("agent_name", (snapshot or {}).get("agent_name")),
        )
        return _merge_snapshot_metadata(replay_events(initial_state, events), snapshot)

    def list_runs(self, *, graph_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        query = {
            "select": "run_id,graph_id,status,status_reason,started_at,ended_at,created_at,agent_id,agent_name,parent_run_id,runtime_instance_id,last_heartbeat_at,metadata",
            "order": "created_at.desc",
            "limit": str(limit),
        }
        if graph_id is not None:
            query["graph_id"] = f"eq.{graph_id}"
        rows = self._request_json("GET", self._table_path(self.runs_table), query=query)
        return [dict(row) for row in rows]

    def _load_run_row(self, run_id: str) -> dict[str, Any] | None:
        rows = self._request_json(
            "GET",
            self._table_path(self.runs_table),
            query={"run_id": f"eq.{run_id}", "select": "*", "limit": "1"},
        )
        if not rows:
            return None
        return dict(rows[0])

    def _upsert_runs(self, rows: list[dict[str, Any]]) -> None:
        self._request_json(
            "POST",
            self._table_path(self.runs_table),
            payload=rows,
            prefer="resolution=merge-duplicates,return=minimal",
        )

    def _next_sequence_number(self, run_id: str) -> int:
        with self._sequence_lock:
            cached = self._sequence_cache.get(run_id)
            if cached is None:
                rows = self._request_json(
                    "GET",
                    self._table_path(self.events_table),
                    query={
                        "run_id": f"eq.{run_id}",
                        "select": "sequence_number",
                        "order": "sequence_number.desc",
                        "limit": "1",
                    },
                )
                cached = int(rows[0]["sequence_number"]) if rows else 0
            cached += 1
            self._sequence_cache[run_id] = cached
            return cached

    def _table_path(self, table_name: str) -> str:
        return f"/rest/v1/{table_name}"

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        payload: Any | None = None,
        prefer: str | None = None,
    ) -> Any:
        url = f"{self.url}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        headers = {
            **build_supabase_rest_auth_headers(self.service_role_key),
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Accept-Profile": self.schema,
            "Content-Profile": self.schema,
        }
        if prefer:
            headers["Prefer"] = prefer
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(url, data=body, method=method, headers=headers)
        try:
            with urlopen(request) as response:
                raw = response.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase run store request failed: {exc.code} {detail}") from exc
        if not raw:
            return []
        return json.loads(raw.decode("utf-8"))
