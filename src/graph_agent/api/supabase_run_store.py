from __future__ import annotations

import json
import logging
import os
import re
import socket
from threading import Lock
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


LOGGER = logging.getLogger(__name__)


def _read_request_timeout_seconds() -> float:
    raw = os.environ.get("GRAPH_AGENT_SUPABASE_RUN_STORE_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return 10.0
    try:
        value = float(raw)
    except ValueError:
        return 10.0
    return value if value > 0 else 10.0


_REQUEST_TIMEOUT_SECONDS = _read_request_timeout_seconds()
# HTTP read/connect timeout used by urllib (mirrors tune flush waits relative to this).
SUPABASE_RUN_STORE_REQUEST_TIMEOUT_SECONDS = float(_REQUEST_TIMEOUT_SECONDS)


# Egress instrumentation — set GRAPH_AGENT_SUPABASE_EGRESS_LOG=1 to log every
# response's byte size at INFO so we can see which queries dominate Supabase
# egress. Totals are accumulated per (method, table, select) tuple in-memory.
_EGRESS_LOG_ENABLED = os.environ.get("GRAPH_AGENT_SUPABASE_EGRESS_LOG", "").strip() in {"1", "true", "yes", "on"}
_EGRESS_TOTALS: dict[tuple[str, str, str], list[int]] = {}
_EGRESS_TOTALS_LOCK = Lock()


def _record_egress(method: str, table: str, select_clause: str, byte_count: int) -> None:
    if not _EGRESS_LOG_ENABLED:
        return
    key = (method, table, select_clause or "*")
    with _EGRESS_TOTALS_LOCK:
        bucket = _EGRESS_TOTALS.setdefault(key, [0, 0])
        bucket[0] += 1
        bucket[1] += byte_count
        running_count, running_bytes = bucket[0], bucket[1]
    LOGGER.info(
        "Supabase run store EGRESS %s %s select=%s bytes=%d total_calls=%d total_bytes=%d",
        method,
        table,
        select_clause or "*",
        byte_count,
        running_count,
        running_bytes,
    )


def supabase_egress_totals_snapshot() -> list[tuple[str, str, str, int, int]]:
    """Return a snapshot of (method, table, select, calls, bytes) for the current process."""
    with _EGRESS_TOTALS_LOCK:
        return [
            (method, table, select_clause, bucket[0], bucket[1])
            for (method, table, select_clause), bucket in _EGRESS_TOTALS.items()
        ]

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


_SCHEMA_CACHE_MISSING_COLUMN_RE = re.compile(r"Could not find the '([^']+)' column of '([^']+)' in the schema cache")


class _SchemaCacheColumnMissingError(RuntimeError):
    def __init__(self, *, table: str, column: str, detail: str) -> None:
        super().__init__(f"Supabase schema cache missing column {table}.{column}: {detail}")
        self.table = table
        self.column = column
        self.detail = detail


class _DuplicateRunEventSequenceError(RuntimeError):
    def __init__(self, *, detail: str) -> None:
        super().__init__(f"Duplicate run_events sequence number: {detail}")
        self.detail = detail


def _extract_schema_cache_missing_column(detail: str) -> tuple[str, str] | None:
    try:
        payload = json.loads(detail)
    except json.JSONDecodeError:
        payload = None
    if not isinstance(payload, dict) or payload.get("code") != "PGRST204":
        return None
    message = str(payload.get("message") or "")
    match = _SCHEMA_CACHE_MISSING_COLUMN_RE.search(message)
    if match is None:
        return None
    column_name, table_name = match.groups()
    return table_name, column_name


def _is_duplicate_run_event_sequence_violation(detail: str) -> bool:
    try:
        payload = json.loads(detail)
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False
    if str(payload.get("code") or "") != "23505":
        return False
    constraint_indicators = (
        "run_events_run_id_sequence_number",
        "(run_id, sequence_number)",
    )
    haystack = " ".join(
        str(payload.get(key) or "") for key in ("message", "details", "hint")
    )
    return any(indicator in haystack for indicator in constraint_indicators)


def _looks_like_html_document(detail: str) -> bool:
    stripped = detail.lstrip().lower()
    return stripped.startswith("<!doctype html") or stripped.startswith("<html")


def _format_http_error_detail(url: str, status_code: int, detail: str) -> str:
    normalized_detail = detail.strip()
    if status_code == 404 and _looks_like_html_document(normalized_detail):
        return (
            "404 Received an HTML page instead of the Supabase REST API. "
            "This usually means the configured Supabase URL points at Supabase Studio/dashboard "
            "rather than the project API base URL. Use a project URL like "
            "https://<project-ref>.supabase.co so requests to /rest/v1/... resolve correctly."
        )
    if status_code == 404 and "/rest/v1/" in url:
        return (
            "404 Supabase PostgREST endpoint not found. Check that the configured Supabase URL is "
            "your project API base URL and not a dashboard or Studio URL."
        )
    if not normalized_detail:
        return f"{status_code} <empty body>"
    if len(normalized_detail) > 400:
        normalized_detail = f"{normalized_detail[:400]}..."
    return f"{status_code} {normalized_detail}"


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
        self._unsupported_columns_lock = Lock()
        self._unsupported_columns_by_table: dict[str, set[str]] = {}

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
                "Supabase run store requires a URL (GRAPH_AGENT_SUPABASE_URL or SUPABASE_URL) "
                "and a service-role key (GRAPH_AGENT_SUPABASE_SECRET_KEY, "
                "GRAPH_AGENT_SUPABASE_SERVICE_ROLE_KEY, SUPABASE_SERVICE_ROLE_KEY, "
                "or SUPABASE_SECRET_KEY) in the process environment."
            )
        return cls(url=url, service_role_key=service_role_key, schema=schema)

    def initialize_run(self, state: dict[str, Any]) -> None:
        self.initialize_runs_batch([state])

    def append_event(self, run_id: str, event: dict[str, Any]) -> None:
        self.append_events_batch([(run_id, event)])

    def write_state(self, run_id: str, state: dict[str, Any]) -> None:
        self.write_states_batch([(run_id, state)])

    def initialize_runs_batch(self, states: list[dict[str, Any]]) -> None:
        if not states:
            return
        rows = [self._build_run_row(state, created_at=utc_now_iso()) for state in states]
        self._upsert_runs(rows)
        with self._sequence_lock:
            for state in states:
                run_id = str(state.get("run_id") or "")
                if run_id:
                    self._sequence_cache.setdefault(run_id, 0)

    def append_events_batch(self, items: list[tuple[str, dict[str, Any]]]) -> None:
        if not items:
            return
        normalized_items = [(str(run_id), normalize_runtime_event_dict(event)) for run_id, event in items]
        affected_run_ids = list(dict.fromkeys(run_id for run_id, _ in normalized_items))
        last_error: _DuplicateRunEventSequenceError | None = None
        for attempt in range(3):
            sequence_numbers = self._next_sequence_numbers([run_id for run_id, _ in normalized_items])
            if len(sequence_numbers) != len(normalized_items):
                raise RuntimeError(
                    "Supabase run store sequence allocation returned a mismatched event count during batch append."
                )
            rows = [
                self._build_event_row(run_id, event, sequence_number=sequence_number)
                for (run_id, event), sequence_number in zip(normalized_items, sequence_numbers)
            ]
            try:
                self._request_json_with_schema_fallback(
                    "POST",
                    self._table_path(self.events_table),
                    payload=rows,
                    prefer="return=minimal",
                )
                return
            except _DuplicateRunEventSequenceError as exc:
                last_error = exc
                LOGGER.warning(
                    "Supabase run_events sequence collision on attempt %d for run_ids=%s; "
                    "refreshing sequence cache and retrying. Detail: %s",
                    attempt + 1,
                    affected_run_ids,
                    exc.detail,
                )
                self._invalidate_sequence_cache(affected_run_ids)
        assert last_error is not None
        raise RuntimeError(
            f"Supabase run store could not resolve sequence-number collision after retries: {last_error.detail}"
        ) from last_error

    def _invalidate_sequence_cache(self, run_ids: list[str]) -> None:
        with self._sequence_lock:
            for run_id in run_ids:
                self._sequence_cache.pop(run_id, None)

    def write_states_batch(self, items: list[tuple[str, dict[str, Any]]]) -> None:
        if not items:
            return
        rows = [self._build_run_row(state) for _, state in items]
        self._upsert_runs(rows)

    # Tight column lists keep Supabase egress down: callers that only need
    # the manifest must NOT trigger a fetch of the (potentially MB-sized)
    # state_snapshot column.
    _MANIFEST_COLUMNS = (
        "run_id,graph_id,agent_id,agent_name,parent_run_id,input_payload,metadata,created_at,"
        "status,status_reason,started_at,ended_at,runtime_instance_id,last_heartbeat_at"
    )
    _STATE_COLUMNS = "run_id,state_snapshot"
    _MANIFEST_AND_STATE_COLUMNS = (
        "run_id,graph_id,agent_id,agent_name,parent_run_id,input_payload,metadata,created_at,"
        "status,status_reason,started_at,ended_at,runtime_instance_id,last_heartbeat_at,state_snapshot"
    )

    def load_manifest(self, run_id: str) -> dict[str, Any] | None:
        row = self._load_run_row(run_id, columns=self._MANIFEST_COLUMNS)
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
            "status": row.get("status"),
            "status_reason": row.get("status_reason"),
            "started_at": row.get("started_at"),
            "ended_at": row.get("ended_at"),
            "runtime_instance_id": row.get("runtime_instance_id"),
            "last_heartbeat_at": row.get("last_heartbeat_at"),
        }

    def load_events(
        self,
        run_id: str,
        *,
        since_sequence: int | None = None,
    ) -> list[dict[str, Any]]:
        query = {
            "run_id": f"eq.{run_id}",
            "select": "run_id,event_type,timestamp,agent_id,parent_run_id,summary,payload",
            "order": "sequence_number.asc",
        }
        if since_sequence is not None:
            query["sequence_number"] = f"gt.{int(since_sequence)}"
        rows = self._request_json_with_schema_fallback(
            "GET",
            self._table_path(self.events_table),
            query=query,
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
        row = self._load_run_row(run_id, columns=self._STATE_COLUMNS)
        if row is None:
            return None
        snapshot = row.get("state_snapshot")
        return normalize_runtime_state_snapshot(snapshot if isinstance(snapshot, dict) else None)

    def recover_run_state(self, run_id: str) -> dict[str, Any] | None:
        # Single round-trip for both manifest and state_snapshot, plus one for events.
        row = self._load_run_row(run_id, columns=self._MANIFEST_AND_STATE_COLUMNS)
        manifest: dict[str, Any] = {}
        snapshot: dict[str, Any] | None = None
        if row is not None:
            manifest = {
                "run_id": row.get("run_id"),
                "graph_id": row.get("graph_id"),
                "agent_id": row.get("agent_id"),
                "agent_name": row.get("agent_name"),
                "parent_run_id": row.get("parent_run_id"),
                "input_payload": row.get("input_payload"),
                "metadata": row.get("metadata"),
                "created_at": row.get("created_at"),
            }
            raw_snapshot = row.get("state_snapshot")
            snapshot = normalize_runtime_state_snapshot(raw_snapshot if isinstance(raw_snapshot, dict) else None)
        events = self.load_events(run_id)
        if not manifest and snapshot is None and not events:
            return None
        if not events:
            return snapshot
        graph_id = str(manifest.get("graph_id") or (snapshot or {}).get("graph_id") or run_id)
        input_payload = manifest.get("input_payload", (snapshot or {}).get("input_payload"))
        documents = (snapshot or {}).get("documents")
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
        rows = self._request_json_with_schema_fallback("GET", self._table_path(self.runs_table), query=query)
        return [dict(row) for row in rows]

    def _load_run_row(self, run_id: str, *, columns: str | None = None) -> dict[str, Any] | None:
        # Default to the manifest columns. Callers that need the (potentially
        # large) state_snapshot must opt in via _STATE_COLUMNS or _MANIFEST_AND_STATE_COLUMNS.
        select_clause = columns if columns is not None else self._MANIFEST_COLUMNS
        rows = self._request_json_with_schema_fallback(
            "GET",
            self._table_path(self.runs_table),
            query={"run_id": f"eq.{run_id}", "select": select_clause, "limit": "1"},
        )
        if not rows:
            return None
        return dict(rows[0])

    def _upsert_runs(self, rows: list[dict[str, Any]]) -> None:
        self._request_json_with_schema_fallback(
            "POST",
            self._table_path(self.runs_table),
            payload=rows,
            prefer="resolution=merge-duplicates,return=minimal",
        )

    def _next_sequence_number(self, run_id: str) -> int:
        return self._next_sequence_numbers([run_id])[0]

    def _next_sequence_numbers(self, run_ids: list[str]) -> list[int]:
        if not run_ids:
            return []
        with self._sequence_lock:
            for run_id in dict.fromkeys(run_ids):
                cached = self._sequence_cache.get(run_id)
                if cached is not None:
                    continue
                rows = self._request_json_with_schema_fallback(
                    "GET",
                    self._table_path(self.events_table),
                    query={
                        "run_id": f"eq.{run_id}",
                        "select": "sequence_number",
                        "order": "sequence_number.desc",
                        "limit": "1",
                    },
                )
                self._sequence_cache[run_id] = int(rows[0]["sequence_number"]) if rows else 0
            sequence_numbers: list[int] = []
            for run_id in run_ids:
                cached = int(self._sequence_cache.get(run_id, 0)) + 1
                self._sequence_cache[run_id] = cached
                sequence_numbers.append(cached)
            return sequence_numbers

    def _build_event_row(self, run_id: str, event: dict[str, Any], *, sequence_number: int) -> dict[str, Any]:
        return {
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

    def _build_run_row(self, state: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
        normalized_state = normalize_runtime_state_snapshot(state) or state
        return {
            "run_id": normalized_state.get("run_id"),
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
            "created_at": created_at or normalized_state.get("started_at") or utc_now_iso(),
        }

    def _table_path(self, table_name: str) -> str:
        return f"/rest/v1/{table_name}"

    def _request_json_with_schema_fallback(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        payload: Any | None = None,
        prefer: str | None = None,
    ) -> Any:
        table_name = self._table_name_from_path(path)
        for _ in range(8):
            try:
                return self._request_json(
                    method,
                    path,
                    query=self._sanitize_query(table_name, query),
                    payload=self._sanitize_payload(table_name, payload),
                    prefer=prefer,
                )
            except _SchemaCacheColumnMissingError as exc:
                if not self._mark_column_unsupported(exc.table, exc.column):
                    raise RuntimeError(
                        f"Supabase run store request could not recover after repeated schema-cache errors "
                        f"for {exc.table}.{exc.column}: {exc.detail}"
                    ) from exc
                LOGGER.warning(
                    "Supabase run store detected missing column %s.%s in the PostgREST schema cache; "
                    "future requests will omit it until the schema is updated or reloaded.",
                    exc.table,
                    exc.column,
                )
        raise RuntimeError(f"Supabase run store request could not recover after repeated schema-cache errors for {table_name}.")

    def _sanitize_query(self, table_name: str, query: dict[str, str] | None) -> dict[str, str] | None:
        if query is None:
            return None
        sanitized = dict(query)
        unsupported_columns = self._unsupported_columns(table_name)
        select_value = sanitized.get("select")
        if unsupported_columns and isinstance(select_value, str) and select_value.strip() and select_value.strip() != "*":
            selected_columns = [part.strip() for part in select_value.split(",") if part.strip()]
            sanitized_columns = [column for column in selected_columns if column not in unsupported_columns]
            sanitized["select"] = ",".join(sanitized_columns) if sanitized_columns else "*"
        return sanitized

    def _sanitize_payload(self, table_name: str, payload: Any | None) -> Any | None:
        unsupported_columns = self._unsupported_columns(table_name)
        if not unsupported_columns or payload is None:
            return payload
        if isinstance(payload, list):
            return [self._sanitize_payload_row(row, unsupported_columns) for row in payload]
        if isinstance(payload, dict):
            return self._sanitize_payload_row(payload, unsupported_columns)
        return payload

    def _sanitize_payload_row(self, row: Any, unsupported_columns: set[str]) -> Any:
        if not isinstance(row, dict):
            return row
        return {key: value for key, value in row.items() if key not in unsupported_columns}

    def _table_name_from_path(self, path: str) -> str:
        return path.rsplit("/", 1)[-1]

    def _unsupported_columns(self, table_name: str) -> set[str]:
        with self._unsupported_columns_lock:
            return set(self._unsupported_columns_by_table.get(table_name, set()))

    def _mark_column_unsupported(self, table_name: str, column_name: str) -> bool:
        with self._unsupported_columns_lock:
            unsupported_columns = self._unsupported_columns_by_table.setdefault(table_name, set())
            if column_name in unsupported_columns:
                return False
            unsupported_columns.add(column_name)
            return True

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
        payload_len = len(body) if body is not None else 0
        LOGGER.debug("Supabase run store %s %s (body=%d bytes)", method, url, payload_len)
        request = Request(url, data=body, method=method, headers=headers)
        try:
            with urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
                raw = response.read()
                status = response.status
        except (socket.timeout, TimeoutError) as exc:
            LOGGER.warning(
                "Supabase run store %s %s timed out after %.1fs",
                method,
                url,
                _REQUEST_TIMEOUT_SECONDS,
            )
            raise RuntimeError(
                f"Supabase run store request timed out after {_REQUEST_TIMEOUT_SECONDS}s ({method} {path})"
            ) from exc
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            missing_column = _extract_schema_cache_missing_column(detail)
            if missing_column is not None:
                table_name, column_name = missing_column
                raise _SchemaCacheColumnMissingError(table=table_name, column=column_name, detail=detail) from exc
            if exc.code == 409 and _is_duplicate_run_event_sequence_violation(detail):
                raise _DuplicateRunEventSequenceError(detail=detail) from exc
            error_detail = _format_http_error_detail(url, exc.code, detail)
            LOGGER.error(
                "Supabase run store %s %s failed: HTTP %d — %s",
                method,
                url,
                exc.code,
                error_detail,
            )
            raise RuntimeError(f"Supabase run store request failed: {error_detail}") from exc
        except URLError as exc:
            LOGGER.error("Supabase run store %s %s network error: %s", method, url, exc.reason)
            raise RuntimeError(f"Supabase run store network error: {exc.reason}") from exc
        LOGGER.debug("Supabase run store %s %s ok (status=%s, bytes=%d)", method, url, status, len(raw))
        _record_egress(
            method,
            self._table_name_from_path(path),
            (query or {}).get("select", "") if query else "",
            len(raw),
        )
        if not raw:
            return []
        return json.loads(raw.decode("utf-8"))

    def check_connectivity(self) -> None:
        try:
            self._request_json_with_schema_fallback(
                "GET",
                self._table_path(self.runs_table),
                query={"select": "run_id", "limit": "1"},
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Supabase run store connectivity check failed for %s (schema=%s, table=%s): %s. "
                "Writes will still be attempted; check credentials, schema, and that supabase/run_events_schema.sql has been applied.",
                self.url,
                self.schema,
                self.runs_table,
                exc,
            )
            return
        LOGGER.info(
            "Supabase run store reachable at %s (schema=%s, runs/%s + run_events/%s).",
            self.url,
            self.schema,
            self.runs_table,
            self.events_table,
        )
