from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import socket
from pathlib import Path
from threading import Thread
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.api.manager import GraphRunManager
from graph_agent.api.run_log_store import RunLogStore
from graph_agent.api.run_store import AsyncBatchingRunStoreMirror, build_default_run_store
from graph_agent.api.run_state_reducer import apply_single_run_event, build_run_state
from graph_agent.api.supabase_run_store import SupabaseRunStore
from graph_agent.examples.tool_schema_repair import build_example_graph_payload, build_example_services
from graph_agent.runtime.event_contract import RUNTIME_EVENT_SCHEMA_VERSION


class _SupabaseStubHandler(BaseHTTPRequestHandler):
    runs: dict[str, dict[str, object]] = {}
    events: dict[str, list[dict[str, object]]] = {}
    run_requests: list[list[dict[str, object]]] = []
    event_requests: list[list[dict[str, object]]] = []
    get_queries: list[tuple[str, dict[str, list[str]]]] = []
    unsupported_columns: dict[str, set[str]] = {"runs": set(), "run_events": set()}
    force_runs_html_404 = False

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        self.__class__.get_queries.append((parsed.path, query))
        if parsed.path == "/rest/v1/runs":
            if self.__class__.force_runs_html_404:
                return self._write_html_404()
            if self._reject_if_select_has_unsupported_column("runs", _single_query_value(query, "select")):
                return
            rows = list(self.__class__.runs.values())
            run_id = _single_query_value(query, "run_id")
            graph_id = _single_query_value(query, "graph_id")
            if run_id:
                rows = [row for row in rows if row.get("run_id") == run_id.removeprefix("eq.")]
            if graph_id:
                rows = [row for row in rows if row.get("graph_id") == graph_id.removeprefix("eq.")]
            order = _single_query_value(query, "order")
            if order == "created_at.desc":
                rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
            limit = _single_query_value(query, "limit")
            if limit:
                rows = rows[: int(limit)]
            return self._write_json(rows)
        if parsed.path == "/rest/v1/run_events":
            if self._reject_if_select_has_unsupported_column("run_events", _single_query_value(query, "select")):
                return
            rows = list(self.__class__.events.get(_single_query_value(query, "run_id").removeprefix("eq."), []))
            sequence_filter = _single_query_value(query, "sequence_number")
            if sequence_filter.startswith("gt."):
                threshold = int(sequence_filter.removeprefix("gt."))
                rows = [row for row in rows if int(row.get("sequence_number", 0)) > threshold]
            order = _single_query_value(query, "order")
            reverse = order == "sequence_number.desc"
            rows.sort(key=lambda row: int(row.get("sequence_number", 0)), reverse=reverse)
            limit = _single_query_value(query, "limit")
            if limit:
                rows = rows[: int(limit)]
            select = _single_query_value(query, "select")
            if select == "sequence_number":
                rows = [{"sequence_number": row["sequence_number"]} for row in rows]
            return self._write_json(rows)
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0") or "0")).decode("utf-8"))
        if parsed.path == "/rest/v1/runs":
            if self._reject_if_payload_has_unsupported_column("runs", payload):
                return
            rows = [dict(row) for row in payload]
            self.__class__.run_requests.append(rows)
            for row in rows:
                run_id = str(row["run_id"])
                existing = dict(self.__class__.runs.get(run_id, {}))
                existing.update(row)
                self.__class__.runs[run_id] = existing
            self.send_response(201)
            self.end_headers()
            return
        if parsed.path == "/rest/v1/run_events":
            if self._reject_if_payload_has_unsupported_column("run_events", payload):
                return
            rows = [dict(row) for row in payload]
            self.__class__.event_requests.append(rows)
            for row in rows:
                run_id = str(row["run_id"])
                self.__class__.events.setdefault(run_id, []).append(dict(row))
            self.send_response(201)
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return

    def _reject_if_select_has_unsupported_column(self, table_name: str, select_value: str) -> bool:
        if not select_value or select_value == "*":
            return False
        selected_columns = [part.strip() for part in select_value.split(",") if part.strip()]
        for column_name in selected_columns:
            if column_name in self.__class__.unsupported_columns.get(table_name, set()):
                self._write_missing_column_error(table_name, column_name)
                return True
        return False

    def _reject_if_payload_has_unsupported_column(self, table_name: str, payload: object) -> bool:
        unsupported_columns = self.__class__.unsupported_columns.get(table_name, set())
        rows = payload if isinstance(payload, list) else [payload]
        for row in rows:
            if not isinstance(row, dict):
                continue
            for column_name in unsupported_columns:
                if column_name in row:
                    self._write_missing_column_error(table_name, column_name)
                    return True
        return False

    def _write_missing_column_error(self, table_name: str, column_name: str) -> None:
        detail = {
            "code": "PGRST204",
            "details": None,
            "hint": None,
            "message": f"Could not find the '{column_name}' column of '{table_name}' in the schema cache",
        }
        body = json.dumps(detail).encode("utf-8")
        self.send_response(400)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_json(self, payload: object) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_html_404(self) -> None:
        body = b"<!DOCTYPE html><html><head><title>Supabase</title></head><body>404</body></html>"
        self.send_response(404)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _single_query_value(query: dict[str, list[str]], key: str) -> str:
    values = query.get(key, [""])
    return values[0]


class SupabaseStubServer:
    def __enter__(self) -> str:
        _SupabaseStubHandler.runs = {}
        _SupabaseStubHandler.events = {}
        _SupabaseStubHandler.run_requests = []
        _SupabaseStubHandler.event_requests = []
        _SupabaseStubHandler.get_queries = []
        _SupabaseStubHandler.unsupported_columns = {"runs": set(), "run_events": set()}
        _SupabaseStubHandler.force_runs_html_404 = False
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _SupabaseStubHandler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


def wait_for_run_completion(manager: GraphRunManager, run_id: str, timeout_seconds: float = 5.0) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        state = manager.get_run(run_id)
        if state["status"] in {"completed", "failed"}:
            return state
        time.sleep(0.05)
    raise AssertionError(f"Run '{run_id}' did not finish within {timeout_seconds} seconds.")


def build_simple_graph_payload(graph_id: str = "simple-run-graph") -> dict[str, object]:
    return {
        "graph_id": graph_id,
        "name": "Simple Mirrored Graph",
        "graph_type": "graph",
        "agents": [
            {
                "agent_id": f"{graph_id}-agent",
                "name": "Simple Agent",
                "start_node_id": "start",
                "nodes": [
                    {
                        "id": "start",
                        "kind": "input",
                        "category": "start",
                        "label": "Start",
                        "provider_id": "start.manual_run",
                        "provider_label": "Run Button Start",
                        "config": {"input_binding": {"type": "input_payload"}},
                        "position": {"x": 0, "y": 0},
                    },
                    {
                        "id": "finish",
                        "kind": "output",
                        "category": "end",
                        "label": "Finish",
                        "provider_id": "core.output",
                        "provider_label": "Core Output Node",
                        "config": {"response_mode": "message"},
                        "position": {"x": 240, "y": 0},
                    },
                ],
                "edges": [
                    {
                        "id": "edge-start-finish",
                        "source_id": "start",
                        "target_id": "finish",
                        "label": "next",
                        "kind": "standard",
                        "priority": 100,
                    }
                ],
            }
        ],
    }


def build_simple_environment_payload(graph_id: str = "simple-mirror-environment") -> dict[str, object]:
    agents = []
    for index in range(1, 3):
        agents.append(
            {
                "agent_id": f"agent-{index}",
                "name": f"Agent {index}",
                "start_node_id": "start",
                "nodes": [
                    {
                        "id": "start",
                        "kind": "input",
                        "category": "start",
                        "label": "Start",
                        "provider_id": "start.manual_run",
                        "provider_label": "Run Button Start",
                        "config": {"input_binding": {"type": "input_payload"}},
                        "position": {"x": 0, "y": 0},
                    },
                    {
                        "id": "finish",
                        "kind": "output",
                        "category": "end",
                        "label": "Finish",
                        "provider_id": "core.output",
                        "provider_label": "Core Output Node",
                        "config": {"response_mode": "message"},
                        "position": {"x": 220, "y": 0},
                    },
                ],
                "edges": [
                    {
                        "id": f"edge-start-finish-{index}",
                        "source_id": "start",
                        "target_id": "finish",
                        "label": "next",
                        "kind": "standard",
                        "priority": 100,
                    }
                ],
            }
        )
    return {
        "graph_id": graph_id,
        "name": "Simple Mirrored Environment",
        "graph_type": "test_environment",
        "agents": agents,
    }


class SupabaseRunStoreTests(unittest.TestCase):
    def test_append_event_persists_generation_prompt_metadata(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            state = build_run_state("run-1", "graph-1", {"prompt": "hello"})
            store.initialize_run(state)
            event = {
                "event_type": "node.completed",
                "summary": "model finished",
                "payload": {
                    "node_id": "model",
                    "node_kind": "model",
                    "node_provider_id": "core.model",
                    "node_provider_label": "API Call Node",
                    "status": "success",
                    "output": {
                        "schema_version": "1.0",
                        "from_node_id": "model",
                        "from_category": "model",
                        "payload": "ok",
                        "artifacts": {
                            "request_messages": [
                                {"role": "system", "content": "Be concise."},
                                {"role": "user", "content": "Hello"},
                            ],
                            "system_prompt": "Be concise.",
                            "user_prompt": "Hello",
                        },
                        "metadata": {
                            "contract": "message_envelope",
                            "node_kind": "model",
                            "prompt_name": "reply_once",
                            "provider": "mock",
                            "response_mode": "message",
                        },
                    },
                    "metadata": {
                        "contract": "message_envelope",
                        "prompt_name": "reply_once",
                        "provider": "mock",
                        "response_mode": "message",
                    },
                },
                "run_id": "run-1",
                "timestamp": "2026-04-02T00:00:02Z",
                "agent_id": None,
                "parent_run_id": None,
            }
            store.append_event("run-1", event)

            rows = _SupabaseStubHandler.events["run-1"]
            self.assertEqual(len(rows), 1)
            row_metadata = rows[0]["metadata"]
            assert isinstance(row_metadata, dict)
            self.assertEqual(row_metadata["node_id"], "model")
            self.assertEqual(row_metadata["contract"], "message_envelope")
            self.assertEqual(row_metadata["generation_prompt_name"], "reply_once")
            self.assertEqual(row_metadata["generation_system_prompt"], "Be concise.")
            self.assertEqual(row_metadata["generation_user_prompt"], "Hello")
            self.assertEqual(row_metadata["generation_prompt"]["messages"][0]["role"], "system")

    def test_write_state_persists_prompt_traces_on_run_row_metadata(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            state = build_run_state("run-1", "graph-1", {"prompt": "hello"})
            state["node_outputs"] = {
                "model_a": {
                    "schema_version": "1.0",
                    "from_node_id": "model_a",
                    "from_category": "model",
                    "payload": "first",
                    "artifacts": {
                        "request_messages": [
                            {"role": "system", "content": "Prompt A"},
                            {"role": "user", "content": "Input A"},
                        ],
                        "system_prompt": "Prompt A",
                        "user_prompt": "Input A",
                    },
                    "metadata": {
                        "contract": "message_envelope",
                        "node_kind": "model",
                        "prompt_name": "prompt_a",
                    },
                },
                "model_b": {
                    "schema_version": "1.0",
                    "from_node_id": "model_b",
                    "from_category": "model",
                    "payload": "second",
                    "artifacts": {
                        "request_messages": [
                            {"role": "system", "content": "Prompt B"},
                            {"role": "user", "content": "Input B"},
                        ],
                        "system_prompt": "Prompt B",
                        "user_prompt": "Input B",
                    },
                    "metadata": {
                        "contract": "message_envelope",
                        "node_kind": "model",
                        "prompt_name": "prompt_b",
                    },
                },
            }
            store.write_state("run-1", state)

            row = _SupabaseStubHandler.runs["run-1"]
            row_metadata = row["metadata"]
            assert isinstance(row_metadata, dict)
            self.assertEqual(row_metadata["prompt_trace_count"], 2)
            self.assertEqual(row_metadata["latest_prompt_name"], "prompt_b")
            self.assertEqual(row_metadata["latest_system_prompt"], "Prompt B")
            self.assertEqual(row_metadata["latest_user_prompt"], "Input B")
            self.assertEqual(
                [trace["prompt_name"] for trace in row_metadata["prompt_traces"]],
                ["prompt_a", "prompt_b"],
            )

    def test_append_events_batch_preserves_per_run_sequence_numbers(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            store.initialize_runs_batch(
                [
                    build_run_state("run-1", "graph-1", {"prompt": "hello"}),
                    build_run_state("run-2", "graph-1", {"prompt": "world"}),
                ]
            )

            store.append_events_batch(
                [
                    (
                        "run-1",
                        {
                            "event_type": "node.started",
                            "summary": "first",
                            "payload": {"node_id": "node-a", "visit_count": 1},
                            "run_id": "run-1",
                            "timestamp": "2026-04-02T00:00:01Z",
                            "agent_id": None,
                            "parent_run_id": None,
                        },
                    ),
                    (
                        "run-2",
                        {
                            "event_type": "node.started",
                            "summary": "second",
                            "payload": {"node_id": "node-b", "visit_count": 1},
                            "run_id": "run-2",
                            "timestamp": "2026-04-02T00:00:02Z",
                            "agent_id": None,
                            "parent_run_id": None,
                        },
                    ),
                    (
                        "run-1",
                        {
                            "event_type": "node.completed",
                            "summary": "third",
                            "payload": {"node_id": "node-a", "output": {"answer": "ok"}, "error": None},
                            "run_id": "run-1",
                            "timestamp": "2026-04-02T00:00:03Z",
                            "agent_id": None,
                            "parent_run_id": None,
                        },
                    ),
                ]
            )

            self.assertEqual(len(_SupabaseStubHandler.event_requests), 1)
            self.assertEqual([row["sequence_number"] for row in _SupabaseStubHandler.events["run-1"]], [1, 2])
            self.assertEqual([row["sequence_number"] for row in _SupabaseStubHandler.events["run-2"]], [1])

    def test_async_mirror_batches_event_writes_and_coalesces_state_writes(self) -> None:
        with SupabaseStubServer() as url:
            mirror = AsyncBatchingRunStoreMirror(
                SupabaseRunStore(url=url, service_role_key="test-key"),
                flush_interval_ms=5000,
                event_batch_size=100,
                run_batch_size=25,
                flush_timeout_seconds=1.0,
            )
            state = build_run_state("run-1", "graph-1", {"prompt": "hello"})
            running_state = dict(state)
            running_state["status"] = "running"
            running_state["started_at"] = "2026-04-02T00:00:00Z"
            completed_state = dict(running_state)
            completed_state["status"] = "completed"
            completed_state["ended_at"] = "2026-04-02T00:00:03Z"

            mirror.initialize_run(state)
            mirror.append_event(
                "run-1",
                {
                    "event_type": "node.started",
                    "summary": "node started",
                    "payload": {"node_id": "node-a", "visit_count": 1},
                    "run_id": "run-1",
                    "timestamp": "2026-04-02T00:00:01Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )
            mirror.append_event(
                "run-1",
                {
                    "event_type": "node.completed",
                    "summary": "node done",
                    "payload": {"node_id": "node-a", "output": {"answer": "ok"}, "error": None},
                    "run_id": "run-1",
                    "timestamp": "2026-04-02T00:00:02Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )
            mirror.write_state("run-1", running_state)
            mirror.write_state("run-1", completed_state)
            mirror.flush()
            mirror.close()

            self.assertEqual(len(_SupabaseStubHandler.event_requests), 1)
            self.assertEqual(len(_SupabaseStubHandler.event_requests[0]), 2)
            self.assertEqual(sum(len(batch) for batch in _SupabaseStubHandler.run_requests), 2)
            self.assertEqual(_SupabaseStubHandler.runs["run-1"]["status"], "completed")

    def test_direct_supabase_store_append_event_remains_synchronous(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            state = build_run_state("run-1", "graph-1", {"prompt": "hello"})
            store.initialize_run(state)
            store.append_event(
                "run-1",
                {
                    "event_type": "node.started",
                    "summary": "first",
                    "payload": {"node_id": "node-a", "visit_count": 1},
                    "run_id": "run-1",
                    "timestamp": "2026-04-02T00:00:01Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )
            store.append_event(
                "run-1",
                {
                    "event_type": "node.completed",
                    "summary": "second",
                    "payload": {"node_id": "node-a", "output": {"answer": "ok"}, "error": None},
                    "run_id": "run-1",
                    "timestamp": "2026-04-02T00:00:02Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )

            self.assertEqual(len(_SupabaseStubHandler.event_requests), 2)
            self.assertTrue(all(len(batch) == 1 for batch in _SupabaseStubHandler.event_requests))

    def test_write_state_retries_without_missing_optional_run_column(self) -> None:
        with SupabaseStubServer() as url:
            _SupabaseStubHandler.unsupported_columns["runs"] = {"last_heartbeat_at"}
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            state = build_run_state("run-compat", "graph-1", {"prompt": "hello"})
            state["last_heartbeat_at"] = "2026-04-02T00:00:01Z"

            store.initialize_run(state)
            store.write_state("run-compat", state)

            row = _SupabaseStubHandler.runs["run-compat"]
            self.assertNotIn("last_heartbeat_at", row)
            self.assertEqual(store._unsupported_columns("runs"), {"last_heartbeat_at"})

            rows = store.list_runs(graph_id="graph-1")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "run-compat")
            self.assertNotIn("last_heartbeat_at", rows[0])

    def test_list_runs_retries_without_missing_selected_column(self) -> None:
        with SupabaseStubServer() as url:
            _SupabaseStubHandler.unsupported_columns["runs"] = {"last_heartbeat_at"}
            _SupabaseStubHandler.runs["run-read-compat"] = {
                "run_id": "run-read-compat",
                "graph_id": "graph-1",
                "status": "completed",
                "status_reason": None,
                "started_at": "2026-04-02T00:00:00Z",
                "ended_at": "2026-04-02T00:00:03Z",
                "created_at": "2026-04-02T00:00:00Z",
                "agent_id": None,
                "agent_name": None,
                "parent_run_id": None,
                "runtime_instance_id": None,
                "metadata": {},
            }
            store = SupabaseRunStore(url=url, service_role_key="test-key")

            rows = store.list_runs(graph_id="graph-1")

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "run-read-compat")
            self.assertEqual(store._unsupported_columns("runs"), {"last_heartbeat_at"})

    def test_list_runs_surfaces_dashboard_url_html_404_hint(self) -> None:
        with SupabaseStubServer() as url:
            _SupabaseStubHandler.force_runs_html_404 = True
            store = SupabaseRunStore(url=url, service_role_key="test-key")

            with self.assertRaises(RuntimeError) as context:
                store.list_runs(limit=1)

            self.assertIn("project API base URL", str(context.exception))
            self.assertIn("Studio/dashboard", str(context.exception))

    def test_store_recovers_run_state_from_events(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            state = build_run_state(
                "run-1",
                "graph-1",
                {"prompt": "hello"},
                documents=[
                    {
                        "document_id": "doc-1",
                        "name": "brief.txt",
                        "mime_type": "text/plain",
                        "size_bytes": 24,
                        "storage_path": "/tmp/brief.txt",
                        "text_content": "Use the attached checklist.",
                        "text_excerpt": "Use the attached checklist.",
                        "status": "ready",
                        "error": None,
                    }
                ],
            )
            store.initialize_run(state)
            state = apply_single_run_event(
                state,
                {
                    "event_type": "run.started",
                    "summary": "started",
                    "payload": {},
                    "run_id": "run-1",
                    "timestamp": "2026-04-02T00:00:00Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )
            store.append_event("run-1", state["event_history"][-1])
            state = apply_single_run_event(
                state,
                {
                    "event_type": "node.started",
                    "summary": "node started",
                    "payload": {"node_id": "node-a", "visit_count": 1, "received_input": {"prompt": "hello"}},
                    "run_id": "run-1",
                    "timestamp": "2026-04-02T00:00:01Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )
            store.append_event("run-1", state["event_history"][-1])
            state = apply_single_run_event(
                state,
                {
                    "event_type": "node.completed",
                    "summary": "node done",
                    "payload": {"node_id": "node-a", "output": {"answer": "ok"}, "error": None},
                    "run_id": "run-1",
                    "timestamp": "2026-04-02T00:00:02Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )
            store.append_event("run-1", state["event_history"][-1])
            state = apply_single_run_event(
                state,
                {
                    "event_type": "run.completed",
                    "summary": "done",
                    "payload": {"final_output": {"answer": "ok"}},
                    "run_id": "run-1",
                    "timestamp": "2026-04-02T00:00:03Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )
            store.append_event("run-1", state["event_history"][-1])
            store.write_state("run-1", state)

            recovered = store.recover_run_state("run-1")
            self.assertEqual(recovered, state)
            self.assertEqual(recovered["documents"][0]["document_id"], "doc-1")
            self.assertTrue(all(event["schema_version"] == RUNTIME_EVENT_SCHEMA_VERSION for event in recovered["event_history"]))
            rows = store.list_runs(graph_id="graph-1")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "run-1")

    def test_manager_keeps_prior_runs_queryable_on_rerun(self) -> None:
        with SupabaseStubServer() as url, tempfile.TemporaryDirectory() as temp_dir:
            services = build_example_services()
            bundled_path = Path(temp_dir) / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            store = GraphStore(
                services,
                path=Path(temp_dir) / "graphs.json",
                bundled_path=bundled_path,
            )
            graph_payload = build_example_graph_payload()
            graph_payload["graph_id"] = "rerun-graph"
            store.create_graph(graph_payload)
            run_store = SupabaseRunStore(url=url, service_role_key="test-key")
            manager = GraphRunManager(services=services, store=store, run_store=run_store)

            first_run_id = manager.start_run("rerun-graph", "first request")
            first_state = wait_for_run_completion(manager, first_run_id)
            second_run_id = manager.start_run("rerun-graph", "second request")
            second_state = wait_for_run_completion(manager, second_run_id)

            self.assertNotEqual(first_run_id, second_run_id)
            self.assertEqual(manager.get_run(first_run_id), first_state)
            self.assertEqual(manager.get_run(second_run_id), second_state)

            history = manager.list_runs("rerun-graph", limit=10)
            self.assertEqual({row["run_id"] for row in history}, {first_run_id, second_run_id})

    def test_manager_mirrors_run_logs_to_explicit_run_store_supabase_connection(self) -> None:
        with SupabaseStubServer() as url, tempfile.TemporaryDirectory() as temp_dir:
            services = build_example_services()
            bundled_path = Path(temp_dir) / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            store = GraphStore(
                services,
                path=Path(temp_dir) / "graphs.json",
                bundled_path=bundled_path,
            )
            graph_payload = build_example_graph_payload()
            graph_payload["graph_id"] = "mirrored-run-graph"
            graph_payload.setdefault("env_vars", {})
            graph_payload["env_vars"].update(
                {
                    "GRAPH_AGENT_SUPABASE_ANALYTICS_URL": url,
                    "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY": "sb_secret_analytics",
                }
            )
            graph_payload["supabase_connections"] = [
                {
                    "connection_id": "analytics-db",
                    "name": "Analytics DB",
                    "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_URL",
                    "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY",
                }
            ]
            graph_payload["run_store_supabase_connection_id"] = "analytics-db"
            store.create_graph(graph_payload)

            manager = GraphRunManager(
                services=services,
                store=store,
                run_log_store=RunLogStore(Path(temp_dir) / ".logs" / "runs"),
            )

            run_id = manager.start_run("mirrored-run-graph", "Repair the response schema.")
            wait_for_run_completion(manager, run_id)

            self.assertIn(run_id, _SupabaseStubHandler.runs)
            self.assertIn(run_id, _SupabaseStubHandler.events)

            run_row = _SupabaseStubHandler.runs[run_id]
            run_metadata = run_row["metadata"]
            assert isinstance(run_metadata, dict)
            self.assertGreater(int(run_metadata.get("prompt_trace_count", 0)), 0)
            self.assertTrue(str(run_metadata.get("latest_system_prompt", "")).strip())
            self.assertTrue(str(run_metadata.get("latest_user_prompt", "")).strip())

            event_rows = _SupabaseStubHandler.events[run_id]
            self.assertGreater(len(event_rows), 0)
            prompt_event_rows = [
                row
                for row in event_rows
                if isinstance(row.get("metadata"), dict) and row["metadata"].get("generation_prompt")
            ]
            self.assertGreater(len(prompt_event_rows), 0)
            self.assertTrue(str(prompt_event_rows[-1]["metadata"].get("generation_system_prompt", "")).strip())
            self.assertTrue(str(prompt_event_rows[-1]["metadata"].get("generation_user_prompt", "")).strip())

    def test_manager_terminal_flush_batches_mirror_writes_before_completion_returns(self) -> None:
        with SupabaseStubServer() as url, tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "GRAPH_AGENT_RUN_STORE_MIRROR_FLUSH_INTERVAL_MS": "5000",
                    "GRAPH_AGENT_RUN_STORE_MIRROR_EVENT_BATCH_SIZE": "100",
                    "GRAPH_AGENT_RUN_STORE_MIRROR_RUN_BATCH_SIZE": "25",
                    "GRAPH_AGENT_RUN_STORE_MIRROR_FLUSH_TIMEOUT_SECONDS": "1",
                },
                clear=False,
            ):
                services = build_example_services()
                bundled_path = Path(temp_dir) / "bundled_graphs.json"
                bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
                store = GraphStore(
                    services,
                    path=Path(temp_dir) / "graphs.json",
                    bundled_path=bundled_path,
                )
                graph_payload = build_simple_graph_payload("mirrored-terminal-flush-graph")
                graph_payload.setdefault("env_vars", {})
                graph_payload["env_vars"].update(
                    {
                        "GRAPH_AGENT_SUPABASE_ANALYTICS_URL": url,
                        "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY": "sb_secret_analytics",
                    }
                )
                graph_payload["supabase_connections"] = [
                    {
                        "connection_id": "analytics-db",
                        "name": "Analytics DB",
                        "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_URL",
                        "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY",
                    }
                ]
                graph_payload["run_store_supabase_connection_id"] = "analytics-db"
                store.create_graph(graph_payload)

                manager = GraphRunManager(
                    services=services,
                    store=store,
                    run_log_store=RunLogStore(Path(temp_dir) / ".logs" / "runs"),
                )
                try:
                    run_id = manager.start_run("mirrored-terminal-flush-graph", "hello")
                    state = wait_for_run_completion(manager, run_id)

                    self.assertEqual(state["status"], "completed")
                    self.assertIn(run_id, _SupabaseStubHandler.runs)
                    self.assertIn(run_id, _SupabaseStubHandler.events)
                    event_rows = _SupabaseStubHandler.events[run_id]
                    self.assertGreater(len(event_rows), 0)
                    self.assertEqual(sum(len(batch) for batch in _SupabaseStubHandler.event_requests), len(event_rows))
                    self.assertLess(len(_SupabaseStubHandler.event_requests), len(event_rows))
                finally:
                    manager.stop_background_services()

    def test_manager_stop_background_services_drains_pending_async_mirror_writes(self) -> None:
        with SupabaseStubServer() as url, tempfile.TemporaryDirectory() as temp_dir:
            services = build_example_services()
            bundled_path = Path(temp_dir) / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            store = GraphStore(
                services,
                path=Path(temp_dir) / "graphs.json",
                bundled_path=bundled_path,
            )
            manager = GraphRunManager(
                services=services,
                store=store,
                run_log_store=RunLogStore(Path(temp_dir) / ".logs" / "runs"),
            )
            mirror = AsyncBatchingRunStoreMirror(
                SupabaseRunStore(url=url, service_role_key="test-key"),
                flush_interval_ms=5000,
                event_batch_size=100,
                run_batch_size=25,
                flush_timeout_seconds=1.0,
            )
            state = build_run_state("queued-run", "graph-1", {"prompt": "hello"})
            mirror.initialize_run(state)
            mirror.append_event(
                "queued-run",
                {
                    "event_type": "run.started",
                    "summary": "started",
                    "payload": {},
                    "run_id": "queued-run",
                    "timestamp": "2026-04-02T00:00:00Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )
            with manager._lock:
                manager._run_store_overrides["queued-run"] = mirror
                manager._run_store_override_instances[id(mirror)] = mirror
                manager._run_store_override_run_ids[id(mirror)] = {"queued-run"}
            manager.stop_background_services()

            self.assertIn("queued-run", _SupabaseStubHandler.runs)
            self.assertIn("queued-run", _SupabaseStubHandler.events)
            self.assertEqual(manager._run_store_overrides, {})
            self.assertEqual(manager._run_store_override_instances, {})
            self.assertEqual(manager._run_store_override_run_ids, {})

    def test_multi_agent_mirror_closes_override_store_after_all_runs_terminal(self) -> None:
        with SupabaseStubServer() as url, tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "GRAPH_AGENT_RUN_STORE_MIRROR_FLUSH_INTERVAL_MS": "5000",
                    "GRAPH_AGENT_RUN_STORE_MIRROR_EVENT_BATCH_SIZE": "100",
                    "GRAPH_AGENT_RUN_STORE_MIRROR_RUN_BATCH_SIZE": "25",
                    "GRAPH_AGENT_RUN_STORE_MIRROR_FLUSH_TIMEOUT_SECONDS": "1",
                },
                clear=False,
            ):
                services = build_example_services()
                bundled_path = Path(temp_dir) / "bundled_graphs.json"
                bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
                store = GraphStore(
                    services,
                    path=Path(temp_dir) / "graphs.json",
                    bundled_path=bundled_path,
                )
                graph_payload = build_simple_environment_payload("mirrored-environment")
                graph_payload.setdefault("env_vars", {})
                graph_payload["env_vars"].update(
                    {
                        "GRAPH_AGENT_SUPABASE_ANALYTICS_URL": url,
                        "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY": "sb_secret_analytics",
                    }
                )
                graph_payload["supabase_connections"] = [
                    {
                        "connection_id": "analytics-db",
                        "name": "Analytics DB",
                        "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_URL",
                        "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY",
                    }
                ]
                graph_payload["run_store_supabase_connection_id"] = "analytics-db"
                store.create_graph(graph_payload)

                manager = GraphRunManager(
                    services=services,
                    store=store,
                    run_log_store=RunLogStore(Path(temp_dir) / ".logs" / "runs"),
                )
                try:
                    run_id = manager.start_run("mirrored-environment", "hello")
                    state = wait_for_run_completion(manager, run_id)
                    child_run_ids = sorted(
                        str(agent_state["run_id"])
                        for agent_state in state["agent_runs"].values()
                    )

                    self.assertEqual(state["status"], "completed")
                    self.assertIn(run_id, _SupabaseStubHandler.runs)
                    for child_run_id in child_run_ids:
                        self.assertIn(child_run_id, _SupabaseStubHandler.runs)
                        self.assertIn(child_run_id, _SupabaseStubHandler.events)
                    self.assertEqual(manager._run_store_overrides, {})
                    self.assertEqual(manager._run_store_override_instances, {})
                    self.assertEqual(manager._run_store_override_run_ids, {})
                finally:
                    manager.stop_background_services()

    def test_manager_reconciles_stale_running_run_to_interrupted(self) -> None:
        with SupabaseStubServer() as url, tempfile.TemporaryDirectory() as temp_dir:
            services = build_example_services()
            bundled_path = Path(temp_dir) / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            store = GraphStore(
                services,
                path=Path(temp_dir) / "graphs.json",
                bundled_path=bundled_path,
            )
            run_store = SupabaseRunStore(url=url, service_role_key="test-key")
            state = build_run_state("stale-run", "graph-1", {"prompt": "resume"})
            state = apply_single_run_event(
                state,
                {
                    "event_type": "run.started",
                    "summary": "started",
                    "payload": {},
                    "run_id": "stale-run",
                    "timestamp": "2026-04-02T00:00:00Z",
                    "agent_id": None,
                    "parent_run_id": None,
                },
            )
            state["runtime_instance_id"] = "old-runtime"
            state["last_heartbeat_at"] = "2026-04-02T00:00:01Z"
            run_store.initialize_run(state)
            run_store.append_event("stale-run", state["event_history"][-1])
            run_store.write_state("stale-run", state)

            manager = GraphRunManager(services=services, store=store, run_store=run_store)
            recovered = manager.get_run("stale-run")

            self.assertEqual(recovered["status"], "interrupted")
            self.assertEqual(recovered["status_reason"], "runtime_heartbeat_expired")
            history = manager.list_runs(limit=10)
            stale_row = next(row for row in history if row["run_id"] == "stale-run")
            self.assertEqual(stale_row["status"], "interrupted")

    def test_manager_start_background_services_tolerates_reconcile_failures(self) -> None:
        with SupabaseStubServer() as url, tempfile.TemporaryDirectory() as temp_dir:
            _SupabaseStubHandler.force_runs_html_404 = True
            services = build_example_services()
            bundled_path = Path(temp_dir) / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            store = GraphStore(
                services,
                path=Path(temp_dir) / "graphs.json",
                bundled_path=bundled_path,
            )
            manager = GraphRunManager(
                services=services,
                store=store,
                run_store=SupabaseRunStore(url=url, service_role_key="test-key"),
            )

            manager.start_background_services()
            manager.stop_background_services()

    def test_load_manifest_does_not_request_state_snapshot_column(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            store.initialize_run(build_run_state("run-egress", "graph-egress", {"prompt": "x"}))
            _SupabaseStubHandler.get_queries.clear()

            store.load_manifest("run-egress")

            run_get_queries = [q for path, q in _SupabaseStubHandler.get_queries if path == "/rest/v1/runs"]
            self.assertEqual(len(run_get_queries), 1)
            select_clause = _single_query_value(run_get_queries[0], "select")
            self.assertNotIn(
                "state_snapshot",
                select_clause,
                msg=f"load_manifest must not request state_snapshot; select was {select_clause!r}",
            )
            self.assertNotEqual(select_clause, "*")

    def test_load_state_only_requests_state_snapshot_column(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            store.initialize_run(build_run_state("run-egress", "graph-egress", {"prompt": "x"}))
            _SupabaseStubHandler.get_queries.clear()

            store.load_state("run-egress")

            run_get_queries = [q for path, q in _SupabaseStubHandler.get_queries if path == "/rest/v1/runs"]
            self.assertEqual(len(run_get_queries), 1)
            select_clause = _single_query_value(run_get_queries[0], "select")
            self.assertIn("state_snapshot", select_clause)
            self.assertNotEqual(select_clause, "*")

    def test_recover_run_state_uses_single_run_row_request(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            state = build_run_state("run-egress", "graph-egress", {"prompt": "x"})
            store.initialize_run(state)
            store.append_event(
                "run-egress",
                {
                    "event_type": "run.started",
                    "summary": "started",
                    "payload": {},
                    "run_id": "run-egress",
                    "timestamp": "2026-04-02T00:00:00Z",
                },
            )
            _SupabaseStubHandler.get_queries.clear()

            store.recover_run_state("run-egress")

            run_get_queries = [q for path, q in _SupabaseStubHandler.get_queries if path == "/rest/v1/runs"]
            self.assertEqual(
                len(run_get_queries),
                1,
                msg=f"recover_run_state should fetch the run row exactly once, got {len(run_get_queries)} fetches",
            )
            select_clause = _single_query_value(run_get_queries[0], "select")
            self.assertIn("state_snapshot", select_clause)
            self.assertIn("graph_id", select_clause)

    def test_load_events_since_sequence_filters_by_sequence_number(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            store.initialize_run(build_run_state("run-egress", "graph-egress", {"prompt": "x"}))
            for index in range(3):
                store.append_event(
                    "run-egress",
                    {
                        "event_type": "node.completed",
                        "summary": f"event-{index}",
                        "payload": {"index": index},
                        "run_id": "run-egress",
                        "timestamp": f"2026-04-02T00:00:0{index}Z",
                    },
                )
            _SupabaseStubHandler.get_queries.clear()

            events = store.load_events("run-egress", since_sequence=1)

            self.assertEqual(len(events), 2)
            event_get_queries = [q for path, q in _SupabaseStubHandler.get_queries if path == "/rest/v1/run_events"]
            self.assertEqual(len(event_get_queries), 1)
            sequence_filter = _single_query_value(event_get_queries[0], "sequence_number")
            self.assertEqual(sequence_filter, "gt.1")


class BuildDefaultRunStoreTests(unittest.TestCase):
    def test_supabase_backend_defaults_to_local_primary_with_async_mirror(self) -> None:
        with patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_RUN_STORE": "supabase",
                "GRAPH_AGENT_SUPABASE_URL": "http://127.0.0.1:9",
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "test-secret",
            },
            clear=False,
        ):
            from graph_agent.api.run_log_store import FilesystemRunStore
            from graph_agent.api.run_store import AsyncBatchingRunStoreMirror, CompositeRunStore

            store = build_default_run_store()
            self.assertIsInstance(store, CompositeRunStore)
            self.assertIsInstance(store.primary, FilesystemRunStore)
            self.assertEqual(len(store.mirrors), 1)
            self.assertIsInstance(store.mirrors[0], AsyncBatchingRunStoreMirror)

    def test_supabase_primary_legacy_async_mirror(self) -> None:
        with patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_RUN_STORE": "supabase",
                "GRAPH_AGENT_SUPABASE_URL": "http://127.0.0.1:9",
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "test-secret",
                "GRAPH_AGENT_RUN_STORE_SUPABASE_PRIMARY": "1",
            },
            clear=False,
        ):
            from graph_agent.api.run_store import AsyncBatchingRunStoreMirror

            store = build_default_run_store()
            self.assertIsInstance(store, AsyncBatchingRunStoreMirror)

    def test_supabase_primary_legacy_sync_store_when_mirror_disabled(self) -> None:
        with patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_RUN_STORE": "supabase",
                "GRAPH_AGENT_SUPABASE_URL": "http://127.0.0.1:9",
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "test-secret",
                "GRAPH_AGENT_RUN_STORE_SUPABASE_PRIMARY": "1",
                "GRAPH_AGENT_RUN_STORE_MIRROR_DISABLED": "1",
            },
            clear=False,
        ):
            store = build_default_run_store()
            self.assertIsInstance(store, SupabaseRunStore)

    def test_local_primary_sync_supabase_when_mirror_disabled(self) -> None:
        with patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_RUN_STORE": "supabase",
                "GRAPH_AGENT_SUPABASE_URL": "http://127.0.0.1:9",
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "test-secret",
                "GRAPH_AGENT_RUN_STORE_MIRROR_DISABLED": "1",
            },
            clear=False,
        ):
            from graph_agent.api.run_log_store import FilesystemRunStore
            from graph_agent.api.run_store import CompositeRunStore

            store = build_default_run_store()
            self.assertIsInstance(store, CompositeRunStore)
            self.assertIsInstance(store.primary, FilesystemRunStore)
            self.assertEqual(len(store.mirrors), 1)
            self.assertIsInstance(store.mirrors[0], SupabaseRunStore)


class SupabaseRunStoreTimeoutTests(unittest.TestCase):
    def test_list_runs_times_out_with_concise_runtime_error(self) -> None:
        with SupabaseStubServer() as url:
            store = SupabaseRunStore(url=url, service_role_key="test-key")
            with patch("graph_agent.api.supabase_run_store.urlopen", side_effect=socket.timeout()):
                with self.assertRaises(RuntimeError) as ctx:
                    store.list_runs(limit=1)
                self.assertIn("timed out", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
