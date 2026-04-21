from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.api.supabase_run_store import SupabaseRunStore
from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.providers.base import ModelRequest, ModelResponse, ProviderPreflightResult
from graph_agent.runtime.core import GraphDefinition, GraphValidationError
from graph_agent.runtime.engine import GraphRuntime
from graph_agent.runtime.supabase_data import (
    SupabaseSchemaColumn,
    SupabaseSchemaSource,
    SupabaseRowWriteRequest,
    fetch_supabase_schema_catalog,
    validate_outbound_email_log_schema,
    write_supabase_row,
)


class _SupabaseStubHandler(BaseHTTPRequestHandler):
    last_headers: dict[str, str] = {}
    last_query: dict[str, list[str]] = {}
    last_path: str = ""
    last_json_body: Any = None
    table_rows: dict[str, list[dict[str, Any]]] = {}

    @staticmethod
    def _coerce_comparable(value: Any) -> Any:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
        text = str(value)
        try:
            return int(text)
        except ValueError:
            try:
                return float(text)
            except ValueError:
                return text

    @classmethod
    def _apply_basic_filter(cls, rows: list[dict[str, Any]], key: str, expression: str) -> list[dict[str, Any]]:
        operator, _, expected = expression.partition(".")
        if not operator:
            return rows
        if operator == "eq":
            return [row for row in rows if cls._coerce_comparable(row.get(key)) == cls._coerce_comparable(expected)]
        if operator == "gt":
            return [row for row in rows if cls._coerce_comparable(row.get(key)) > cls._coerce_comparable(expected)]
        return rows

    @classmethod
    def _apply_or_filter(cls, rows: list[dict[str, Any]], expression: str) -> list[dict[str, Any]]:
        normalized = str(expression or "").strip()
        if not normalized.startswith("(") or not normalized.endswith(")"):
            return rows
        inner = normalized[1:-1]
        marker = ",and("
        if marker not in inner:
            return rows
        first_expression, _, remainder = inner.partition(marker)
        remainder = remainder[:-1] if remainder.endswith(")") else remainder
        second_expression, _, third_expression = remainder.partition(",")
        key_one, _, op_one = first_expression.partition(".")
        key_two, _, op_two = second_expression.partition(".")
        key_three, _, op_three = third_expression.partition(".")
        first_rows = cls._apply_basic_filter(rows, key_one, op_one)
        second_rows = cls._apply_basic_filter(rows, key_two, op_two)
        third_rows = cls._apply_basic_filter(second_rows, key_three, op_three)
        seen: set[tuple[tuple[str, Any], ...]] = set()
        merged: list[dict[str, Any]] = []
        for row in [*first_rows, *third_rows]:
            marker_key = tuple(sorted(row.items()))
            if marker_key in seen:
                continue
            seen.add(marker_key)
            merged.append(row)
        return merged

    @classmethod
    def _apply_query(cls, rows: list[dict[str, Any]], parsed: Any) -> list[dict[str, Any]]:
        filtered = [dict(row) for row in rows]
        query = parse_qs(parsed.query)
        for key, values in query.items():
            if key in {"select", "order", "limit", "or"}:
                continue
            for value in values:
                filtered = cls._apply_basic_filter(filtered, key, value)
        for expression in query.get("or", []):
            filtered = cls._apply_or_filter(filtered, expression)
        for order_expression in reversed(query.get("order", [])):
            column_name, _, direction = order_expression.partition(".")
            filtered.sort(
                key=lambda row, column_name=column_name: cls._coerce_comparable(row.get(column_name)),
                reverse=direction.lower() == "desc",
            )
        limit_values = query.get("limit", [])
        if limit_values:
            filtered = filtered[: int(limit_values[0])]
        return filtered

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        type(self).last_headers = {str(key).lower(): str(value) for key, value in self.headers.items()}
        type(self).last_query = parse_qs(parsed.query)
        type(self).last_path = parsed.path
        if parsed.path == "/rest/v1/":
            payload = {
                "openapi": "3.0.0",
                "paths": {
                    "/projects": {
                        "get": {
                            "summary": "Project records",
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "type": "array",
                                                "items": {"$ref": "#/components/schemas/projects"},
                                            }
                                        }
                                    }
                                }
                            },
                        }
                    }
                },
                "components": {
                    "schemas": {
                        "projects": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer", "description": "Project id."},
                                "name": {"type": "string", "description": "Project name."},
                                "status": {"type": "string", "nullable": True, "description": "Lifecycle state."},
                            },
                        }
                    }
                },
            }
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/rest/v1/projects":
            payload = self._apply_query(
                type(self).table_rows.get(
                    "projects",
                    [
                        {"id": 1, "name": "Alpha", "status": "active"},
                        {"id": 2, "name": "Beta", "status": "active"},
                    ],
                ),
                parsed,
            )
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        table_rows = type(self).table_rows.get(parsed.path.removeprefix("/rest/v1/"))
        if table_rows is not None:
            payload = self._apply_query(table_rows, parsed)
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        type(self).last_headers = {str(key).lower(): str(value) for key, value in self.headers.items()}
        type(self).last_query = parse_qs(parsed.query)
        type(self).last_path = parsed.path
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(content_length) if content_length > 0 else b""
        try:
            type(self).last_json_body = json.loads(raw_body.decode("utf-8")) if raw_body else None
        except json.JSONDecodeError:
            type(self).last_json_body = raw_body.decode("utf-8", errors="replace")
        if parsed.path == "/mcp":
            body = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "supabase-stub", "version": "1.0.0"},
                    },
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path.startswith("/rest/v1/") and parsed.path != "/rest/v1/":
            prefer = str(self.headers.get("Prefer", ""))
            if "return=minimal" in prefer:
                self.send_response(201)
                self.end_headers()
                return
            body = json.dumps(type(self).last_json_body).encode("utf-8")
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


class SupabaseStubServer:
    def __enter__(self) -> str:
        _SupabaseStubHandler.last_headers = {}
        _SupabaseStubHandler.last_query = {}
        _SupabaseStubHandler.last_path = ""
        _SupabaseStubHandler.last_json_body = None
        _SupabaseStubHandler.table_rows = {}
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _SupabaseStubHandler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


def supabase_graph_payload(
    graph_id: str = "supabase-data-graph",
    *,
    node_config: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved_node_config: dict[str, object] = {
        "mode": "supabase_data",
        "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
        "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
        "schema": "public",
        "source_kind": "table",
        "source_name": "projects",
        "select": "id,name,status",
        "filters_text": "status=eq.active",
        "order_by": "id",
        "order_desc": False,
        "limit": 2,
        "single_row": False,
        "output_mode": "records",
        "rpc_params_json": "{}",
    }
    if node_config:
        resolved_node_config.update(node_config)
    return {
        "graph_id": graph_id,
        "name": "Supabase Data Graph",
        "description": "",
        "version": "1.0",
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
                "id": "supabase",
                "kind": "data",
                "category": "data",
                "label": "Supabase Data Source",
                "provider_id": "core.supabase_data",
                "provider_label": "Supabase Data Source",
                "config": resolved_node_config,
                "position": {"x": 240, "y": 0},
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {"source_binding": {"type": "latest_payload", "source": "supabase"}},
                "position": {"x": 460, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "supabase", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "supabase", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


def supabase_row_write_graph_payload(
    graph_id: str = "supabase-row-write-graph",
    *,
    node_config: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved_node_config: dict[str, object] = {
        "mode": "supabase_row_write",
        "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
        "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
        "schema": "public",
        "table_name": "audit_logs",
        "write_mode": "insert",
        "on_conflict": "",
        "ignore_duplicates": False,
        "returning": "representation",
        "base_row_json_path": "",
        "column_values_json": json.dumps(
            {
                "email": {"mode": "path", "path": "event.email"},
                "event_type": {"mode": "path", "path": "event.type"},
                "status": {"mode": "literal", "value": "queued"},
                "metadata": {"mode": "path", "path": "event.metadata"},
                "created_at": {"mode": "default"},
            }
        ),
    }
    if node_config:
        resolved_node_config.update(node_config)
    return {
        "graph_id": graph_id,
        "name": "Supabase Row Write Graph",
        "description": "",
        "version": "1.0",
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
                "id": "writer",
                "kind": "data",
                "category": "data",
                "label": "Supabase Row Write",
                "provider_id": "core.supabase_row_write",
                "provider_label": "Supabase Row Write",
                "config": resolved_node_config,
                "position": {"x": 240, "y": 0},
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {"source_binding": {"type": "latest_payload", "source": "writer"}},
                "position": {"x": 460, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "writer", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "writer", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


def supabase_table_rows_graph_payload(
    graph_id: str = "supabase-table-rows-graph",
    *,
    node_config: dict[str, object] | None = None,
    finish_provider_id: str = "core.output",
) -> dict[str, object]:
    resolved_node_config: dict[str, object] = {
        "mode": "supabase_table_rows",
        "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
        "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
        "schema": "public",
        "table_name": "projects",
        "select": "id,name,status,created_at",
        "filters_text": "status=eq.active",
        "cursor_column": "created_at",
        "row_id_column": "id",
        "page_size": 500,
    }
    if node_config:
        resolved_node_config.update(node_config)
    return {
        "graph_id": graph_id,
        "name": "Supabase Table Rows Graph",
        "description": "",
        "version": "1.0",
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
                "id": "iterator",
                "kind": "control_flow_unit",
                "category": "control_flow_unit",
                "label": "Supabase Table Rows",
                "provider_id": "core.supabase_table_rows",
                "provider_label": "Supabase Table Rows",
                "config": resolved_node_config,
                "position": {"x": 220, "y": 0},
            },
            {
                "id": "model",
                "kind": "model",
                "category": "api",
                "label": "Model",
                "provider_id": "core.api",
                "provider_label": "API Call Node",
                "model_provider_name": "spreadsheet_echo",
                "prompt_name": "supabase_table_rows_prompt",
                "config": {
                    "provider_name": "spreadsheet_echo",
                    "prompt_name": "supabase_table_rows_prompt",
                    "system_prompt": "Process the current Supabase row.",
                    "user_message_template": "{input_payload}",
                    "response_mode": "message",
                },
                "position": {"x": 440, "y": 0},
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": finish_provider_id,
                "provider_label": "Finish",
                "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                "position": {"x": 660, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "iterator", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "iterator", "target_id": "model", "label": "", "kind": "standard", "priority": 100},
            {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


class SpreadsheetEchoProvider:
    name = "spreadsheet_echo"

    def __init__(self) -> None:
        self.user_messages: list[str] = []

    def generate(self, request: ModelRequest) -> ModelResponse:
        user_message = request.messages[-1].content if request.messages else ""
        self.user_messages.append(user_message)
        return ModelResponse(
            content=user_message,
            structured_output={
                "message": user_message,
                "need_tool": False,
                "tool_calls": [],
            },
        )

    def preflight(self, provider_config=None) -> ProviderPreflightResult:
        return ProviderPreflightResult(
            status="available",
            ok=True,
            message="Test echo provider is available.",
            details={"backend_type": "test"},
        )


class FailingEchoProvider(SpreadsheetEchoProvider):
    def generate(self, request: ModelRequest) -> ModelResponse:
        user_message = request.messages[-1].content if request.messages else ""
        self.user_messages.append(user_message)
        raise RuntimeError("Intentional provider failure")


class SupabaseDataNodeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def _runtime(self) -> GraphRuntime:
        return GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

    def test_supabase_provider_appears_in_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GraphStore(self.services, path=Path(directory) / "graphs.json")
            catalog = store.catalog()

        provider = next(candidate for candidate in catalog["node_providers"] if candidate["provider_id"] == "core.supabase_data")
        self.assertEqual(provider["default_config"]["mode"], "supabase_data")
        self.assertEqual(provider["default_config"]["source_kind"], "table")
        self.assertEqual(provider["default_config"]["output_mode"], "records")

    def test_supabase_row_write_provider_appears_in_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GraphStore(self.services, path=Path(directory) / "graphs.json")
            catalog = store.catalog()

        provider = next(candidate for candidate in catalog["node_providers"] if candidate["provider_id"] == "core.supabase_row_write")
        self.assertEqual(provider["default_config"]["mode"], "supabase_row_write")
        self.assertEqual(provider["default_config"]["write_mode"], "insert")
        self.assertEqual(provider["default_config"]["returning"], "representation")

    def test_supabase_table_rows_provider_appears_in_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GraphStore(self.services, path=Path(directory) / "graphs.json")
            catalog = store.catalog()

        provider = next(candidate for candidate in catalog["node_providers"] if candidate["provider_id"] == "core.supabase_table_rows")
        self.assertEqual(provider["default_config"]["mode"], "supabase_table_rows")
        self.assertEqual(provider["default_config"]["row_id_column"], "id")
        self.assertEqual(provider["default_config"]["page_size"], 500)

    def test_supabase_data_node_fetches_rows_and_emits_data_envelope(self) -> None:
        graph = GraphDefinition.from_dict(supabase_graph_payload())
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with SupabaseStubServer() as base_url, patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": base_url,
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            state = runtime.run(graph, {"request": "load projects"}, run_id="run-supabase-data")

        self.assertEqual(state.status, "completed")
        self.assertEqual(
            state.final_output,
            [
                {"id": 1, "name": "Alpha", "status": "active"},
                {"id": 2, "name": "Beta", "status": "active"},
            ],
        )
        self.assertEqual(state.node_outputs["supabase"]["metadata"]["data_mode"], "supabase_data")
        self.assertEqual(state.node_outputs["supabase"]["metadata"]["row_count"], 2)
        self.assertEqual(_SupabaseStubHandler.last_path, "/rest/v1/projects")
        self.assertEqual(_SupabaseStubHandler.last_query.get("select"), ["id,name,status"])
        self.assertEqual(_SupabaseStubHandler.last_query.get("status"), ["eq.active"])
        self.assertEqual(_SupabaseStubHandler.last_query.get("order"), ["id.asc"])
        self.assertEqual(_SupabaseStubHandler.last_query.get("limit"), ["2"])
        self.assertEqual(_SupabaseStubHandler.last_headers.get("apikey"), "service-role-key")
        self.assertEqual(_SupabaseStubHandler.last_headers.get("authorization"), "Bearer service-role-key")
        self.assertEqual(_SupabaseStubHandler.last_headers.get("accept-profile"), "public")

    def test_missing_supabase_env_vars_fail_cleanly(self) -> None:
        graph = GraphDefinition.from_dict(supabase_graph_payload("supabase-data-missing-env"))
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with patch.dict(os.environ, {"GRAPH_AGENT_SUPABASE_URL": "", "GRAPH_AGENT_SUPABASE_SECRET_KEY": ""}, clear=False):
            state = runtime.run(graph, {"request": "load projects"}, run_id="run-supabase-missing-env")

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "missing_supabase_url")

    def test_supabase_data_node_can_exist_without_source_name_during_editing(self) -> None:
        graph = GraphDefinition.from_dict(
            supabase_graph_payload(
                "supabase-data-missing-source-name",
                node_config={"source_name": ""},
            )
        )
        graph.validate_against_services(self.services)

    def test_manager_can_preview_supabase_schema_catalog(self) -> None:
        with SupabaseStubServer() as base_url, patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": base_url,
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            from graph_agent.api.manager import GraphRunManager

            manager = GraphRunManager(services=self.services)
            result = manager.preview_supabase_schema(
                {
                    "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
                    "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
                    "schema": "public",
                    "graph_env_vars": {},
                }
            )

        self.assertEqual(result["schema"], "public")
        self.assertEqual(result["source_count"], 1)
        self.assertEqual(result["sources"][0]["name"], "projects")
        self.assertEqual([column["name"] for column in result["sources"][0]["columns"]], ["id", "name", "status"])
        self.assertEqual(_SupabaseStubHandler.last_path, "/rest/v1/")
        self.assertEqual(_SupabaseStubHandler.last_headers.get("accept"), "application/openapi+json")

    def test_manager_can_verify_supabase_auth(self) -> None:
        with SupabaseStubServer() as base_url:
            from graph_agent.api.manager import GraphRunManager

            manager = GraphRunManager(services=self.services)
            result = manager.verify_supabase_auth(
                {
                    "supabase_url": base_url,
                    "supabase_key": "service-role-key",
                    "schema": "public",
                    "project_ref": "project-123",
                    "access_token": "access-token-xyz",
                    "mcp_base_url": f"{base_url}/mcp",
                }
            )

        self.assertTrue(result["static_auth_valid"])
        self.assertTrue(result["mcp_auth_checked"])
        self.assertTrue(result["mcp_auth_valid"])
        self.assertEqual(result["source_count"], 1)
        self.assertEqual(result["sources"][0]["name"], "projects")
        self.assertEqual(result["mcp_server"]["server_name"], "supabase-stub")
        self.assertEqual(_SupabaseStubHandler.last_path, "/mcp")
        self.assertEqual(_SupabaseStubHandler.last_headers.get("authorization"), "Bearer access-token-xyz")
        self.assertEqual(_SupabaseStubHandler.last_headers.get("accept"), "application/json, text/event-stream")

    def test_supabase_data_node_uses_graph_env_literal_values(self) -> None:
        graph_payload = supabase_graph_payload("supabase-data-graph-env-literals")
        graph_payload["env_vars"] = {
            "GRAPH_AGENT_SUPABASE_URL": "",
            "GRAPH_AGENT_SUPABASE_SECRET_KEY": "",
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with SupabaseStubServer() as base_url:
            graph.env_vars["GRAPH_AGENT_SUPABASE_URL"] = base_url
            graph.env_vars["GRAPH_AGENT_SUPABASE_SECRET_KEY"] = "service-role-key"
            with patch.dict(os.environ, {"GRAPH_AGENT_SUPABASE_URL": "", "GRAPH_AGENT_SUPABASE_SECRET_KEY": ""}, clear=False):
                state = runtime.run(graph, {"request": "load projects"}, run_id="run-supabase-graph-env-literals")

        self.assertEqual(state.status, "completed")

    def test_manager_can_inspect_supabase_runtime_status(self) -> None:
        from graph_agent.api.manager import GraphRunManager

        manager = GraphRunManager(services=self.services)
        with patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": "http://127.0.0.1:9999",
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            result = manager.inspect_supabase_runtime(
                {
                    "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
                    "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
                    "graph_env_vars": {},
                }
            )

        self.assertTrue(result["ready"])
        self.assertTrue(result["supabase_url_env_present"])
        self.assertTrue(result["supabase_key_env_present"])
        self.assertEqual(result["missing_env_vars"], [])

    def test_manager_reports_missing_supabase_runtime_status(self) -> None:
        from graph_agent.api.manager import GraphRunManager

        manager = GraphRunManager(services=self.services)
        with patch.dict(os.environ, {"GRAPH_AGENT_SUPABASE_URL": "", "GRAPH_AGENT_SUPABASE_SECRET_KEY": ""}, clear=False):
            result = manager.inspect_supabase_runtime(
                {
                    "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
                    "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
                    "graph_env_vars": {},
                }
            )

        self.assertFalse(result["ready"])
        self.assertFalse(result["supabase_url_env_present"])
        self.assertFalse(result["supabase_key_env_present"])
        self.assertEqual(result["missing_env_vars"], ["GRAPH_AGENT_SUPABASE_URL", "GRAPH_AGENT_SUPABASE_SECRET_KEY"])

    def test_invalid_rpc_json_fails_cleanly(self) -> None:
        graph = GraphDefinition.from_dict(
            supabase_graph_payload(
                "supabase-data-invalid-rpc-json",
                node_config={"rpc_params_json": "{not-json}"},
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": "http://127.0.0.1:9999",
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            state = runtime.run(graph, {"request": "load projects"}, run_id="run-supabase-invalid-rpc-json")

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "invalid_supabase_rpc_params")

    def test_supabase_row_write_node_writes_runtime_row_and_preserves_defaults(self) -> None:
        graph = GraphDefinition.from_dict(supabase_row_write_graph_payload())
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with SupabaseStubServer() as base_url, patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": base_url,
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            state = runtime.run(
                graph,
                {
                    "event": {
                        "email": "person@example.com",
                        "type": "signup",
                        "metadata": {"plan": "pro"},
                    }
                },
                run_id="run-supabase-row-write",
            )

        self.assertEqual(state.status, "completed")
        self.assertEqual(
            state.final_output,
            {
                "email": "person@example.com",
                "event_type": "signup",
                "status": "queued",
                "metadata": {"plan": "pro"},
            },
        )
        self.assertEqual(state.node_outputs["writer"]["metadata"]["data_mode"], "supabase_row_write")
        self.assertEqual(state.node_outputs["writer"]["metadata"]["table_name"], "audit_logs")
        self.assertEqual(_SupabaseStubHandler.last_path, "/rest/v1/audit_logs")
        self.assertEqual(
            _SupabaseStubHandler.last_json_body,
            {
                "email": "person@example.com",
                "event_type": "signup",
                "status": "queued",
                "metadata": {"plan": "pro"},
            },
        )
        self.assertNotIn("created_at", _SupabaseStubHandler.last_json_body)
        self.assertEqual(_SupabaseStubHandler.last_headers.get("prefer"), "return=representation")

    def test_supabase_row_write_node_can_exist_without_table_name_during_editing(self) -> None:
        graph = GraphDefinition.from_dict(
            supabase_row_write_graph_payload(
                "supabase-row-write-missing-table-name",
                node_config={"table_name": ""},
            )
        )
        graph.validate_against_services(self.services)

    def test_invalid_row_mapping_json_fails_cleanly(self) -> None:
        graph = GraphDefinition.from_dict(
            supabase_row_write_graph_payload(
                "supabase-row-write-invalid-mapping",
                node_config={"column_values_json": "{not-json}"},
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": "http://127.0.0.1:9999",
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            state = runtime.run(
                graph,
                {"event": {"email": "person@example.com", "type": "signup"}},
                run_id="run-supabase-row-write-invalid-mapping",
        )

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "invalid_supabase_row_mapping")

    def test_outbound_email_log_schema_validation_reports_required_missing_columns(self) -> None:
        result = validate_outbound_email_log_schema(
            sources=[
                SupabaseSchemaSource(
                    name="outbound_email_messages",
                    source_kind="table",
                    description="Outbound drafts",
                    columns=[
                        SupabaseSchemaColumn(name="provider", data_type="string", nullable=False, description=""),
                        SupabaseSchemaColumn(name="mailbox_account", data_type="string", nullable=False, description=""),
                        SupabaseSchemaColumn(name="subject", data_type="string", nullable=False, description=""),
                        SupabaseSchemaColumn(name="body_text", data_type="string", nullable=False, description=""),
                        SupabaseSchemaColumn(name="message_type", data_type="string", nullable=False, description=""),
                        SupabaseSchemaColumn(name="outreach_step", data_type="integer", nullable=False, description=""),
                        SupabaseSchemaColumn(name="sales_approach", data_type="string", nullable=False, description=""),
                        SupabaseSchemaColumn(name="provider_draft_id", data_type="string", nullable=True, description=""),
                        SupabaseSchemaColumn(name="provider_message_id", data_type="string", nullable=True, description=""),
                        SupabaseSchemaColumn(name="drafted_at", data_type="string", nullable=False, description=""),
                        SupabaseSchemaColumn(name="metadata", data_type="object", nullable=False, description=""),
                        SupabaseSchemaColumn(name="raw_provider_payload", data_type="object", nullable=False, description=""),
                    ],
                )
            ],
            schema="public",
            table_name="outbound_email_messages",
        )

        self.assertFalse(result.valid)
        self.assertEqual(result.table_name, "outbound_email_messages")
        self.assertEqual(result.missing_required_columns, ["recipient_email"])

    def test_outbound_email_log_schema_validation_accepts_minimal_logger_table(self) -> None:
        result = validate_outbound_email_log_schema(
            sources=[
                SupabaseSchemaSource(
                    name="outbound_email_messages",
                    source_kind="table",
                    description="Outbound drafts",
                    columns=[
                        SupabaseSchemaColumn(name="recipient_email", data_type="text", nullable=False, description=""),
                    ],
                )
            ],
            schema="public",
            table_name="outbound_email_messages",
        )

        self.assertTrue(result.valid)
        self.assertEqual(result.missing_required_columns, [])
        self.assertIn("provider", result.missing_optional_columns)
        self.assertIn("drafted_at", result.missing_optional_columns)

    def test_outbound_email_log_schema_validation_accepts_postgres_type_aliases(self) -> None:
        result = validate_outbound_email_log_schema(
            sources=[
                SupabaseSchemaSource(
                    name="outbound_email_messages",
                    source_kind="table",
                    description="Outbound drafts",
                    columns=[
                        SupabaseSchemaColumn(name="provider", data_type="text", nullable=False, description=""),
                        SupabaseSchemaColumn(name="mailbox_account", data_type="text", nullable=False, description=""),
                        SupabaseSchemaColumn(name="recipient_email", data_type="text", nullable=False, description=""),
                        SupabaseSchemaColumn(name="subject", data_type="text", nullable=False, description=""),
                        SupabaseSchemaColumn(name="body_text", data_type="text", nullable=False, description=""),
                        SupabaseSchemaColumn(name="message_type", data_type="text", nullable=False, description=""),
                        SupabaseSchemaColumn(name="outreach_step", data_type="integer", nullable=False, description=""),
                        SupabaseSchemaColumn(name="sales_approach", data_type="text", nullable=False, description=""),
                        SupabaseSchemaColumn(name="provider_draft_id", data_type="text", nullable=True, description=""),
                        SupabaseSchemaColumn(name="provider_message_id", data_type="text", nullable=True, description=""),
                        SupabaseSchemaColumn(name="internet_message_id", data_type="text", nullable=True, description=""),
                        SupabaseSchemaColumn(name="conversation_id", data_type="text", nullable=True, description=""),
                        SupabaseSchemaColumn(name="drafted_at", data_type="timestamp with time zone", nullable=False, description=""),
                        SupabaseSchemaColumn(name="metadata", data_type="jsonb", nullable=False, description=""),
                        SupabaseSchemaColumn(name="raw_provider_payload", data_type="jsonb", nullable=False, description=""),
                        SupabaseSchemaColumn(name="source_run_id", data_type="text", nullable=True, description=""),
                        SupabaseSchemaColumn(name="sales_approach_version", data_type="text", nullable=True, description=""),
                        SupabaseSchemaColumn(name="parent_outbound_email_id", data_type="uuid", nullable=True, description=""),
                        SupabaseSchemaColumn(name="root_outbound_email_id", data_type="uuid", nullable=True, description=""),
                        SupabaseSchemaColumn(name="observed_sent_at", data_type="timestamp with time zone", nullable=True, description=""),
                        SupabaseSchemaColumn(name="created_at", data_type="timestamp with time zone", nullable=True, description=""),
                    ],
                )
            ],
            schema="public",
            table_name="outbound_email_messages",
        )

        self.assertTrue(result.valid)
        self.assertEqual(result.type_mismatches, [])
        self.assertEqual(result.missing_required_columns, [])

    def test_outbound_email_log_schema_validation_accepts_openapi_decorated_type_labels(self) -> None:
        result = validate_outbound_email_log_schema(
            sources=[
                SupabaseSchemaSource(
                    name="outbound_email_messages",
                    source_kind="table",
                    description="Outbound drafts",
                    columns=[
                        SupabaseSchemaColumn(name="provider", data_type="string (text)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="mailbox_account", data_type="string (text)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="recipient_email", data_type="string (text)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="subject", data_type="string (text)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="body_text", data_type="string (text)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="message_type", data_type="string (text)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="outreach_step", data_type="integer (integer)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="sales_approach", data_type="string (text)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="provider_draft_id", data_type="string (text)", nullable=True, description=""),
                        SupabaseSchemaColumn(name="provider_message_id", data_type="string (text)", nullable=True, description=""),
                        SupabaseSchemaColumn(name="internet_message_id", data_type="string (text)", nullable=True, description=""),
                        SupabaseSchemaColumn(name="conversation_id", data_type="string (text)", nullable=True, description=""),
                        SupabaseSchemaColumn(name="drafted_at", data_type="string (timestamp with time zone)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="metadata", data_type="unknown (jsonb)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="raw_provider_payload", data_type="unknown (jsonb)", nullable=False, description=""),
                        SupabaseSchemaColumn(name="source_run_id", data_type="string (text)", nullable=True, description=""),
                        SupabaseSchemaColumn(name="sales_approach_version", data_type="string (text)", nullable=True, description=""),
                        SupabaseSchemaColumn(name="parent_outbound_email_id", data_type="string (uuid)", nullable=True, description=""),
                        SupabaseSchemaColumn(name="root_outbound_email_id", data_type="string (uuid)", nullable=True, description=""),
                        SupabaseSchemaColumn(name="observed_sent_at", data_type="string (timestamp with time zone)", nullable=True, description=""),
                        SupabaseSchemaColumn(name="created_at", data_type="string (timestamp with time zone)", nullable=True, description=""),
                    ],
                )
            ],
            schema="public",
            table_name="outbound_email_messages",
        )

        self.assertTrue(result.valid)
        self.assertEqual(result.type_mismatches, [])
        self.assertEqual(result.missing_required_columns, [])

    def test_supabase_connection_id_uses_named_graph_connection(self) -> None:
        with SupabaseStubServer() as base_url:
            graph = GraphDefinition.from_dict(
                supabase_graph_payload(
                    "supabase-data-connection-id",
                    node_config={
                        "supabase_connection_id": "analytics-db",
                    },
                )
                | {
                    "env_vars": {
                        "GRAPH_AGENT_SUPABASE_ANALYTICS_URL": base_url,
                        "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY": "analytics-secret",
                    },
                    "supabase_connections": [
                        {
                            "connection_id": "analytics-db",
                            "name": "Analytics DB",
                            "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_URL",
                            "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY",
                            "project_ref_env_var": "SUPABASE_ANALYTICS_PROJECT_REF",
                            "access_token_env_var": "SUPABASE_ANALYTICS_ACCESS_TOKEN",
                        }
                    ],
                    "default_supabase_connection_id": "analytics-db",
                },
            )
            runtime = self._runtime()

            state = runtime.run(
                graph,
                {"event": {"email": "person@example.com", "type": "signup"}},
                run_id="run-supabase-connection-id",
            )

        self.assertEqual(state.status, "completed")
        self.assertEqual(_SupabaseStubHandler.last_headers.get("apikey"), "analytics-secret")

    def test_supabase_connection_id_validation_rejects_missing_connection(self) -> None:
        with self.assertRaisesRegex(GraphValidationError, "unknown Supabase connection 'missing-db'"):
            GraphDefinition.from_dict(
                supabase_graph_payload(
                    "supabase-data-missing-connection",
                    node_config={"supabase_connection_id": "missing-db"},
                )
            )

    def test_supabase_table_rows_connection_id_validation_rejects_missing_connection(self) -> None:
        with self.assertRaisesRegex(GraphValidationError, "unknown Supabase connection 'missing-db'"):
            GraphDefinition.from_dict(
                supabase_table_rows_graph_payload(
                    "supabase-table-rows-missing-connection",
                    node_config={"supabase_connection_id": "missing-db"},
                )
            )

    def test_supabase_connection_id_rejects_registry_rows_dropped_by_normalization(self) -> None:
        with self.assertRaisesRegex(GraphValidationError, "unknown Supabase connection 'analytics-db'"):
            GraphDefinition.from_dict(
                supabase_graph_payload(
                    "supabase-data-invalid-connection-row",
                    node_config={"supabase_connection_id": "analytics-db"},
                )
                | {
                    "supabase_connections": [
                        {
                            "connection_id": "analytics-db",
                            "name": "",
                            "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_URL",
                            "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY",
                            "project_ref_env_var": "SUPABASE_ANALYTICS_PROJECT_REF",
                            "access_token_env_var": "SUPABASE_ANALYTICS_ACCESS_TOKEN",
                        }
                    ],
                }
            )

    def test_default_supabase_connection_id_validation_rejects_stale_default(self) -> None:
        with self.assertRaisesRegex(GraphValidationError, "Unknown default Supabase connection 'missing-db'"):
            GraphDefinition.from_dict(
                supabase_graph_payload("supabase-data-stale-default")
                | {
                    "supabase_connections": [
                        {
                            "connection_id": "analytics-db",
                            "name": "Analytics DB",
                            "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_URL",
                            "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY",
                            "project_ref_env_var": "SUPABASE_ANALYTICS_PROJECT_REF",
                            "access_token_env_var": "SUPABASE_ANALYTICS_ACCESS_TOKEN",
                        }
                    ],
                    "default_supabase_connection_id": "missing-db",
                }
            )

    def test_supabase_table_rows_runtime_processes_rows_in_cursor_order_and_persists_watermark(self) -> None:
        provider = SpreadsheetEchoProvider()
        self.services.model_providers["spreadsheet_echo"] = provider
        graph = GraphDefinition.from_dict(
            supabase_table_rows_graph_payload(
                "supabase-table-rows-runtime",
                node_config={"page_size": 1},
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with tempfile.TemporaryDirectory() as temp_dir:
            cursor_db_path = Path(temp_dir) / "supabase-table-rows.sqlite3"
            with SupabaseStubServer() as base_url, patch.dict(
                os.environ,
                {
                    "GRAPH_AGENT_SUPABASE_URL": base_url,
                    "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
                    "GRAPH_AGENT_SUPABASE_TABLE_ROWS_DB": str(cursor_db_path),
                },
                clear=False,
            ):
                _SupabaseStubHandler.table_rows["projects"] = [
                    {"id": 1, "name": "Alpha", "status": "active", "created_at": "2026-04-10T12:00:00Z"},
                    {"id": 2, "name": "Beta", "status": "active", "created_at": "2026-04-10T12:00:00Z"},
                    {"id": 3, "name": "Gamma", "status": "active", "created_at": "2026-04-11T09:30:00Z"},
                ]
                first_state = runtime.run(graph, {"request": "process unread rows"}, run_id="run-supabase-table-rows-1")
                second_state = runtime.run(graph, {"request": "process unread rows"}, run_id="run-supabase-table-rows-2")

        self.assertEqual(first_state.status, "completed")
        self.assertEqual(second_state.status, "completed")
        self.assertEqual(len(provider.user_messages), 3)
        self.assertIn('"id": 1', provider.user_messages[0])
        self.assertIn('"id": 2', provider.user_messages[1])
        self.assertIn('"id": 3', provider.user_messages[2])
        self.assertEqual(first_state.iterator_states["iterator"]["iterator_type"], "supabase_table_rows")
        self.assertEqual(first_state.iterator_states["iterator"]["current_row_index"], 3)
        self.assertEqual(first_state.iterator_states["iterator"]["total_rows"], 3)
        self.assertEqual(first_state.iterator_states["iterator"]["table_name"], "projects")
        self.assertEqual(second_state.visit_counts.get("model", 0), 0)
        self.assertEqual(second_state.iterator_states["iterator"]["total_rows"], 0)
        self.assertEqual(second_state.iterator_states["iterator"]["last_cached_cursor_value"], "2026-04-11T09:30:00Z")
        self.assertEqual(second_state.iterator_states["iterator"]["last_cached_row_id"], "3")

    def test_supabase_table_rows_missing_cursor_column_fails_cleanly(self) -> None:
        provider = SpreadsheetEchoProvider()
        self.services.model_providers["spreadsheet_echo"] = provider
        graph = GraphDefinition.from_dict(
            supabase_table_rows_graph_payload(
                "supabase-table-rows-missing-cursor-column",
                node_config={"cursor_column": "processed_at"},
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with tempfile.TemporaryDirectory() as temp_dir:
            cursor_db_path = Path(temp_dir) / "supabase-table-rows.sqlite3"
            with SupabaseStubServer() as base_url, patch.dict(
                os.environ,
                {
                    "GRAPH_AGENT_SUPABASE_URL": base_url,
                    "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
                    "GRAPH_AGENT_SUPABASE_TABLE_ROWS_DB": str(cursor_db_path),
                },
                clear=False,
            ):
                _SupabaseStubHandler.table_rows["projects"] = [
                    {"id": 1, "name": "Alpha", "status": "active", "created_at": "2026-04-10T12:00:00Z"},
                ]
                state = runtime.run(graph, {"request": "process unread rows"}, run_id="run-supabase-table-rows-missing-cursor")

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "missing_supabase_iterator_field")
        self.assertEqual(state.terminal_error["field_name"], "processed_at")

    def test_supabase_table_rows_failure_does_not_advance_watermark(self) -> None:
        provider = FailingEchoProvider()
        self.services.model_providers["spreadsheet_echo"] = provider
        graph = GraphDefinition.from_dict(supabase_table_rows_graph_payload("supabase-table-rows-failure"))
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with tempfile.TemporaryDirectory() as temp_dir:
            cursor_db_path = Path(temp_dir) / "supabase-table-rows.sqlite3"
            with SupabaseStubServer() as base_url, patch.dict(
                os.environ,
                {
                    "GRAPH_AGENT_SUPABASE_URL": base_url,
                    "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
                    "GRAPH_AGENT_SUPABASE_TABLE_ROWS_DB": str(cursor_db_path),
                },
                clear=False,
            ):
                _SupabaseStubHandler.table_rows["projects"] = [
                    {"id": 1, "name": "Alpha", "status": "active", "created_at": "2026-04-10T12:00:00Z"},
                ]
                first_state = runtime.run(graph, {"request": "process unread rows"}, run_id="run-supabase-table-rows-failure-1")
            self.services.model_providers["spreadsheet_echo"] = SpreadsheetEchoProvider()
            runtime = self._runtime()
            with SupabaseStubServer() as base_url, patch.dict(
                os.environ,
                {
                    "GRAPH_AGENT_SUPABASE_URL": base_url,
                    "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
                    "GRAPH_AGENT_SUPABASE_TABLE_ROWS_DB": str(cursor_db_path),
                },
                clear=False,
            ):
                _SupabaseStubHandler.table_rows["projects"] = [
                    {"id": 1, "name": "Alpha", "status": "active", "created_at": "2026-04-10T12:00:00Z"},
                ]
                second_state = runtime.run(graph, {"request": "process unread rows"}, run_id="run-supabase-table-rows-failure-2")

        self.assertEqual(first_state.status, "failed")
        self.assertEqual(second_state.status, "completed")
        self.assertEqual(second_state.visit_counts.get("model"), 1)
        self.assertEqual(second_state.iterator_states["iterator"]["current_row_index"], 1)

    def test_supabase_table_rows_terminated_run_does_not_advance_watermark(self) -> None:
        provider = SpreadsheetEchoProvider()
        self.services.model_providers["spreadsheet_echo"] = provider
        graph = GraphDefinition.from_dict(
            supabase_table_rows_graph_payload(
                "supabase-table-rows-terminated",
                finish_provider_id="end.agent_run",
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        with tempfile.TemporaryDirectory() as temp_dir:
            cursor_db_path = Path(temp_dir) / "supabase-table-rows.sqlite3"
            with SupabaseStubServer() as base_url, patch.dict(
                os.environ,
                {
                    "GRAPH_AGENT_SUPABASE_URL": base_url,
                    "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
                    "GRAPH_AGENT_SUPABASE_TABLE_ROWS_DB": str(cursor_db_path),
                },
                clear=False,
            ):
                _SupabaseStubHandler.table_rows["projects"] = [
                    {"id": 1, "name": "Alpha", "status": "active", "created_at": "2026-04-10T12:00:00Z"},
                    {"id": 2, "name": "Beta", "status": "active", "created_at": "2026-04-11T09:30:00Z"},
                ]
                first_state = runtime.run(graph, {"request": "process unread rows"}, run_id="run-supabase-table-rows-terminated-1")
                second_state = runtime.run(graph, {"request": "process unread rows"}, run_id="run-supabase-table-rows-terminated-2")

        self.assertEqual(first_state.status, "completed")
        self.assertEqual(first_state.iterator_states["iterator"]["status"], "terminated")
        self.assertEqual(second_state.status, "completed")
        self.assertEqual(second_state.visit_counts.get("model"), 1)
        self.assertEqual(second_state.iterator_states["iterator"]["status"], "terminated")

    def test_supabase_table_rows_scope_changes_when_filters_change(self) -> None:
        provider = SpreadsheetEchoProvider()
        self.services.model_providers["spreadsheet_echo"] = provider
        runtime = self._runtime()

        with tempfile.TemporaryDirectory() as temp_dir:
            cursor_db_path = Path(temp_dir) / "supabase-table-rows.sqlite3"
            first_graph = GraphDefinition.from_dict(
                supabase_table_rows_graph_payload(
                    "supabase-table-rows-scope-one",
                    node_config={"filters_text": "status=eq.active"},
                )
            )
            second_graph = GraphDefinition.from_dict(
                supabase_table_rows_graph_payload(
                    "supabase-table-rows-scope-two",
                    node_config={"filters_text": "status=eq.paused"},
                )
            )
            first_graph.validate_against_services(self.services)
            second_graph.validate_against_services(self.services)
            with SupabaseStubServer() as base_url, patch.dict(
                os.environ,
                {
                    "GRAPH_AGENT_SUPABASE_URL": base_url,
                    "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
                    "GRAPH_AGENT_SUPABASE_TABLE_ROWS_DB": str(cursor_db_path),
                },
                clear=False,
            ):
                _SupabaseStubHandler.table_rows["projects"] = [
                    {"id": 1, "name": "Alpha", "status": "active", "created_at": "2026-04-10T12:00:00Z"},
                    {"id": 2, "name": "Beta", "status": "paused", "created_at": "2026-04-11T09:30:00Z"},
                ]
                first_state = runtime.run(first_graph, {"request": "process active rows"}, run_id="run-supabase-table-rows-scope-1")
                second_state = runtime.run(second_graph, {"request": "process paused rows"}, run_id="run-supabase-table-rows-scope-2")

        self.assertEqual(first_state.status, "completed")
        self.assertEqual(second_state.status, "completed")
        self.assertEqual(first_state.visit_counts.get("model"), 1)
        self.assertEqual(second_state.visit_counts.get("model"), 1)

    def test_supabase_secret_key_omits_authorization_header(self) -> None:
        with SupabaseStubServer() as base_url:
            fetch_supabase_schema_catalog(
                supabase_url=base_url,
                supabase_key="sb_secret_example",
                schema="public",
            )
            self.assertEqual(_SupabaseStubHandler.last_headers.get("apikey"), "sb_secret_example")
            self.assertNotIn("authorization", _SupabaseStubHandler.last_headers)

            write_supabase_row(
                SupabaseRowWriteRequest(
                    supabase_url=base_url,
                    supabase_key="sb_secret_example",
                    schema="public",
                    table_name="projects",
                    row={"id": 1, "name": "Alpha"},
                )
            )
            self.assertEqual(_SupabaseStubHandler.last_headers.get("apikey"), "sb_secret_example")
            self.assertNotIn("authorization", _SupabaseStubHandler.last_headers)

    def test_supabase_run_store_can_select_custom_env_vars(self) -> None:
        with patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_RUN_STORE_SUPABASE_URL_ENV_VAR": "CUSTOM_SUPABASE_URL",
                "GRAPH_AGENT_RUN_STORE_SUPABASE_SECRET_KEY_ENV_VAR": "CUSTOM_SUPABASE_SECRET_KEY",
                "CUSTOM_SUPABASE_URL": "https://db.example.supabase.co",
                "CUSTOM_SUPABASE_SECRET_KEY": "sb_secret_custom",
                "GRAPH_AGENT_SUPABASE_URL": "https://default.example.supabase.co",
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "default-secret",
            },
            clear=False,
        ):
            store = SupabaseRunStore.from_env()

        self.assertEqual(store.url, "https://db.example.supabase.co")
        self.assertEqual(store.service_role_key, "sb_secret_custom")


if __name__ == "__main__":
    unittest.main()
