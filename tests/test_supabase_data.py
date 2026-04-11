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
from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.runtime.core import GraphDefinition
from graph_agent.runtime.engine import GraphRuntime


class _SupabaseStubHandler(BaseHTTPRequestHandler):
    last_headers: dict[str, str] = {}
    last_query: dict[str, list[str]] = {}
    last_path: str = ""

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
            payload = [
                {"id": 1, "name": "Alpha", "status": "active"},
                {"id": 2, "name": "Beta", "status": "active"},
            ]
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
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


class SupabaseStubServer:
    def __enter__(self) -> str:
        _SupabaseStubHandler.last_headers = {}
        _SupabaseStubHandler.last_query = {}
        _SupabaseStubHandler.last_path = ""
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


if __name__ == "__main__":
    unittest.main()
