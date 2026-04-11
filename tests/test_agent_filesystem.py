from __future__ import annotations

import csv
import importlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.api.manager import GraphRunManager
from graph_agent.api.run_log_store import RunLogStore
from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.providers.base import ModelRequest, ModelResponse, ProviderPreflightResult
from graph_agent.runtime.agent_filesystem import (
    AgentFilesystemError,
    normalize_workspace_relative_path,
    resolve_agent_workspace_path,
)
from graph_agent.runtime.core import GraphDefinition
from graph_agent.runtime.engine import GraphRuntime


class FileWriterProvider:
    name = "file_writer_test"

    def generate(self, request: ModelRequest) -> ModelResponse:
        return ModelResponse(
            content="Workspace output from the model.",
            structured_output={
                "message": "Workspace output from the model.",
                "need_tool": False,
                "tool_calls": [],
            },
        )

    def preflight(self, provider_config=None) -> ProviderPreflightResult:
        return ProviderPreflightResult(
            status="available",
            ok=True,
            message="Test provider is available.",
            details={"backend_type": "test"},
        )


class StaticFileWriterProvider(FileWriterProvider):
    def __init__(self, content: str) -> None:
        self.content = content

    def generate(self, request: ModelRequest) -> ModelResponse:
        return ModelResponse(
            content=self.content,
            structured_output={
                "message": self.content,
                "need_tool": False,
                "tool_calls": [],
            },
        )


def wait_for_run_completion(manager: GraphRunManager, run_id: str, timeout_seconds: float = 5.0) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        state = manager.get_run(run_id)
        if state["status"] in {"completed", "failed"}:
            return state
        time.sleep(0.05)
    raise AssertionError(f"Run '{run_id}' did not finish within {timeout_seconds} seconds.")


def writer_graph_payload(
    graph_id: str = "writer-graph",
    *,
    writer_config: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved_writer_config: dict[str, object] = {
        "mode": "write_text_file",
        "relative_path": "outputs/result.txt",
    }
    if writer_config:
        resolved_writer_config.update(writer_config)
    return {
        "graph_id": graph_id,
        "name": "Writer Graph",
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
                "id": "model",
                "kind": "model",
                "category": "api",
                "label": "Model",
                "provider_id": "core.api",
                "provider_label": "API Call Node",
                "model_provider_name": "file_writer_test",
                "prompt_name": "writer_prompt",
                "config": {
                    "provider_name": "file_writer_test",
                    "prompt_name": "writer_prompt",
                    "system_prompt": "Produce a short file body.",
                    "user_message_template": "{input_payload}",
                    "response_mode": "message",
                },
                "position": {"x": 180, "y": 0},
            },
            {
                "id": "writer",
                "kind": "data",
                "category": "data",
                "label": "Write Text File",
                "provider_id": "core.write_text_file",
                "provider_label": "Write Text File",
                "config": resolved_writer_config,
                "position": {"x": 360, "y": 0},
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {"source_binding": {"type": "latest_payload", "source": "writer"}},
                "position": {"x": 540, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "model", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "model", "target_id": "writer", "label": "", "kind": "standard", "priority": 100},
            {"id": "e3", "source_id": "writer", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


def writer_environment_payload(graph_id: str = "writer-environment") -> dict[str, object]:
    def writer_agent_payload(agent_id: str, name: str, relative_path: str) -> dict[str, object]:
        graph_payload = writer_graph_payload(f"{graph_id}-{agent_id}", writer_config={"relative_path": relative_path})
        return {
            "agent_id": agent_id,
            "name": name,
            "description": "",
            "version": "1.0",
            "start_node_id": str(graph_payload["start_node_id"]),
            "nodes": list(graph_payload["nodes"]),
            "edges": list(graph_payload["edges"]),
        }

    return {
        "graph_id": graph_id,
        "name": "Writer Environment",
        "description": "",
        "version": "1.0",
        "graph_type": "test_environment",
        "agents": [
            writer_agent_payload("agent-alpha", "Agent Alpha", "outputs/alpha.txt"),
            writer_agent_payload("agent-beta", "Agent Beta", "outputs/beta.txt"),
        ],
    }


def spreadsheet_writer_graph_payload(
    csv_path: Path,
    *,
    graph_id: str = "writer-loop-graph",
    writer_config: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved_writer_config: dict[str, object] = {
        "mode": "write_text_file",
        "relative_path": "outputs/rows.txt",
    }
    if writer_config:
        resolved_writer_config.update(writer_config)
    return {
        "graph_id": graph_id,
        "name": "Spreadsheet Writer Graph",
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
                "id": "sheet",
                "kind": "control_flow_unit",
                "category": "control_flow_unit",
                "label": "Spreadsheet Rows",
                "provider_id": "core.spreadsheet_rows",
                "provider_label": "Spreadsheet Rows",
                "config": {
                    "mode": "spreadsheet_rows",
                    "file_format": "csv",
                    "file_path": str(csv_path),
                    "sheet_name": "",
                    "header_row_index": 1,
                    "start_row_index": 2,
                    "empty_row_policy": "skip",
                },
                "position": {"x": 160, "y": 0},
            },
            {
                "id": "writer",
                "kind": "data",
                "category": "data",
                "label": "Write Text File",
                "provider_id": "core.write_text_file",
                "provider_label": "Write Text File",
                "config": resolved_writer_config,
                "position": {"x": 340, "y": 0},
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {"source_binding": {"type": "latest_payload", "source": "writer"}},
                "position": {"x": 520, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "sheet", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "sheet", "target_id": "writer", "label": "", "kind": "standard", "priority": 100},
            {"id": "e3", "source_id": "writer", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


class AgentFilesystemTests(unittest.TestCase):
    def test_workspace_path_rejects_parent_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(Path(temp_dir) / ".graph-agent" / "runs")}, clear=False):
                self.assertEqual(normalize_workspace_relative_path("outputs/result.txt").as_posix(), "outputs/result.txt")
                with self.assertRaises(AgentFilesystemError):
                    normalize_workspace_relative_path("../secret.txt")
                with self.assertRaises(AgentFilesystemError):
                    resolve_agent_workspace_path("run-1", "agent-1", "../secret.txt")

    def test_writer_node_saves_model_output_inside_agent_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / ".graph-agent" / "runs"
            services = build_example_services()
            services.model_providers["file_writer_test"] = FileWriterProvider()
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph = GraphDefinition.from_dict(writer_graph_payload("writer-runtime"))
            graph.validate_against_services(services)
            with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
                state = runtime.run(
                    graph,
                    "Write the response into a file.",
                    run_id="run-writer-runtime",
                    agent_id="agent-alpha",
                )

            self.assertEqual(state.status, "completed")
            writer_output = state.node_outputs["writer"]
            file_record = writer_output["payload"]["file"]
            self.assertEqual(file_record["path"], "outputs/result.txt")
            self.assertEqual(file_record["write_mode"], "created")
            stored_path = workspace_root / "run-writer-runtime" / "agents" / "agent-alpha" / "workspace" / "outputs" / "result.txt"
            self.assertTrue(stored_path.exists())
            self.assertEqual(stored_path.read_text(encoding="utf-8"), "Workspace output from the model.")

    def test_writer_node_overwrites_existing_file_by_default_outside_loop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / ".graph-agent" / "runs"
            services = build_example_services()
            services.model_providers["file_writer_test"] = StaticFileWriterProvider("First version")
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph = GraphDefinition.from_dict(writer_graph_payload("writer-overwrite-default"))
            graph.validate_against_services(services)
            with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
                first_state = runtime.run(
                    graph,
                    "Write the first response into a file.",
                    run_id="run-writer-overwrite",
                    agent_id="agent-alpha",
                )
                services.model_providers["file_writer_test"] = StaticFileWriterProvider("Second version")
                second_state = runtime.run(
                    graph,
                    "Write the second response into a file.",
                    run_id="run-writer-overwrite",
                    agent_id="agent-alpha",
                )

            self.assertEqual(first_state.status, "completed")
            self.assertEqual(second_state.status, "completed")
            writer_output = second_state.node_outputs["writer"]
            self.assertEqual(writer_output["payload"]["write_mode"], "overwritten")
            stored_path = workspace_root / "run-writer-overwrite" / "agents" / "agent-alpha" / "workspace" / "outputs" / "result.txt"
            self.assertEqual(stored_path.read_text(encoding="utf-8"), "Second version")

    def test_writer_node_appends_by_default_inside_loop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / ".graph-agent" / "runs"
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])
                writer.writerow(["Portland", "62"])

            services = build_example_services()
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph = GraphDefinition.from_dict(spreadsheet_writer_graph_payload(csv_path, graph_id="writer-append-loop"))
            graph.validate_against_services(services)
            with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
                state = runtime.run(graph, {"request": "Write each row."}, run_id="run-writer-append-loop", agent_id="agent-alpha")

            self.assertEqual(state.status, "completed")
            writer_output = state.node_outputs["writer"]
            self.assertEqual(writer_output["payload"]["write_mode"], "created")
            self.assertTrue(writer_output["metadata"]["loop_execution"])
            self.assertEqual(writer_output["payload"]["configured_path"], "outputs/rows.txt")
            first_path = workspace_root / "run-writer-append-loop" / "agents" / "agent-alpha" / "workspace" / "outputs" / "rows-sheet-row-1.txt"
            second_path = workspace_root / "run-writer-append-loop" / "agents" / "agent-alpha" / "workspace" / "outputs" / "rows-sheet-row-2.txt"
            self.assertTrue(first_path.exists())
            self.assertTrue(second_path.exists())
            self.assertIn("Spreadsheet record 1", first_path.read_text(encoding="utf-8"))
            self.assertIn("Spreadsheet record 2", second_path.read_text(encoding="utf-8"))

    def test_writer_node_honors_explicit_overwrite_inside_loop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / ".graph-agent" / "runs"
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])
                writer.writerow(["Portland", "62"])

            services = build_example_services()
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph = GraphDefinition.from_dict(
                spreadsheet_writer_graph_payload(
                    csv_path,
                    graph_id="writer-overwrite-loop",
                    writer_config={"exists_behavior": "overwrite"},
                )
            )
            graph.validate_against_services(services)
            with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
                state = runtime.run(graph, {"request": "Write each row."}, run_id="run-writer-overwrite-loop", agent_id="agent-alpha")

            self.assertEqual(state.status, "completed")
            writer_output = state.node_outputs["writer"]
            self.assertEqual(writer_output["payload"]["write_mode"], "created")
            first_path = workspace_root / "run-writer-overwrite-loop" / "agents" / "agent-alpha" / "workspace" / "outputs" / "rows-sheet-row-1.txt"
            second_path = workspace_root / "run-writer-overwrite-loop" / "agents" / "agent-alpha" / "workspace" / "outputs" / "rows-sheet-row-2.txt"
            self.assertTrue(first_path.exists())
            self.assertTrue(second_path.exists())
            self.assertIn("Spreadsheet record 1", first_path.read_text(encoding="utf-8"))
            self.assertIn("Spreadsheet record 2", second_path.read_text(encoding="utf-8"))
            self.assertIn("city: Portland", second_path.read_text(encoding="utf-8"))

    def test_writer_node_honors_explicit_error_inside_loop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / ".graph-agent" / "runs"
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])
                writer.writerow(["Portland", "62"])

            services = build_example_services()
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph = GraphDefinition.from_dict(
                spreadsheet_writer_graph_payload(
                    csv_path,
                    graph_id="writer-error-loop",
                    writer_config={"exists_behavior": "error"},
                )
            )
            graph.validate_against_services(services)
            with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
                state = runtime.run(graph, {"request": "Write each row."}, run_id="run-writer-error-loop", agent_id="agent-alpha")

            self.assertEqual(state.status, "completed")
            self.assertIsNone(state.terminal_error)
            first_path = workspace_root / "run-writer-error-loop" / "agents" / "agent-alpha" / "workspace" / "outputs" / "rows-sheet-row-1.txt"
            second_path = workspace_root / "run-writer-error-loop" / "agents" / "agent-alpha" / "workspace" / "outputs" / "rows-sheet-row-2.txt"
            self.assertTrue(first_path.exists())
            self.assertTrue(second_path.exists())
            self.assertIn("Spreadsheet record 1", first_path.read_text(encoding="utf-8"))
            self.assertIn("Spreadsheet record 2", second_path.read_text(encoding="utf-8"))

    def test_files_endpoints_list_and_read_run_workspace_files(self) -> None:
        app_module = importlib.import_module("graph_agent.api.app")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bundled_path = temp_path / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            services = build_example_services()
            services.model_providers["file_writer_test"] = FileWriterProvider()
            manager = GraphRunManager(
                services=services,
                store=GraphStore(services, path=temp_path / "graphs.json", bundled_path=bundled_path),
                run_log_store=RunLogStore(temp_path / ".logs" / "runs"),
            )
            manager.create_graph(writer_graph_payload("writer-api"))
            original_manager = app_module.manager
            app_module.manager = manager
            try:
                with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(temp_path / ".graph-agent" / "runs")}, clear=False):
                    run_id = manager.start_run("writer-api", "Save the model output.")
                    state = wait_for_run_completion(manager, run_id)
                    self.assertEqual(state["status"], "completed")
                    with TestClient(app_module.app) as client:
                        listing_response = client.get(f"/api/runs/{run_id}/files")
                        self.assertEqual(listing_response.status_code, 200, msg=listing_response.text)
                        listing_payload = listing_response.json()
                        self.assertEqual([entry["path"] for entry in listing_payload["files"]], ["outputs/result.txt"])

                        content_response = client.get(
                            f"/api/runs/{run_id}/files/content",
                            params={"path": "outputs/result.txt"},
                        )
                        self.assertEqual(content_response.status_code, 200, msg=content_response.text)
                        content_payload = content_response.json()
                        self.assertEqual(content_payload["path"], "outputs/result.txt")
                        self.assertEqual(content_payload["content"], "Workspace output from the model.")
            finally:
                app_module.manager = original_manager
                manager.stop_background_services()

    def test_parent_environment_run_files_endpoint_lists_child_agent_workspace_files(self) -> None:
        app_module = importlib.import_module("graph_agent.api.app")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bundled_path = temp_path / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            services = build_example_services()
            services.model_providers["file_writer_test"] = FileWriterProvider()
            manager = GraphRunManager(
                services=services,
                store=GraphStore(services, path=temp_path / "graphs.json", bundled_path=bundled_path),
                run_log_store=RunLogStore(temp_path / ".logs" / "runs"),
            )
            manager.create_graph(writer_environment_payload("writer-environment-api"))
            original_manager = app_module.manager
            app_module.manager = manager
            try:
                with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(temp_path / ".graph-agent" / "runs")}, clear=False):
                    run_id = manager.start_run("writer-environment-api", "Save outputs for both agents.")
                    state = wait_for_run_completion(manager, run_id)
                    self.assertEqual(state["status"], "completed")
                    with TestClient(app_module.app) as client:
                        listing_response = client.get(f"/api/runs/{run_id}/files")
                        self.assertEqual(listing_response.status_code, 200, msg=listing_response.text)
                        listing_payload = listing_response.json()
                        self.assertEqual(
                            [entry["path"] for entry in listing_payload["files"]],
                            ["agent-alpha/outputs/alpha.txt", "agent-beta/outputs/beta.txt"],
                        )

                        content_response = client.get(
                            f"/api/runs/{run_id}/files/content",
                            params={"path": "agent-alpha/outputs/alpha.txt"},
                        )
                        self.assertEqual(content_response.status_code, 200, msg=content_response.text)
                        content_payload = content_response.json()
                        self.assertEqual(content_payload["path"], "agent-alpha/outputs/alpha.txt")
                        self.assertEqual(content_payload["workspace_path"], "outputs/alpha.txt")
                        self.assertEqual(content_payload["content"], "Workspace output from the model.")
            finally:
                app_module.manager = original_manager
                manager.stop_background_services()

    def test_parent_environment_run_files_endpoint_keeps_agent_metadata_when_one_agent_selected(self) -> None:
        app_module = importlib.import_module("graph_agent.api.app")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bundled_path = temp_path / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            services = build_example_services()
            services.model_providers["file_writer_test"] = FileWriterProvider()
            manager = GraphRunManager(
                services=services,
                store=GraphStore(services, path=temp_path / "graphs.json", bundled_path=bundled_path),
                run_log_store=RunLogStore(temp_path / ".logs" / "runs"),
            )
            manager.create_graph(writer_environment_payload("writer-environment-single-agent-api"))
            original_manager = app_module.manager
            app_module.manager = manager
            try:
                with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(temp_path / ".graph-agent" / "runs")}, clear=False):
                    run_id = manager.start_run("writer-environment-single-agent-api", "Save only one agent output.", agent_ids=["agent-alpha"])
                    state = wait_for_run_completion(manager, run_id)
                    self.assertEqual(state["status"], "completed")
                    with TestClient(app_module.app) as client:
                        listing_response = client.get(f"/api/runs/{run_id}/files")
                        self.assertEqual(listing_response.status_code, 200, msg=listing_response.text)
                        listing_payload = listing_response.json()
                        self.assertEqual([entry["path"] for entry in listing_payload["files"]], ["outputs/alpha.txt"])
                        self.assertEqual([entry["agent_id"] for entry in listing_payload["files"]], ["agent-alpha"])

                        content_response = client.get(
                            f"/api/runs/{run_id}/files/content",
                            params={"path": "outputs/alpha.txt"},
                        )
                        self.assertEqual(content_response.status_code, 200, msg=content_response.text)
                        content_payload = content_response.json()
                        self.assertEqual(content_payload["agent_id"], "agent-alpha")
                        self.assertEqual(content_payload["content"], "Workspace output from the model.")
            finally:
                app_module.manager = original_manager
                manager.stop_background_services()


if __name__ == "__main__":
    unittest.main()
