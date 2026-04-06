from __future__ import annotations

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


def wait_for_run_completion(manager: GraphRunManager, run_id: str, timeout_seconds: float = 5.0) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        state = manager.get_run(run_id)
        if state["status"] in {"completed", "failed"}:
            return state
        time.sleep(0.05)
    raise AssertionError(f"Run '{run_id}' did not finish within {timeout_seconds} seconds.")


def writer_graph_payload(graph_id: str = "writer-graph") -> dict[str, object]:
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
                "config": {
                    "mode": "write_text_file",
                    "relative_path": "outputs/result.txt",
                },
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
            stored_path = workspace_root / "run-writer-runtime" / "agents" / "agent-alpha" / "workspace" / "outputs" / "result.txt"
            self.assertTrue(stored_path.exists())
            self.assertEqual(stored_path.read_text(encoding="utf-8"), "Workspace output from the model.")

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


if __name__ == "__main__":
    unittest.main()
