from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from threading import Event, Thread
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.api.manager import GraphRunManager, RunControl
from graph_agent.api.project_files import (
    PROJECT_FILE_SOURCE_SCRIPTS,
    PROJECT_FILE_SOURCE_UPLOAD,
    ProjectFileError,
    ProjectFileStore,
    SCRIPTS_FILE_ID_PREFIX,
)
from graph_agent.api.run_log_store import RunLogStore
from graph_agent.examples.tool_schema_repair import build_example_services




def wait_for_run_completion(manager: GraphRunManager, run_id: str, timeout_seconds: float = 5.0) -> dict[str, object]:
    import time

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        state = manager.get_run(run_id)
        if state["status"] in {"completed", "failed"}:
            return state
        time.sleep(0.05)
    raise AssertionError(f"Run '{run_id}' did not finish within {timeout_seconds} seconds.")


class ProjectFileTests(unittest.TestCase):
    def setUp(self) -> None:
        self._scripts_isolation_dir = tempfile.TemporaryDirectory()
        self._scripts_env_patch = patch.dict(
            os.environ, {"GRAPH_AGENT_SCRIPTS_DIR": self._scripts_isolation_dir.name}
        )
        self._scripts_env_patch.start()
        self.addCleanup(self._scripts_env_patch.stop)
        self.addCleanup(self._scripts_isolation_dir.cleanup)

    def test_manager_resolves_spreadsheet_rows_project_file_when_file_path_is_blank(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bundled_path = temp_path / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            services = build_example_services()
            store = GraphStore(services, path=temp_path / "graphs.json", bundled_path=bundled_path)
            manager = GraphRunManager(
                services=services,
                store=store,
                run_log_store=RunLogStore(temp_path / ".logs" / "runs"),
                project_file_store=ProjectFileStore(temp_path / ".graph-agent" / "project-files"),
            )
            try:
                uploaded_files = manager.upload_project_files(
                    "spreadsheet-project-file-graph",
                    [{"name": "rows.csv", "content_type": "text/csv", "data": b"city,temp\nSeattle,58\n"}],
                )
                project_file = uploaded_files[0]
                manager.create_graph(
                    {
                        "graph_id": "spreadsheet-project-file-graph",
                        "name": "Spreadsheet Project File Graph",
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
                                    "file_path": "",
                                    "project_file_id": project_file["file_id"],
                                    "project_file_name": project_file["name"],
                                    "sheet_name": "",
                                    "empty_row_policy": "skip",
                                },
                                "position": {"x": 140, "y": 0},
                            },
                            {
                                "id": "finish",
                                "kind": "output",
                                "category": "end",
                                "label": "Finish",
                                "provider_id": "core.output",
                                "provider_label": "Core Output Node",
                                "config": {"source_binding": {"type": "latest_payload", "source": "sheet"}},
                                "position": {"x": 280, "y": 0},
                            },
                        ],
                        "edges": [
                            {
                                "id": "start-sheet",
                                "source_id": "start",
                                "target_id": "sheet",
                                "label": "",
                                "kind": "standard",
                                "priority": 100,
                            },
                            {
                                "id": "sheet-finish",
                                "source_id": "sheet",
                                "source_handle_id": "control-flow-loop-body",
                                "target_id": "finish",
                                "label": "",
                                "kind": "standard",
                                "priority": 100,
                            },
                        ],
                        "env_vars": {},
                    }
                )

                run_id = manager.start_run("spreadsheet-project-file-graph", {"request": "Process the spreadsheet"})
                state = wait_for_run_completion(manager, run_id)

                self.assertEqual(state["status"], "completed")
                self.assertIsNone(state.get("terminal_error"))
                iterator_state = state["iterator_states"]["sheet"]
                self.assertEqual(iterator_state["total_rows"], 1)
                self.assertEqual(Path(iterator_state["source_file"]), Path(project_file["storage_path"]))
            finally:
                manager.stop_background_services()

    def test_project_file_endpoints_upload_list_and_delete(self) -> None:
        app_module = importlib.import_module("graph_agent.api.app")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bundled_path = temp_path / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            services = build_example_services()
            manager = GraphRunManager(
                services=services,
                store=GraphStore(services, path=temp_path / "graphs.json", bundled_path=bundled_path),
                run_log_store=RunLogStore(temp_path / ".logs" / "runs"),
                project_file_store=ProjectFileStore(temp_path / ".graph-agent" / "project-files"),
            )
            original_manager = app_module.manager
            app_module.manager = manager
            try:
                with TestClient(app_module.app) as client:
                    upload_response = client.post(
                        "/api/graphs/agent-123/files/upload",
                        files=[("files", ("matrix.csv", b"row,a\nb,1\n", "text/csv"))],
                    )
                    self.assertEqual(upload_response.status_code, 200, msg=upload_response.text)
                    upload_payload = upload_response.json()
                    self.assertEqual(len(upload_payload["files"]), 1)
                    project_file = upload_payload["files"][0]
                    self.assertEqual(project_file["graph_id"], "agent-123")
                    self.assertEqual(project_file["name"], "matrix.csv")
                    self.assertTrue(Path(project_file["storage_path"]).exists())

                    list_response = client.get("/api/graphs/agent-123/files")
                    self.assertEqual(list_response.status_code, 200, msg=list_response.text)
                    listed_files = list_response.json()["files"]
                    self.assertEqual([file["file_id"] for file in listed_files], [project_file["file_id"]])

                    delete_response = client.delete(f"/api/graphs/agent-123/files/{project_file['file_id']}")
                    self.assertEqual(delete_response.status_code, 200, msg=delete_response.text)

                    final_list_response = client.get("/api/graphs/agent-123/files")
                    self.assertEqual(final_list_response.status_code, 200, msg=final_list_response.text)
                    self.assertEqual(final_list_response.json()["files"], [])
            finally:
                app_module.manager = original_manager
                manager.stop_background_services()

    def test_stop_runtime_endpoint_requests_cancellation_without_resetting_state(self) -> None:
        app_module = importlib.import_module("graph_agent.api.app")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bundled_path = temp_path / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            services = build_example_services()
            manager = GraphRunManager(
                services=services,
                store=GraphStore(services, path=temp_path / "graphs.json", bundled_path=bundled_path),
                run_log_store=RunLogStore(temp_path / ".logs" / "runs"),
                project_file_store=ProjectFileStore(temp_path / ".graph-agent" / "project-files"),
            )
            stop_event = Event()
            worker = Thread(target=stop_event.wait, daemon=True)
            worker.start()
            with manager._lock:
                manager._run_controls["active-run"] = RunControl(
                    run_id="active-run",
                    cancel_event=Event(),
                    thread=worker,
                )
            original_manager = app_module.manager
            app_module.manager = manager
            try:
                with TestClient(app_module.app) as client:
                    response = client.post("/api/runtime/stop")
                    self.assertEqual(response.status_code, 200, msg=response.text)
                    payload = response.json()
                    self.assertEqual(payload["stopping_run_ids"], ["active-run"])
                    self.assertEqual(payload["stopping_run_count"], 1)
                    with manager._lock:
                        self.assertTrue(manager._run_controls["active-run"].cancel_event.is_set())
            finally:
                app_module.manager = original_manager
                stop_event.set()
                worker.join(timeout=1.0)
                manager.stop_background_services()

    def test_manager_moves_project_files_when_graph_id_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            bundled_path = temp_path / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            services = build_example_services()
            store = GraphStore(services, path=temp_path / "graphs.json", bundled_path=bundled_path)
            manager = GraphRunManager(
                services=services,
                store=store,
                run_log_store=RunLogStore(temp_path / ".logs" / "runs"),
                project_file_store=ProjectFileStore(temp_path / ".graph-agent" / "project-files"),
            )
            try:
                manager.create_graph(
                    {
                        "graph_id": "alpha-agent",
                        "name": "Alpha",
                        "description": "",
                        "version": "1.0",
                        "start_node_id": "start",
                        "nodes": [
                            {
                                "id": "start",
                                "kind": "input",
                                "category": "start",
                                "label": "Start",
                                "provider_id": "core.input",
                                "provider_label": "Input",
                                "config": {},
                                "position": {"x": 0, "y": 0},
                            },
                            {
                                "id": "finish",
                                "kind": "output",
                                "category": "end",
                                "label": "Finish",
                                "provider_id": "core.output",
                                "provider_label": "Output",
                                "config": {},
                                "position": {"x": 240, "y": 0},
                            },
                        ],
                        "edges": [
                            {
                                "id": "start-finish",
                                "source_id": "start",
                                "target_id": "finish",
                                "label": "",
                                "kind": "standard",
                                "priority": 100,
                            }
                        ],
                        "env_vars": {},
                    }
                )
                uploaded_files = manager.upload_project_files(
                    "alpha-agent",
                    [{"name": "sheet.csv", "content_type": "text/csv", "data": b"a,b\n1,2\n"}],
                )
                original_path = Path(uploaded_files[0]["storage_path"])
                self.assertTrue(original_path.exists())

                manager.update_graph(
                    "alpha-agent",
                    {
                        "graph_id": "beta-agent",
                        "name": "Beta",
                        "description": "",
                        "version": "1.0",
                        "start_node_id": "start",
                        "nodes": [
                            {
                                "id": "start",
                                "kind": "input",
                                "category": "start",
                                "label": "Start",
                                "provider_id": "core.input",
                                "provider_label": "Input",
                                "config": {},
                                "position": {"x": 0, "y": 0},
                            },
                            {
                                "id": "finish",
                                "kind": "output",
                                "category": "end",
                                "label": "Finish",
                                "provider_id": "core.output",
                                "provider_label": "Output",
                                "config": {},
                                "position": {"x": 240, "y": 0},
                            },
                        ],
                        "edges": [
                            {
                                "id": "start-finish",
                                "source_id": "start",
                                "target_id": "finish",
                                "label": "",
                                "kind": "standard",
                                "priority": 100,
                            }
                        ],
                        "env_vars": {},
                    },
                )

                self.assertEqual(manager.list_project_files("alpha-agent"), [])
                renamed_files = manager.list_project_files("beta-agent")
                self.assertEqual(len(renamed_files), 1)
                self.assertEqual(renamed_files[0]["name"], "sheet.csv")
                self.assertTrue(Path(renamed_files[0]["storage_path"]).exists())
                self.assertFalse(original_path.exists())
            finally:
                manager.stop_background_services()

    def test_scripts_folder_surfaces_as_virtual_project_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            scripts_dir = temp_path / "scripts"
            scripts_dir.mkdir()
            (scripts_dir / "sync.py").write_text("def run():\n    return True\n", encoding="utf-8")
            (scripts_dir / "README.md").write_text("ignored", encoding="utf-8")
            nested = scripts_dir / "nested"
            nested.mkdir()
            (nested / "helper.py").write_text("def run():\n    return True\n", encoding="utf-8")

            with patch.dict(os.environ, {"GRAPH_AGENT_SCRIPTS_DIR": str(scripts_dir)}):
                store = ProjectFileStore(temp_path / ".graph-agent" / "project-files")

                listed = store.list_files("some-graph")
                names_by_source: dict[str, list[str]] = {}
                for entry in listed:
                    names_by_source.setdefault(entry["source"], []).append(entry["name"])
                self.assertIn(PROJECT_FILE_SOURCE_SCRIPTS, names_by_source)
                self.assertEqual(sorted(names_by_source[PROJECT_FILE_SOURCE_SCRIPTS]), ["nested/helper.py", "sync.py"])
                self.assertNotIn(PROJECT_FILE_SOURCE_UPLOAD, names_by_source)

                scripts_entry = next(entry for entry in listed if entry["name"] == "sync.py")
                self.assertEqual(scripts_entry["file_id"], f"{SCRIPTS_FILE_ID_PREFIX}sync.py")
                self.assertEqual(scripts_entry["status"], "ready")
                self.assertEqual(scripts_entry["storage_path"], str(scripts_dir / "sync.py"))

                content = store.read_file_content("some-graph", scripts_entry["file_id"])
                self.assertIn("def run()", content["content"])

                with self.assertRaises(ProjectFileError):
                    store.delete_file("some-graph", scripts_entry["file_id"])

                uploaded = store.upload_files(
                    "some-graph",
                    [{"name": "notes.txt", "content_type": "text/plain", "data": b"hi"}],
                )
                self.assertEqual(uploaded[0]["source"], PROJECT_FILE_SOURCE_UPLOAD)

                merged = store.list_files("some-graph")
                merged_sources = [entry["source"] for entry in merged]
                self.assertEqual(
                    sorted(merged_sources),
                    [PROJECT_FILE_SOURCE_SCRIPTS, PROJECT_FILE_SOURCE_SCRIPTS, PROJECT_FILE_SOURCE_UPLOAD],
                )


if __name__ == "__main__":
    unittest.main()
