from __future__ import annotations

import csv
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

from openpyxl import Workbook

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.api.run_state_reducer import apply_single_run_event, build_run_state
from graph_agent.providers.base import ModelRequest, ModelResponse, ProviderPreflightResult
from graph_agent.runtime.core import GraphDefinition
from graph_agent.runtime.documents import load_graph_document
from graph_agent.runtime.engine import GraphRuntime
from graph_agent.runtime.event_contract import normalize_runtime_state_snapshot
from graph_agent.runtime.spreadsheets import (
    SpreadsheetParseError,
    parse_spreadsheet,
    resolve_spreadsheet_path_from_run_documents,
)


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
            message="Spreadsheet echo provider is available for tests.",
            details={"backend_type": "test"},
        )


class SpreadsheetSessionEchoProvider(SpreadsheetEchoProvider):
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
            metadata={"session_id": f"session-{len(self.user_messages)}"},
        )


class SpreadsheetRowTests(unittest.TestCase):
    def test_resolve_spreadsheet_path_from_run_documents(self) -> None:
        path = "/data/rows.csv"
        docs = [
            {
                "document_id": "d1",
                "name": "rows.csv",
                "status": "ready",
                "storage_path": path,
            }
        ]
        self.assertEqual(resolve_spreadsheet_path_from_run_documents(docs), path)
        two = [
            docs[0],
            {**docs[0], "document_id": "d2", "name": "b.csv", "storage_path": "/other.csv"},
        ]
        self.assertEqual(resolve_spreadsheet_path_from_run_documents(two), "")

    def test_runtime_uses_run_document_when_file_path_empty(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])

            services = build_example_services()
            provider = SpreadsheetEchoProvider()
            services.model_providers["spreadsheet_echo"] = provider
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph_payload = {
                "graph_id": "spreadsheet-row-docs-graph",
                "name": "Spreadsheet Row Docs Graph",
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
                            "sheet_name": "",
                            "header_row_index": 1,
                            "start_row_index": 2,
                            "empty_row_policy": "skip",
                        },
                        "position": {"x": 100, "y": 0},
                    },
                    {
                        "id": "model",
                        "kind": "model",
                        "category": "api",
                        "label": "Model",
                        "provider_id": "core.api",
                        "provider_label": "API Call Node",
                        "model_provider_name": "spreadsheet_echo",
                        "prompt_name": "spreadsheet_prompt",
                        "config": {
                            "provider_name": "spreadsheet_echo",
                            "prompt_name": "spreadsheet_prompt",
                            "system_prompt": "Process the current spreadsheet row.",
                            "user_message_template": "{input_payload}",
                            "response_mode": "message",
                        },
                        "position": {"x": 220, "y": 0},
                    },
                    {
                        "id": "finish",
                        "kind": "output",
                        "category": "end",
                        "label": "Finish",
                        "provider_id": "core.output",
                        "provider_label": "Core Output Node",
                        "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                        "position": {"x": 340, "y": 0},
                    },
                ],
                "edges": [
                    {"id": "e1", "source_id": "start", "target_id": "sheet", "label": "", "kind": "standard", "priority": 100},
                    {
                        "id": "e2",
                        "source_id": "sheet",
                        "source_handle_id": "control-flow-loop-body",
                        "target_id": "model",
                        "label": "",
                        "kind": "standard",
                        "priority": 100,
                    },
                    {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                ],
            }
            graph = GraphDefinition.from_dict(graph_payload)
            graph.validate_against_services(services)
            run_docs = [
                {
                    "document_id": "doc-1",
                    "name": "rows.csv",
                    "mime_type": "text/csv",
                    "size_bytes": 1,
                    "storage_path": str(csv_path),
                    "text_content": "",
                    "text_excerpt": "",
                    "status": "ready",
                    "error": None,
                }
            ]
            state = runtime.run(
                graph,
                {"request": "Process spreadsheet rows"},
                run_id="spreadsheet-row-runtime-docs",
                documents=run_docs,
            )

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(provider.user_messages), 1)
        self.assertIn('"city": "Seattle"', provider.user_messages[0])

    def test_parse_csv_normalizes_headers_and_skips_empty_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "people.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Name", "Name", ""])
                writer.writerow(["Alice", "Engineer", "Seattle"])
                writer.writerow(["", "", ""])
                writer.writerow(["Bob", "Manager", "Portland"])

            parsed = parse_spreadsheet(file_path=str(csv_path), file_format="csv")

        self.assertEqual(parsed.headers, ["Name", "Name_2", "column_3"])
        self.assertEqual(parsed.row_count, 2)
        self.assertEqual(parsed.rows[0].row_number, 2)
        self.assertEqual(
            parsed.rows[0].row_data,
            {"Name": "Alice", "Name_2": "Engineer", "column_3": "Seattle"},
        )
        self.assertEqual(parsed.rows[1].row_number, 4)

    def test_parse_spreadsheet_always_uses_first_row_as_headers(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "people.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])
                writer.writerow(["Portland", "62"])

            parsed = parse_spreadsheet(
                file_path=str(csv_path),
                file_format="csv",
                header_row_index=9,
                start_row_index=2,
            )

        self.assertEqual(parsed.headers, ["city", "temperature"])
        self.assertEqual(parsed.rows[0].row_number, 2)
        self.assertEqual(parsed.rows[0].row_data, {"city": "Seattle", "temperature": "58"})

    def test_parse_spreadsheet_honors_custom_start_row_index(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "people.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])
                writer.writerow(["Portland", "62"])
                writer.writerow(["Boise", "71"])

            parsed = parse_spreadsheet(
                file_path=str(csv_path),
                file_format="csv",
                start_row_index=3,
            )

        self.assertEqual(parsed.row_count, 2)
        self.assertEqual(parsed.rows[0].row_number, 3)
        self.assertEqual(parsed.rows[0].row_data, {"city": "Portland", "temperature": "62"})
        self.assertEqual(parsed.rows[1].row_number, 4)

    def test_parse_spreadsheet_rejects_start_row_index_at_or_before_header(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "people.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])

            with self.assertRaises(SpreadsheetParseError):
                parse_spreadsheet(
                    file_path=str(csv_path),
                    file_format="csv",
                    start_row_index=1,
                )

    def test_parse_xlsx_uses_selected_sheet(self) -> None:
        with TemporaryDirectory() as temp_dir:
            xlsx_path = Path(temp_dir) / "inventory.xlsx"
            workbook = Workbook()
            default_sheet = workbook.active
            default_sheet.title = "IgnoreMe"
            default_sheet.append(["unused"])
            data_sheet = workbook.create_sheet("Products")
            data_sheet.append(["sku", "qty"])
            data_sheet.append(["A-1", 3])
            data_sheet.append(["B-2", 9])
            workbook.save(xlsx_path)

            parsed = parse_spreadsheet(
                file_path=str(xlsx_path),
                file_format="xlsx",
                sheet_name="Products",
                header_row_index=1,
                start_row_index=2,
            )

        self.assertEqual(parsed.sheet_name, "Products")
        self.assertIn("IgnoreMe", parsed.sheet_names)
        self.assertEqual(parsed.headers, ["sku", "qty"])
        self.assertEqual(parsed.row_count, 2)
        self.assertEqual(parsed.rows[1].row_data, {"sku": "B-2", "qty": 9})

    def test_runtime_processes_spreadsheet_rows_sequentially(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])
                writer.writerow(["Portland", "62"])

            services = build_example_services()
            provider = SpreadsheetEchoProvider()
            services.model_providers["spreadsheet_echo"] = provider
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph_payload = {
                "graph_id": "spreadsheet-row-runtime-graph",
                "name": "Spreadsheet Row Runtime Graph",
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
                        "position": {"x": 100, "y": 0},
                    },
                    {
                        "id": "model",
                        "kind": "model",
                        "category": "api",
                        "label": "Model",
                        "provider_id": "core.api",
                        "provider_label": "API Call Node",
                        "model_provider_name": "spreadsheet_echo",
                        "prompt_name": "spreadsheet_prompt",
                        "config": {
                            "provider_name": "spreadsheet_echo",
                            "prompt_name": "spreadsheet_prompt",
                            "system_prompt": "Process the current spreadsheet row.",
                            "user_message_template": "{input_payload}",
                            "response_mode": "message",
                        },
                        "position": {"x": 220, "y": 0},
                    },
                    {
                        "id": "finish",
                        "kind": "output",
                        "category": "end",
                        "label": "Finish",
                        "provider_id": "core.output",
                        "provider_label": "Core Output Node",
                        "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                        "position": {"x": 340, "y": 0},
                    },
                ],
                "edges": [
                    {"id": "e1", "source_id": "start", "target_id": "sheet", "label": "", "kind": "standard", "priority": 100},
                    {"id": "e2", "source_id": "sheet", "target_id": "model", "label": "", "kind": "standard", "priority": 100},
                    {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                ],
            }
            graph = GraphDefinition.from_dict(graph_payload)
            graph.validate_against_services(services)

            state = runtime.run(graph, {"request": "Process spreadsheet rows"}, run_id="spreadsheet-row-runtime")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(provider.user_messages), 2)
        self.assertIn('"city": "Seattle"', provider.user_messages[0])
        self.assertIn('"city": "Portland"', provider.user_messages[1])
        self.assertEqual(state.visit_counts.get("model"), 2)
        self.assertEqual(state.iterator_states["sheet"]["status"], "completed")
        self.assertEqual(state.iterator_states["sheet"]["current_row_index"], 2)
        self.assertEqual(state.iterator_states["sheet"]["total_rows"], 2)
        model_started_events = [
            event
            for event in state.event_history
            if event.event_type == "node.started" and isinstance(event.payload, dict) and event.payload.get("node_id") == "model"
        ]
        self.assertEqual(len(model_started_events), 2)
        self.assertEqual(
            [event.payload.get("iteration_id") for event in model_started_events],
            ["sheet:row:1", "sheet:row:2"],
        )
        self.assertTrue(all(event.payload.get("iterator_node_id") == "sheet" for event in model_started_events))
        self.assertIsInstance(state.final_output, str)
        self.assertIn("Portland", state.final_output)

    def test_model_completed_events_include_iteration_and_session_ids_per_row(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])
                writer.writerow(["Portland", "62"])

            services = build_example_services()
            provider = SpreadsheetSessionEchoProvider()
            services.model_providers["spreadsheet_echo"] = provider
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph_payload = {
                "graph_id": "spreadsheet-row-session-ids-graph",
                "name": "Spreadsheet Row Session IDs Graph",
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
                        "position": {"x": 100, "y": 0},
                    },
                    {
                        "id": "model",
                        "kind": "model",
                        "category": "api",
                        "label": "Model",
                        "provider_id": "core.api",
                        "provider_label": "API Call Node",
                        "model_provider_name": "spreadsheet_echo",
                        "prompt_name": "spreadsheet_prompt",
                        "config": {
                            "provider_name": "spreadsheet_echo",
                            "prompt_name": "spreadsheet_prompt",
                            "system_prompt": "Process the current spreadsheet row.",
                            "user_message_template": "{input_payload}",
                            "response_mode": "message",
                        },
                        "position": {"x": 220, "y": 0},
                    },
                    {
                        "id": "finish",
                        "kind": "output",
                        "category": "end",
                        "label": "Finish",
                        "provider_id": "core.output",
                        "provider_label": "Core Output Node",
                        "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                        "position": {"x": 340, "y": 0},
                    },
                ],
                "edges": [
                    {"id": "e1", "source_id": "start", "target_id": "sheet", "label": "", "kind": "standard", "priority": 100},
                    {"id": "e2", "source_id": "sheet", "target_id": "model", "label": "", "kind": "standard", "priority": 100},
                    {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                ],
            }
            graph = GraphDefinition.from_dict(graph_payload)
            graph.validate_against_services(services)

            state = runtime.run(graph, {"request": "Process spreadsheet rows"}, run_id="spreadsheet-row-session-ids")

        self.assertEqual(state.status, "completed")
        model_completed_events = [
            event
            for event in state.event_history
            if event.event_type == "node.completed" and isinstance(event.payload, dict) and event.payload.get("node_id") == "model"
        ]
        self.assertEqual(len(model_completed_events), 2)
        self.assertEqual(
            [event.payload.get("iteration_id") for event in model_completed_events],
            ["sheet:row:1", "sheet:row:2"],
        )
        self.assertEqual(
            [event.payload.get("session_id") for event in model_completed_events],
            ["session-1", "session-2"],
        )

    def test_end_agent_run_node_halts_spreadsheet_iteration_immediately(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])
                writer.writerow(["Portland", "62"])

            services = build_example_services()
            provider = SpreadsheetEchoProvider()
            services.model_providers["spreadsheet_echo"] = provider
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph_payload = {
                "graph_id": "spreadsheet-row-end-agent-run-graph",
                "name": "Spreadsheet Row End Agent Run Graph",
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
                        "position": {"x": 100, "y": 0},
                    },
                    {
                        "id": "model",
                        "kind": "model",
                        "category": "api",
                        "label": "Model",
                        "provider_id": "core.api",
                        "provider_label": "API Call Node",
                        "model_provider_name": "spreadsheet_echo",
                        "prompt_name": "spreadsheet_prompt",
                        "config": {
                            "provider_name": "spreadsheet_echo",
                            "prompt_name": "spreadsheet_prompt",
                            "system_prompt": "Process the current spreadsheet row.",
                            "user_message_template": "{input_payload}",
                            "response_mode": "message",
                        },
                        "position": {"x": 220, "y": 0},
                    },
                    {
                        "id": "finish",
                        "kind": "output",
                        "category": "end",
                        "label": "End Agent Run",
                        "provider_id": "end.agent_run",
                        "provider_label": "End Agent Run",
                        "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                        "position": {"x": 340, "y": 0},
                    },
                ],
                "edges": [
                    {"id": "e1", "source_id": "start", "target_id": "sheet", "label": "", "kind": "standard", "priority": 100},
                    {"id": "e2", "source_id": "sheet", "target_id": "model", "label": "", "kind": "standard", "priority": 100},
                    {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                ],
            }
            graph = GraphDefinition.from_dict(graph_payload)
            graph.validate_against_services(services)

            state = runtime.run(graph, {"request": "Process spreadsheet rows"}, run_id="spreadsheet-row-end-agent-run")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(provider.user_messages), 1)
        self.assertIn('"city": "Seattle"', provider.user_messages[0])
        self.assertEqual(state.visit_counts.get("model"), 1)
        self.assertEqual(state.iterator_states["sheet"]["status"], "terminated")
        self.assertEqual(state.iterator_states["sheet"]["current_row_index"], 1)
        self.assertEqual(state.iterator_states["sheet"]["total_rows"], 2)
        self.assertEqual(state.final_output, provider.user_messages[0])
        completed_events = [event for event in state.event_history if event.event_type == "run.completed"]
        self.assertEqual(len(completed_events), 1)
        self.assertEqual(completed_events[0].payload.get("terminal_node_id"), "finish")

    def test_context_builder_preserves_spreadsheet_rows_as_structured_sections(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "jobs.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Company", "CEO", "Summer_2026_Internships?"])
                writer.writerow(["Scale AI", "Alexandr Wang", "YES"])

            services = build_example_services()
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph_payload = {
                "graph_id": "spreadsheet-row-context-builder-graph",
                "name": "Spreadsheet Row Context Builder Graph",
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
                            "empty_row_policy": "skip",
                        },
                        "position": {"x": 100, "y": 0},
                    },
                    {
                        "id": "compose",
                        "kind": "data",
                        "category": "data",
                        "label": "Compose",
                        "provider_id": "core.context_builder",
                        "provider_label": "Context Builder",
                        "config": {"mode": "context_builder", "template": "", "input_bindings": [], "joiner": "\n\n"},
                        "position": {"x": 220, "y": 0},
                    },
                    {
                        "id": "finish",
                        "kind": "output",
                        "category": "end",
                        "label": "Finish",
                        "provider_id": "core.output",
                        "provider_label": "Core Output Node",
                        "config": {"source_binding": {"type": "latest_payload", "source": "compose"}},
                        "position": {"x": 340, "y": 0},
                    },
                ],
                "edges": [
                    {"id": "e1", "source_id": "start", "target_id": "sheet", "label": "", "kind": "standard", "priority": 100},
                    {
                        "id": "e2",
                        "source_id": "sheet",
                        "source_handle_id": "control-flow-loop-body",
                        "target_id": "compose",
                        "label": "",
                        "kind": "standard",
                        "priority": 100,
                    },
                    {"id": "e3", "source_id": "compose", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                ],
            }
            graph = GraphDefinition.from_dict(graph_payload)
            graph.validate_against_services(services)

            state = runtime.run(graph, {"request": "Process spreadsheet rows"}, run_id="spreadsheet-row-context-builder")

        self.assertEqual(state.status, "completed")
        self.assertIsInstance(state.final_output, list)
        assert isinstance(state.final_output, list)
        self.assertEqual(len(state.final_output), 1)
        self.assertEqual(
            state.final_output,
            [
                {
                    "Spreadsheet Rows": {
                        "row_index": 1,
                        "row_number": 2,
                        "sheet_name": "Sheet1",
                        "source_file": str(csv_path),
                        "row_data": {
                            "Company": "Scale AI",
                            "CEO": "Alexandr Wang",
                            "Summer_2026_Internships?": "YES",
                        },
                    }
                }
            ],
        )

    def test_failed_spreadsheet_rows_do_not_traverse_standard_edges(self) -> None:
        services = build_example_services()
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "spreadsheet-row-failure-graph",
            "name": "Spreadsheet Row Failure Graph",
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
                        "sheet_name": "",
                        "empty_row_policy": "skip",
                    },
                    "position": {"x": 100, "y": 0},
                },
                {
                    "id": "compose",
                    "kind": "data",
                    "category": "data",
                    "label": "Compose",
                    "provider_id": "core.context_builder",
                    "provider_label": "Context Builder",
                    "config": {"mode": "context_builder", "template": "", "input_bindings": [], "joiner": "\n\n"},
                    "position": {"x": 220, "y": 0},
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {"source_binding": {"type": "latest_payload", "source": "compose"}},
                    "position": {"x": 340, "y": 0},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "sheet", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "sheet",
                    "source_handle_id": "control-flow-loop-body",
                    "target_id": "compose",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {"id": "e3", "source_id": "compose", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)

        state = runtime.run(graph, {"request": "Process spreadsheet rows"}, run_id="spreadsheet-row-failure")

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error, state.node_errors.get("sheet"))
        assert isinstance(state.terminal_error, dict)
        self.assertEqual(state.terminal_error.get("type"), "spreadsheet_parse_error")
        self.assertNotEqual(state.terminal_error.get("type"), "max_steps_exceeded")
        self.assertEqual(state.visit_counts.get("compose"), None)
        self.assertFalse(any(transition.target_id == "compose" for transition in state.transition_history))

    def test_runtime_rejects_blank_start_row_index(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["city", "temperature"])
                writer.writerow(["Seattle", "58"])

            services = build_example_services()
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph_payload = {
                "graph_id": "spreadsheet-row-empty-start-row-graph",
                "name": "Spreadsheet Row Empty Start Row Graph",
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
                            "empty_row_policy": "skip",
                            "start_row_index": "",
                        },
                        "position": {"x": 100, "y": 0},
                    },
                    {
                        "id": "finish",
                        "kind": "output",
                        "category": "end",
                        "label": "Finish",
                        "provider_id": "core.output",
                        "provider_label": "Core Output Node",
                        "config": {"source_binding": {"type": "latest_payload", "source": "sheet"}},
                        "position": {"x": 220, "y": 0},
                    },
                ],
                "edges": [
                    {"id": "e1", "source_id": "start", "target_id": "sheet", "label": "", "kind": "standard", "priority": 100},
                    {"id": "e2", "source_id": "sheet", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                ],
            }
            graph = GraphDefinition.from_dict(graph_payload)
            graph.validate_against_services(services)

            state = runtime.run(graph, {"request": "Process spreadsheet rows"}, run_id="spreadsheet-row-empty-start-row")

        self.assertEqual(state.status, "failed")
        assert isinstance(state.terminal_error, dict)
        self.assertEqual(state.terminal_error.get("type"), "spreadsheet_parse_error")
        self.assertEqual(state.terminal_error.get("message"), "Starting row index is required.")
        self.assertEqual(state.visit_counts.get("finish"), None)

    def test_load_graph_document_migrates_legacy_spreadsheet_nodes(self) -> None:
        legacy_graph = load_graph_document(
            {
                "graph_id": "legacy-spreadsheet-graph",
                "name": "Legacy Spreadsheet Graph",
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
                        "kind": "data",
                        "category": "data",
                        "label": "Spreadsheet Rows",
                        "provider_id": "core.spreadsheet_rows",
                        "provider_label": "Spreadsheet Rows",
                        "config": {
                            "mode": "spreadsheet_rows",
                            "file_format": "csv",
                            "file_path": "/tmp/rows.csv",
                        },
                        "position": {"x": 100, "y": 0},
                    },
                    {
                        "id": "finish",
                        "kind": "output",
                        "category": "end",
                        "label": "Finish",
                        "provider_id": "core.output",
                        "provider_label": "Core Output Node",
                        "config": {},
                        "position": {"x": 220, "y": 0},
                    },
                ],
                "edges": [
                    {"id": "e1", "source_id": "start", "target_id": "sheet", "label": "", "kind": "standard", "priority": 100},
                    {"id": "e2", "source_id": "sheet", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                ],
            }
        )

        graph = legacy_graph.as_graph()
        sheet_node = next(node for node in graph.nodes.values() if node.id == "sheet")
        self.assertEqual(sheet_node.kind, "control_flow_unit")
        self.assertEqual(sheet_node.category.value, "control_flow_unit")

    def test_logic_conditions_routes_if_and_else(self) -> None:
        services = build_example_services()
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "logic-conditions-graph",
            "name": "Logic Conditions Graph",
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
                    "id": "branch",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Branch",
                    "provider_id": "core.logic_conditions",
                    "provider_label": "Logic Conditions",
                    "config": {
                        "mode": "logic_conditions",
                        "clauses": [
                            {
                                "id": "if",
                                "label": "If Approved",
                                "path": "approved",
                                "operator": "equals",
                                "value": True,
                                "source_contracts": ["message_envelope"],
                                "output_handle_id": "control-flow-if",
                            }
                        ],
                        "else_output_handle_id": "control-flow-else",
                    },
                    "position": {"x": 120, "y": 0},
                },
                {
                    "id": "if_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "If Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": -60},
                },
                {
                    "id": "else_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Else Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": 60},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "branch", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "branch",
                    "source_handle_id": "control-flow-if",
                    "target_id": "if_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {
                    "id": "e3",
                    "source_id": "branch",
                    "source_handle_id": "control-flow-else",
                    "target_id": "else_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)

        matched_state = runtime.run(graph, {"approved": True}, run_id="logic-conditions-if")
        self.assertEqual(matched_state.status, "completed")
        self.assertEqual(matched_state.final_output, {"approved": True})
        self.assertTrue(any(transition.target_id == "if_finish" for transition in matched_state.transition_history))
        self.assertFalse(any(transition.target_id == "else_finish" for transition in matched_state.transition_history))
        branch_output = matched_state.node_outputs["branch"]
        self.assertEqual(branch_output["metadata"]["matched_clause_label"], "If Approved")
        self.assertEqual(branch_output["metadata"]["matched_branch_label"], "If Approved")
        self.assertEqual(branch_output["metadata"]["condition_evaluations"][0]["matched"], True)
        self.assertEqual(branch_output["metadata"]["condition_evaluations"][0]["actual_value"], True)
        self.assertEqual(branch_output["metadata"]["branch_evaluations"][0]["matched"], True)

        else_state = runtime.run(graph, {"approved": False}, run_id="logic-conditions-else")
        self.assertEqual(else_state.status, "completed")
        self.assertEqual(else_state.final_output, {"approved": False})
        self.assertTrue(any(transition.target_id == "else_finish" for transition in else_state.transition_history))
        self.assertFalse(any(transition.target_id == "if_finish" for transition in else_state.transition_history))
        else_branch_output = else_state.node_outputs["branch"]
        self.assertEqual(else_branch_output["metadata"]["matched_clause_label"], "Else")
        self.assertEqual(else_branch_output["metadata"]["matched_branch_label"], "Else")
        self.assertEqual(else_branch_output["metadata"]["condition_evaluations"][0]["matched"], False)
        self.assertEqual(else_branch_output["metadata"]["condition_evaluations"][0]["actual_value"], False)
        self.assertEqual(else_branch_output["metadata"]["branch_evaluations"][0]["matched"], False)

    def test_logic_conditions_allows_unwired_else_fallthrough(self) -> None:
        services = build_example_services()
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "logic-conditions-unwired-else-graph",
            "name": "Logic Conditions Unwired Else Graph",
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
                    "id": "branch",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Branch",
                    "provider_id": "core.logic_conditions",
                    "provider_label": "Logic Conditions",
                    "config": {
                        "mode": "logic_conditions",
                        "clauses": [
                            {
                                "id": "if",
                                "label": "If Approved",
                                "path": "approved",
                                "operator": "equals",
                                "value": True,
                                "source_contracts": ["message_envelope"],
                                "output_handle_id": "control-flow-if",
                            }
                        ],
                        "else_output_handle_id": "control-flow-else",
                    },
                    "position": {"x": 120, "y": 0},
                },
                {
                    "id": "if_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "If Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": -60},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "branch", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "branch",
                    "source_handle_id": "control-flow-if",
                    "target_id": "if_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)

        else_state = runtime.run(graph, {"approved": False}, run_id="logic-conditions-unwired-else")

        self.assertEqual(else_state.status, "completed")
        self.assertIsNone(else_state.terminal_error)
        self.assertIsNone(else_state.final_output)
        branch_output = else_state.node_outputs["branch"]
        self.assertEqual(branch_output["metadata"]["matched_branch_label"], "Else")
        self.assertEqual(branch_output["metadata"]["selected_handle_id"], "control-flow-else")
        self.assertFalse(any(transition.target_id == "if_finish" for transition in else_state.transition_history))

    def test_logic_conditions_support_nested_branch_groups(self) -> None:
        services = build_example_services()
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "logic-branches-graph",
            "name": "Logic Branches Graph",
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
                    "id": "branch",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Branch",
                    "provider_id": "core.logic_conditions",
                    "provider_label": "Logic Conditions",
                    "config": {
                        "mode": "logic_conditions",
                        "branches": [
                            {
                                "id": "qualified",
                                "label": "Qualified",
                                "output_handle_id": "qualified-handle",
                                "root_group": {
                                    "id": "qualified-root",
                                    "type": "group",
                                    "combinator": "all",
                                    "children": [
                                        {
                                            "id": "approved-rule",
                                            "type": "rule",
                                            "path": "approved",
                                            "operator": "equals",
                                            "value": True,
                                            "source_contracts": [],
                                        },
                                        {
                                            "id": "score-group",
                                            "type": "group",
                                            "combinator": "any",
                                            "children": [
                                                {
                                                    "id": "score-rule",
                                                    "type": "rule",
                                                    "path": "score",
                                                    "operator": "gte",
                                                    "value": 90,
                                                    "source_contracts": [],
                                                },
                                                {
                                                    "id": "priority-rule",
                                                    "type": "rule",
                                                    "path": "priority",
                                                    "operator": "equals",
                                                    "value": "high",
                                                    "source_contracts": [],
                                                },
                                            ],
                                        },
                                    ],
                                },
                            },
                            {
                                "id": "review",
                                "label": "Needs Review",
                                "output_handle_id": "review-handle",
                                "root_group": {
                                    "id": "review-root",
                                    "type": "group",
                                    "combinator": "any",
                                    "children": [
                                        {
                                            "id": "flagged-rule",
                                            "type": "rule",
                                            "path": "flagged",
                                            "operator": "equals",
                                            "value": True,
                                            "source_contracts": [],
                                        }
                                    ],
                                },
                            },
                        ],
                        "else_output_handle_id": "control-flow-else",
                    },
                    "position": {"x": 120, "y": 0},
                },
                {
                    "id": "qualified_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Qualified Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": -90},
                },
                {
                    "id": "review_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Review Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": 0},
                },
                {
                    "id": "else_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Else Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": 90},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "branch", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "branch",
                    "source_handle_id": "qualified-handle",
                    "target_id": "qualified_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {
                    "id": "e3",
                    "source_id": "branch",
                    "source_handle_id": "review-handle",
                    "target_id": "review_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {
                    "id": "e4",
                    "source_id": "branch",
                    "source_handle_id": "control-flow-else",
                    "target_id": "else_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)

        qualified_state = runtime.run(
            graph,
            {"approved": True, "score": 95, "priority": "medium", "flagged": True},
            run_id="logic-branches-qualified",
        )
        self.assertEqual(qualified_state.status, "completed")
        self.assertTrue(any(transition.target_id == "qualified_finish" for transition in qualified_state.transition_history))
        self.assertFalse(any(transition.target_id == "review_finish" for transition in qualified_state.transition_history))
        qualified_output = qualified_state.node_outputs["branch"]
        self.assertEqual(qualified_output["metadata"]["matched_branch_label"], "Qualified")
        self.assertEqual(qualified_output["metadata"]["selected_handle_id"], "qualified-handle")
        self.assertEqual(qualified_output["metadata"]["branch_evaluations"][0]["matched"], True)
        qualified_trace = qualified_output["metadata"]["branch_evaluations"][0]["trace"]
        self.assertEqual(qualified_trace["combinator"], "all")
        self.assertEqual(qualified_trace["children"][1]["combinator"], "any")

        else_state = runtime.run(
            graph,
            {"approved": False, "score": 70, "priority": "low", "flagged": False},
            run_id="logic-branches-else",
        )
        self.assertEqual(else_state.status, "completed")
        self.assertTrue(any(transition.target_id == "else_finish" for transition in else_state.transition_history))
        else_output = else_state.node_outputs["branch"]
        self.assertEqual(else_output["metadata"]["matched_branch_label"], "Else")
        self.assertEqual(else_output["metadata"]["branch_evaluations"][0]["matched"], False)
        self.assertEqual(else_output["metadata"]["branch_evaluations"][1]["matched"], False)

    def test_logic_conditions_coerce_numeric_string_thresholds(self) -> None:
        services = build_example_services()
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "logic-string-thresholds-graph",
            "name": "Logic String Thresholds Graph",
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
                    "id": "branch",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Branch",
                    "provider_id": "core.logic_conditions",
                    "provider_label": "Logic Conditions",
                    "config": {
                        "mode": "logic_conditions",
                        "branches": [
                            {
                                "id": "within-limit",
                                "label": "Within Limit",
                                "output_handle_id": "within-limit-handle",
                                "root_group": {
                                    "id": "within-limit-root",
                                    "type": "group",
                                    "combinator": "all",
                                    "children": [
                                        {
                                            "id": "row-index-rule",
                                            "type": "rule",
                                            "path": "row_index",
                                            "operator": "lte",
                                            "value": "3",
                                            "source_contracts": [],
                                        }
                                    ],
                                },
                            }
                        ],
                        "else_output_handle_id": "control-flow-else",
                    },
                    "position": {"x": 120, "y": 0},
                },
                {
                    "id": "within_limit_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Within Limit Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": -60},
                },
                {
                    "id": "else_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Else Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": 60},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "branch", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "branch",
                    "source_handle_id": "within-limit-handle",
                    "target_id": "within_limit_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {
                    "id": "e3",
                    "source_id": "branch",
                    "source_handle_id": "control-flow-else",
                    "target_id": "else_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)

        matched_state = runtime.run(graph, {"row_index": 2}, run_id="logic-string-thresholds-if")
        self.assertEqual(matched_state.status, "completed")
        self.assertTrue(any(transition.target_id == "within_limit_finish" for transition in matched_state.transition_history))
        self.assertFalse(any(transition.target_id == "else_finish" for transition in matched_state.transition_history))
        matched_output = matched_state.node_outputs["branch"]
        self.assertEqual(matched_output["metadata"]["matched_branch_label"], "Within Limit")
        self.assertEqual(matched_output["metadata"]["condition_evaluations"][0]["matched"], True)
        self.assertEqual(matched_output["metadata"]["condition_evaluations"][0]["actual_value"], 2)
        self.assertEqual(matched_output["metadata"]["condition_evaluations"][0]["expected_value"], "3")

        else_state = runtime.run(graph, {"row_index": 4}, run_id="logic-string-thresholds-else")
        self.assertEqual(else_state.status, "completed")
        self.assertTrue(any(transition.target_id == "else_finish" for transition in else_state.transition_history))
        else_output = else_state.node_outputs["branch"]
        self.assertEqual(else_output["metadata"]["matched_branch_label"], "Else")
        self.assertEqual(else_output["metadata"]["condition_evaluations"][0]["matched"], False)

    def test_logic_conditions_no_matching_edge_error_reports_selected_handle(self) -> None:
        services = build_example_services()
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "logic-handle-debug-graph",
            "name": "Logic Handle Debug Graph",
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
                    "id": "branch",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Branch",
                    "provider_id": "core.logic_conditions",
                    "provider_label": "Logic Conditions",
                    "config": {
                        "mode": "logic_conditions",
                        "branches": [
                            {
                                "id": "company-exists",
                                "label": "Company Exists",
                                "output_handle_id": "control-flow-company-exists",
                                "root_group": {
                                    "id": "company-group",
                                    "type": "group",
                                    "combinator": "all",
                                    "children": [
                                        {
                                            "id": "company-rule",
                                            "type": "rule",
                                            "path": "company_exists",
                                            "operator": "equals",
                                            "value": True,
                                            "source_contracts": ["message_envelope"],
                                        }
                                    ],
                                },
                            }
                        ],
                        "else_output_handle_id": "control-flow-else",
                    },
                    "position": {"x": 120, "y": 0},
                },
                {
                    "id": "validation_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Validation Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": -60},
                },
                {
                    "id": "else_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Else Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {},
                    "position": {"x": 260, "y": 60},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "branch", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "branch",
                    "source_handle_id": "control-flow-company-exists",
                    "target_id": "validation_finish",
                    "label": "validation only",
                    "kind": "conditional",
                    "priority": 100,
                    "condition": {
                        "id": "validation-error",
                        "label": "Validation error",
                        "type": "result_status_equals",
                        "value": "validation_error",
                    },
                },
                {
                    "id": "e3",
                    "source_id": "branch",
                    "source_handle_id": "control-flow-else",
                    "target_id": "else_finish",
                    "label": "skip row",
                    "kind": "standard",
                    "priority": 100,
                },
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)

        state = runtime.run(graph, {"company_exists": True}, run_id="logic-handle-debug")

        self.assertEqual(state.status, "failed")
        assert state.terminal_error is not None
        self.assertEqual(state.terminal_error["type"], "no_matching_edge")
        self.assertEqual(state.terminal_error["selected_handle_id"], "control-flow-company-exists")
        self.assertEqual(state.terminal_error["matched_branch_label"], "Company Exists")
        self.assertEqual(state.terminal_error["emitted_route_handles"], ["control-flow-company-exists"])
        self.assertIn("Selected output handle was 'control-flow-company-exists'.", state.terminal_error["message"])
        self.assertIn("Matched branch was 'Company Exists'.", state.terminal_error["message"])

    def test_run_state_reducer_tracks_iterator_updates(self) -> None:
        state = build_run_state("run-iterator", "graph-1", None, execution_node_ids=["sheet"])
        next_state = apply_single_run_event(
            state,
            {
                "event_type": "node.iterator.updated",
                "summary": "Iterator updated.",
                "payload": {
                    "node_id": "sheet",
                    "iterator_type": "spreadsheet_rows",
                    "status": "running",
                    "current_row_index": 1,
                    "total_rows": 3,
                    "headers": ["city", "temperature"],
                    "sheet_name": "Sheet1",
                    "source_file": "/tmp/test.csv",
                    "file_format": "csv",
                },
                "run_id": "run-iterator",
                "timestamp": "2026-04-02T00:00:00Z",
            },
        )
        self.assertEqual(next_state["iterator_states"]["sheet"]["current_row_index"], 1)
        self.assertEqual(next_state["iterator_states"]["sheet"]["total_rows"], 3)
        self.assertEqual(next_state["loop_regions"]["sheet"]["active_iteration_id"], "sheet:row:1")

    def test_run_state_reducer_tracks_loop_region_members(self) -> None:
        state = build_run_state("run-loop", "graph-1", None, execution_node_ids=["sheet", "model", "finish"])
        state = apply_single_run_event(
            state,
            {
                "event_type": "node.iterator.updated",
                "summary": "Iterator updated.",
                "payload": {
                    "node_id": "sheet",
                    "iterator_node_id": "sheet",
                    "iterator_type": "spreadsheet_rows",
                    "status": "running",
                    "current_row_index": 2,
                    "iterator_row_index": 2,
                    "total_rows": 3,
                    "iterator_total_rows": 3,
                    "iteration_id": "sheet:row:2",
                },
                "run_id": "run-loop",
                "timestamp": "2026-04-02T00:00:00Z",
            },
        )
        state = apply_single_run_event(
            state,
            {
                "event_type": "node.started",
                "summary": "Started node 'Model'.",
                "payload": {
                    "node_id": "model",
                    "visit_count": 2,
                    "received_input": {"payload": "row 2"},
                    "iterator_node_id": "sheet",
                    "iterator_row_index": 2,
                    "iterator_total_rows": 3,
                    "iteration_id": "sheet:row:2",
                },
                "run_id": "run-loop",
                "timestamp": "2026-04-02T00:00:01Z",
            },
        )
        state = apply_single_run_event(
            state,
            {
                "event_type": "node.completed",
                "summary": "Completed node 'Finish'.",
                "payload": {
                    "node_id": "finish",
                    "status": "success",
                    "output": "done",
                    "route_outputs": {},
                    "error": None,
                    "metadata": {},
                    "iterator_node_id": "sheet",
                    "iterator_row_index": 2,
                    "iterator_total_rows": 3,
                    "iteration_id": "sheet:row:2",
                },
                "run_id": "run-loop",
                "timestamp": "2026-04-02T00:00:02Z",
            },
        )

        loop_region = state["loop_regions"]["sheet"]
        self.assertEqual(loop_region["status"], "running")
        self.assertEqual(loop_region["current_row_index"], 2)
        self.assertEqual(loop_region["total_rows"], 3)
        self.assertEqual(loop_region["active_iteration_id"], "sheet:row:2")
        self.assertEqual(loop_region["member_node_ids"], ["model", "finish"])

    def test_runtime_state_snapshot_normalizes_iterator_states(self) -> None:
        normalized = normalize_runtime_state_snapshot(
            {
                "run_id": "run-1",
                "graph_id": "graph-1",
                "event_history": [],
                "node_statuses": {},
                "iterator_states": {
                    "sheet": {
                        "iterator_type": "spreadsheet_rows",
                        "status": "completed",
                        "current_row_index": 2,
                        "total_rows": 2,
                    }
                },
                "loop_regions": {
                    "sheet": {
                        "iterator_node_id": "sheet",
                        "status": "completed",
                        "current_row_index": 2,
                        "total_rows": 2,
                        "member_node_ids": ["model", "finish"],
                    }
                },
                "agent_runs": {},
            }
        )
        assert normalized is not None
        self.assertEqual(normalized["iterator_states"]["sheet"]["status"], "completed")
        self.assertEqual(normalized["loop_regions"]["sheet"]["member_node_ids"], ["model", "finish"])


if __name__ == "__main__":
    unittest.main()
