from __future__ import annotations

import csv
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.providers.base import ModelRequest, ModelResponse, ProviderPreflightResult
from graph_agent.providers.vendor_api import OpenAIChatModelProvider
from graph_agent.runtime.core import GraphDefinition
from graph_agent.runtime.engine import GraphRuntime
from graph_agent.runtime.spreadsheets import SpreadsheetParseError, parse_spreadsheet_matrix


class MatrixDecisionProvider:
    name = "matrix_decision_test"

    def __init__(self) -> None:
        self.user_messages: list[str] = []

    def generate(self, request: ModelRequest) -> ModelResponse:
        self.user_messages.append(request.messages[-1].content if request.messages else "")
        return ModelResponse(
            content="Selected a matrix cell.",
            structured_output={
                "message": {
                    "row_label": "High urgency",
                    "column_label": "Enterprise",
                    "reasoning": "High urgency enterprise requests should escalate immediately.",
                },
                "need_tool": False,
                "tool_calls": [],
            },
        )

    def preflight(self, provider_config=None) -> ProviderPreflightResult:
        return ProviderPreflightResult(
            status="available",
            ok=True,
            message="Matrix decision test provider is available.",
            details={"backend_type": "test"},
        )


class WrappedMatrixDecisionProvider:
    name = "wrapped_matrix_decision_test"

    def generate(self, request: ModelRequest) -> ModelResponse:
        return ModelResponse(
            content="Selected a matrix cell.",
            structured_output={
                "message": {
                    "row_label": "High urgency",
                    "column_label": "Enterprise",
                    "reasoning": "High urgency enterprise requests should escalate immediately.",
                },
                "need_tool": True,
                "tool_calls": [
                    {
                        "tool_name": "ignored_tool",
                        "arguments": {"note": "ignored by spreadsheet matrix node"},
                    }
                ],
            },
        )

    def preflight(self, provider_config=None) -> ProviderPreflightResult:
        return ProviderPreflightResult(
            status="available",
            ok=True,
            message="Wrapped matrix decision test provider is available.",
            details={"backend_type": "test"},
        )


class PlainTextMatrixOpenAIProvider(OpenAIChatModelProvider):
    name = "plain_text_matrix_openai"

    def _post_json(
        self,
        url: str,
        payload: dict[str, object],
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> dict[str, object]:
        return {
            "model": payload.get("model"),
            "choices": [
                {
                    "finish_reason": "stop",
                    "message": {
                        "content": "I would escalate this, but I did not return JSON.",
                        "tool_calls": [],
                    },
                }
            ],
            "usage": {"prompt_tokens": 8, "completion_tokens": 7},
        }

    def _headers(self, provider_config=None) -> dict[str, str]:
        return {"Authorization": "Bearer test"}


class SpreadsheetMatrixDecisionTests(unittest.TestCase):
    def test_parse_spreadsheet_matrix_rejects_duplicate_row_labels(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "matrix.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Audience", "SMB", "Enterprise"])
                writer.writerow(["High urgency", "Fast lane", "Escalate"])
                writer.writerow(["High urgency", "Retry", "Escalate again"])

            with self.assertRaises(SpreadsheetParseError):
                parse_spreadsheet_matrix(file_path=str(csv_path), file_format="csv")

    def test_runtime_selects_matrix_cell_and_outputs_value(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "decision-matrix.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Audience", "SMB", "Enterprise"])
                writer.writerow(["Low urgency", "Send help article", "Queue for success team"])
                writer.writerow(["High urgency", "Page support", "Escalate to dedicated team"])

            services = build_example_services()
            provider = MatrixDecisionProvider()
            services.model_providers[provider.name] = provider
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph = GraphDefinition.from_dict(
                {
                    "graph_id": "spreadsheet-matrix-runtime-graph",
                    "name": "Spreadsheet Matrix Runtime Graph",
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
                            "id": "matrix",
                            "kind": "model",
                            "category": "api",
                            "label": "Matrix Decision",
                            "provider_id": "core.spreadsheet_matrix_decision",
                            "provider_label": "Spreadsheet Matrix Decision",
                            "model_provider_name": provider.name,
                            "prompt_name": "spreadsheet_matrix_prompt",
                            "config": {
                                "provider_name": provider.name,
                                "model": "test-model",
                                "prompt_name": "spreadsheet_matrix_prompt",
                                "mode": "spreadsheet_matrix_decision",
                                "system_prompt": "Use the matrix to decide the next action.",
                                "user_message_template": "{input_payload}",
                                "file_format": "csv",
                                "file_path": str(csv_path),
                                "sheet_name": "",
                            },
                            "position": {"x": 240, "y": 0},
                        },
                        {
                            "id": "finish",
                            "kind": "output",
                            "category": "end",
                            "label": "Finish",
                            "provider_id": "core.output",
                            "provider_label": "Core Output Node",
                            "config": {},
                            "position": {"x": 520, "y": 0},
                        },
                    ],
                    "edges": [
                        {"id": "e1", "source_id": "start", "target_id": "matrix", "label": "", "kind": "standard", "priority": 100},
                        {"id": "e2", "source_id": "matrix", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                    ],
                }
            )

            state = runtime.run(graph, "An enterprise customer has a high urgency issue.", run_id="spreadsheet-matrix-runtime")

        self.assertEqual(state.final_output, "Escalate to dedicated team")
        matrix_output = state.node_outputs["matrix"]
        self.assertEqual(matrix_output["payload"], "Escalate to dedicated team")
        self.assertEqual(matrix_output["metadata"]["row_label"], "High urgency")
        self.assertEqual(matrix_output["metadata"]["column_label"], "Enterprise")
        self.assertEqual(
            matrix_output["artifacts"]["spreadsheet_matrix_selection"]["reasoning"],
            "High urgency enterprise requests should escalate immediately.",
        )
        self.assertEqual(len(provider.user_messages), 1)
        self.assertIn("Available row labels:", provider.user_messages[0])
        self.assertIn("High urgency", provider.user_messages[0])
        self.assertIn("Enterprise", provider.user_messages[0])
        self.assertIn("Escalate to dedicated team", provider.user_messages[0])
        self.assertIn("likely to respond to", provider.user_messages[0])
        self.assertIn("trust the detailed responsibilities over the headline title", provider.user_messages[0])
        self.assertIn("technical PM versus people manager", provider.user_messages[0])

    def test_validation_rejects_tool_capable_matrix_node(self) -> None:
        services = build_example_services()
        with self.assertRaisesRegex(ValueError, "cannot expose allowed_tool_names"):
            GraphDefinition.from_dict(
                {
                    "graph_id": "spreadsheet-matrix-invalid-tools",
                    "name": "Spreadsheet Matrix Invalid Tools",
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
                            "config": {},
                            "position": {"x": 0, "y": 0},
                        },
                        {
                            "id": "matrix",
                            "kind": "model",
                            "category": "api",
                            "label": "Matrix Decision",
                            "provider_id": "core.spreadsheet_matrix_decision",
                            "provider_label": "Spreadsheet Matrix Decision",
                            "config": {
                                "provider_name": "mock",
                                "mode": "spreadsheet_matrix_decision",
                                "system_prompt": "Use the matrix to decide the next action.",
                                "user_message_template": "{input_payload}",
                                "response_mode": "tool_call",
                                "allowed_tool_names": ["search_catalog"],
                            },
                            "position": {"x": 240, "y": 0},
                        },
                        {
                            "id": "finish",
                            "kind": "output",
                            "category": "end",
                            "label": "Finish",
                            "provider_id": "core.output",
                            "provider_label": "Core Output Node",
                            "config": {},
                            "position": {"x": 520, "y": 0},
                        },
                    ],
                    "edges": [
                        {"id": "e1", "source_id": "start", "target_id": "matrix", "label": "", "kind": "standard", "priority": 100},
                        {"id": "e2", "source_id": "matrix", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                    ],
                }
            ).validate_against_services(services)

    def test_runtime_ignores_wrapped_decision_control_fields(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "decision-matrix.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Audience", "SMB", "Enterprise"])
                writer.writerow(["Low urgency", "Send help article", "Queue for success team"])
                writer.writerow(["High urgency", "Page support", "Escalate to dedicated team"])

            services = build_example_services()
            provider = WrappedMatrixDecisionProvider()
            services.model_providers[provider.name] = provider
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph = GraphDefinition.from_dict(
                {
                    "graph_id": "spreadsheet-matrix-wrapped-runtime-graph",
                    "name": "Spreadsheet Matrix Wrapped Runtime Graph",
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
                            "id": "matrix",
                            "kind": "model",
                            "category": "api",
                            "label": "Matrix Decision",
                            "provider_id": "core.spreadsheet_matrix_decision",
                            "provider_label": "Spreadsheet Matrix Decision",
                            "model_provider_name": provider.name,
                            "prompt_name": "spreadsheet_matrix_prompt",
                            "config": {
                                "provider_name": provider.name,
                                "model": "test-model",
                                "prompt_name": "spreadsheet_matrix_prompt",
                                "mode": "spreadsheet_matrix_decision",
                                "system_prompt": "Use the matrix to decide the next action.",
                                "user_message_template": "{input_payload}",
                                "file_format": "csv",
                                "file_path": str(csv_path),
                                "sheet_name": "",
                            },
                            "position": {"x": 240, "y": 0},
                        },
                        {
                            "id": "finish",
                            "kind": "output",
                            "category": "end",
                            "label": "Finish",
                            "provider_id": "core.output",
                            "provider_label": "Core Output Node",
                            "config": {},
                            "position": {"x": 520, "y": 0},
                        },
                    ],
                    "edges": [
                        {"id": "e1", "source_id": "start", "target_id": "matrix", "label": "", "kind": "standard", "priority": 100},
                        {"id": "e2", "source_id": "matrix", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                    ],
                }
            )

            state = runtime.run(graph, "An enterprise customer has a high urgency issue.", run_id="spreadsheet-matrix-wrapped-runtime")

        self.assertEqual(state.final_output, "Escalate to dedicated team")
        matrix_output = state.node_outputs["matrix"]
        self.assertEqual(matrix_output["metadata"]["row_label"], "High urgency")
        self.assertEqual(matrix_output["metadata"]["column_label"], "Enterprise")

    def test_runtime_reports_selection_error_when_provider_returns_plain_text(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "decision-matrix.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Audience", "SMB", "Enterprise"])
                writer.writerow(["Low urgency", "Send help article", "Queue for success team"])
                writer.writerow(["High urgency", "Page support", "Escalate to dedicated team"])

            services = build_example_services()
            provider = PlainTextMatrixOpenAIProvider()
            services.model_providers[provider.name] = provider
            runtime = GraphRuntime(
                services=services,
                max_steps=services.config["max_steps"],
                max_visits_per_node=services.config["max_visits_per_node"],
            )
            graph = GraphDefinition.from_dict(
                {
                    "graph_id": "spreadsheet-matrix-plain-text-runtime-graph",
                    "name": "Spreadsheet Matrix Plain Text Runtime Graph",
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
                            "id": "matrix",
                            "kind": "model",
                            "category": "api",
                            "label": "Matrix Decision",
                            "provider_id": "core.spreadsheet_matrix_decision",
                            "provider_label": "Spreadsheet Matrix Decision",
                            "model_provider_name": provider.name,
                            "prompt_name": "spreadsheet_matrix_prompt",
                            "config": {
                                "provider_name": provider.name,
                                "model": "gpt-4.1-mini",
                                "prompt_name": "spreadsheet_matrix_prompt",
                                "mode": "spreadsheet_matrix_decision",
                                "system_prompt": "Use the matrix to decide the next action.",
                                "user_message_template": "{input_payload}",
                                "file_format": "csv",
                                "file_path": str(csv_path),
                                "sheet_name": "",
                            },
                            "position": {"x": 240, "y": 0},
                        },
                        {
                            "id": "finish",
                            "kind": "output",
                            "category": "end",
                            "label": "Finish",
                            "provider_id": "core.output",
                            "provider_label": "Core Output Node",
                            "config": {},
                            "position": {"x": 520, "y": 0},
                        },
                    ],
                    "edges": [
                        {"id": "e1", "source_id": "start", "target_id": "matrix", "label": "", "kind": "standard", "priority": 100},
                        {"id": "e2", "source_id": "matrix", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
                    ],
                }
            )

            state = runtime.run(
                graph,
                "An enterprise customer has a high urgency issue.",
                run_id="spreadsheet-matrix-plain-text-runtime",
            )

        self.assertEqual(state.status, "failed")
        self.assertIsInstance(state.terminal_error, dict)
        assert isinstance(state.terminal_error, dict)
        self.assertEqual(state.terminal_error["type"], "spreadsheet_matrix_selection_error")
        self.assertIn("structured JSON object response", state.terminal_error["message"])


if __name__ == "__main__":
    unittest.main()
