from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from tempfile import TemporaryDirectory
from threading import Thread
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.providers.base import ModelProvider, ModelRequest, ModelResponse, ProviderPreflightResult
from graph_agent.providers.outlook import OutlookDraftClient, OutlookDraftResult
from graph_agent.runtime.core import GraphDefinition, _extract_outlook_recipient_value
from graph_agent.runtime.engine import GraphRuntime
from graph_agent.runtime.microsoft_auth import MicrosoftAuthStatus


class OutlookRecipientExtractionTests(unittest.TestCase):
    def test_extracts_email_from_apollo_payload_shape(self) -> None:
        apollo_payload = {
            "person": {
                "id": "person-123",
                "name": "Peter Mocarski",
                "email": "peterm@anthropic.com",
                "email_status": "verified",
                "contact": {},
                "personal_emails": [],
            },
            "organization": {"name": "Anthropic"},
        }
        self.assertEqual(_extract_outlook_recipient_value(apollo_payload), "peterm@anthropic.com")

    def test_prefers_envelope_metadata_resolved_email(self) -> None:
        envelope = {
            "payload": {"person": {"email": "stale@example.com"}},
            "metadata": {"resolved_email": "fresh@example.com"},
        }
        self.assertEqual(_extract_outlook_recipient_value(envelope), "fresh@example.com")

    def test_returns_none_when_no_recipient_anywhere(self) -> None:
        self.assertIsNone(_extract_outlook_recipient_value({"person": {}, "organization": {}}))


class FakeOutlookDraftClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create_draft(
        self,
        *,
        access_token: str,
        to_recipients: list[str],
        subject: str,
        body: str,
        signature: str = "",
    ) -> OutlookDraftResult:
        self.calls.append(
            {
                "access_token": access_token,
                "to_recipients": list(to_recipients),
                "subject": subject,
                "body": body,
                "signature": signature,
            }
        )
        return OutlookDraftResult(
            draft_id="draft-123",
            subject=subject,
            body=f"{body}\n\n{signature}".strip() if signature else body,
            to_recipients=list(to_recipients),
            web_link="https://outlook.office.com/mail/draft-123",
            created_at="2026-04-10T12:00:00Z",
            last_modified_at="2026-04-10T12:00:00Z",
            raw_response={
                "id": "draft-123",
                "conversationId": "conversation-123",
                "internetMessageId": "internet-message-123",
            },
        )


class FakeMicrosoftAuthService:
    def __init__(self) -> None:
        self.acquire_calls = 0
        self.failures_before_success = 0

    def connection_status(self) -> MicrosoftAuthStatus:
        return MicrosoftAuthStatus(
            status="connected",
            connected=True,
            pending=False,
            client_id="client-123",
            tenant_id="tenant-456",
            account_username="morgan@example.com",
            scopes=["Mail.ReadWrite"],
        )

    def acquire_access_token(self, *, scopes=None) -> str:  # noqa: ANN001
        self.acquire_calls += 1
        if self.acquire_calls <= self.failures_before_success:
            raise TimeoutError("The read operation timed out")
        return "graph-token"


class EmailDraftCaptureProvider(ModelProvider):
    name = "email_request_capture"

    def __init__(self) -> None:
        self.last_request: ModelRequest | None = None

    def generate(self, request: ModelRequest) -> ModelResponse:
        self.last_request = request
        return ModelResponse(
            content="",
            structured_output={
                "message": {
                    "subject": "Prompt-generated subject",
                    "body": "Prompt-generated body.",
                },
                "need_tool": False,
                "tool_calls": [],
            },
            metadata={"provider": self.name},
        )

    def preflight(self, provider_config=None) -> ProviderPreflightResult:  # noqa: ANN001
        return ProviderPreflightResult(status="available", ok=True, message="ok")


class _OutlookDraftStubHandler(BaseHTTPRequestHandler):
    requests: list[dict[str, object]] = []

    def do_POST(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(content_length).decode("utf-8")
        payload = json.loads(raw_body or "{}")
        self.__class__.requests.append(
            {
                "method": "POST",
                "path": self.path,
                "authorization": self.headers.get("Authorization", ""),
                "body": payload,
            }
        )
        response_body = json.dumps(
            {
                "id": "draft-http-1",
                "subject": payload.get("subject", ""),
                "conversationId": "conversation-http-1",
                "internetMessageId": "internet-http-1",
                "webLink": "https://outlook.office.com/mail/draft-http-1",
                "createdDateTime": "2026-04-10T14:00:00Z",
                "lastModifiedDateTime": "2026-04-10T14:00:00Z",
            }
        ).encode("utf-8")
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


class OutlookHttpStubServer:
    def __enter__(self) -> str:
        _OutlookDraftStubHandler.requests = []
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _OutlookDraftStubHandler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


class _SupabaseEmailLogStubHandler(BaseHTTPRequestHandler):
    last_headers: dict[str, str] = {}
    last_path: str = ""
    last_json_body: object | None = None
    request_bodies: list[object] = []
    fail_source_run_id_fk_once: bool = False
    source_run_id_fk_failures: int = 0

    def do_GET(self) -> None:  # noqa: N802
        type(self).last_headers = {str(key).lower(): str(value) for key, value in self.headers.items()}
        type(self).last_path = self.path
        if self.path == "/rest/v1/":
            payload = {
                "openapi": "3.0.0",
                "paths": {
                    "/outbound_email_messages": {
                        "get": {
                            "summary": "Outbound email messages",
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "type": "array",
                                                "items": {"$ref": "#/components/schemas/outbound_email_messages"},
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
                        "outbound_email_messages": {
                            "type": "object",
                            "properties": {
                                "provider": {"type": "string"},
                                "mailbox_account": {"type": "string"},
                                "recipient_email": {"type": "string"},
                                "subject": {"type": "string"},
                                "body_text": {"type": "string"},
                                "message_type": {"type": "string"},
                                "outreach_step": {"type": "integer"},
                                "sales_approach": {"type": "string"},
                                "sales_approach_version": {"type": "string", "nullable": True},
                                "parent_outbound_email_id": {"type": "string", "nullable": True},
                                "root_outbound_email_id": {"type": "string", "nullable": True},
                                "provider_draft_id": {"type": "string"},
                                "provider_message_id": {"type": "string"},
                                "internet_message_id": {"type": "string"},
                                "conversation_id": {"type": "string"},
                                "drafted_at": {"type": "string"},
                                "observed_sent_at": {"type": "string", "nullable": True},
                                "metadata": {"type": "object"},
                                "raw_provider_payload": {"type": "object"},
                                "source_run_id": {"type": "string", "nullable": True},
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
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        type(self).last_headers = {str(key).lower(): str(value) for key, value in self.headers.items()}
        type(self).last_path = self.path
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(content_length).decode("utf-8")
        type(self).last_json_body = json.loads(raw_body or "{}")
        type(self).request_bodies.append(type(self).last_json_body)
        if self.path == "/rest/v1/outbound_email_messages":
            if (
                type(self).fail_source_run_id_fk_once
                and type(self).source_run_id_fk_failures == 0
                and isinstance(type(self).last_json_body, dict)
                and "source_run_id" in type(self).last_json_body
            ):
                type(self).source_run_id_fk_failures += 1
                body = json.dumps(
                    {
                        "code": "23503",
                        "details": 'Key (source_run_id)=(run-outlook-log-fk-retry) is not present in table "runs".',
                        "hint": None,
                        "message": 'insert or update on table "outbound_email_messages" violates foreign key constraint "outbound_email_messages_source_run_id_fkey"',
                    }
                ).encode("utf-8")
                self.send_response(409)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
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

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


class SupabaseEmailLogStubServer:
    def __enter__(self) -> str:
        _SupabaseEmailLogStubHandler.last_headers = {}
        _SupabaseEmailLogStubHandler.last_path = ""
        _SupabaseEmailLogStubHandler.last_json_body = None
        _SupabaseEmailLogStubHandler.request_bodies = []
        _SupabaseEmailLogStubHandler.fail_source_run_id_fk_once = False
        _SupabaseEmailLogStubHandler.source_run_id_fk_failures = 0
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _SupabaseEmailLogStubHandler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


def build_outlook_draft_graph_payload() -> dict[str, object]:
    return {
        "graph_id": "outlook-draft-agent",
        "name": "Outlook Draft Agent",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "nodes": [
            {
                "id": "start",
                "kind": "input",
                "category": "start",
                "label": "Run Button Start",
                "provider_id": "start.manual_run",
                "provider_label": "Run Button Start",
                "description": "",
                "position": {"x": 0, "y": 0},
                "config": {"input_binding": {"type": "input_payload"}},
            },
            {
                "id": "draft",
                "kind": "output",
                "category": "end",
                "label": "Outlook Draft End",
                "provider_id": "end.outlook_draft",
                "provider_label": "Outlook Draft End",
                "description": "",
                "position": {"x": 280, "y": 0},
                "config": {
                    "to": "alex@example.com; taylor@example.com",
                    "subject": "Follow-up for {graph_id}",
                    "require_to": True,
                    "require_subject": True,
                    "require_body": True,
                },
            },
        ],
        "edges": [
            {
                "id": "edge-start-draft",
                "source_id": "start",
                "target_id": "draft",
                "label": "draft",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            }
        ],
    }


def build_outlook_draft_graph_with_logger_payload() -> dict[str, object]:
    payload = build_outlook_draft_graph_payload()
    nodes = list(payload["nodes"])
    edges = list(payload["edges"])
    nodes.append(
        {
            "id": "logger",
            "kind": "data",
            "category": "data",
            "label": "Outbound Email Logger",
            "provider_id": "core.outbound_email_logger",
            "provider_label": "Outbound Email Logger",
            "description": "",
            "position": {"x": 280, "y": 140},
            "config": {
                "mode": "outbound_email_logger",
                "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
                "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
                "schema": "public",
                "table_name": "outbound_email_messages",
                "message_type": "initial",
                "outreach_step": 0,
                "sales_approach": "warm intro",
                "sales_approach_version": "v1",
                "metadata_json": "{\"campaign\":\"spring-launch\",\"run_id\":\"{run_id}\"}",
            },
        }
    )
    edges.append(
        {
            "id": "edge-logger-draft",
            "source_id": "logger",
            "target_id": "draft",
            "label": "email log",
            "kind": "binding",
            "priority": 0,
            "condition": None,
        }
    )
    payload["nodes"] = nodes
    payload["edges"] = edges
    return payload


def build_model_outlook_draft_graph_with_logger_payload() -> dict[str, object]:
    return {
        "graph_id": "outlook-draft-model-agent",
        "name": "Outlook Draft Model Agent",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "nodes": [
            {
                "id": "start",
                "kind": "input",
                "category": "start",
                "label": "Run Button Start",
                "provider_id": "start.manual_run",
                "provider_label": "Run Button Start",
                "description": "",
                "position": {"x": 0, "y": 0},
                "config": {"input_binding": {"type": "input_payload"}},
            },
            {
                "id": "model",
                "kind": "model",
                "category": "api",
                "label": "Compose Email",
                "provider_id": "core.api",
                "provider_label": "API Call Node",
                "model_provider_name": "email_request_capture",
                "prompt_name": "compose_email",
                "config": {
                    "provider_name": "email_request_capture",
                    "prompt_name": "compose_email",
                    "system_prompt": "Write a concise outreach email.",
                    "user_message_template": "Prospect context:\n{input_payload}",
                    "response_mode": "message",
                },
                "position": {"x": 180, "y": 0},
            },
            {
                "id": "draft",
                "kind": "output",
                "category": "end",
                "label": "Outlook Draft End",
                "provider_id": "end.outlook_draft",
                "provider_label": "Outlook Draft End",
                "description": "",
                "position": {"x": 420, "y": 0},
                "config": {
                    "to": "alex@example.com",
                    "source_binding": {"type": "latest_payload", "source": "model"},
                    "subject": "",
                    "require_to": True,
                    "require_subject": True,
                    "require_body": True,
                },
            },
            {
                "id": "logger",
                "kind": "data",
                "category": "data",
                "label": "Outbound Email Logger",
                "provider_id": "core.outbound_email_logger",
                "provider_label": "Outbound Email Logger",
                "description": "",
                "position": {"x": 420, "y": 140},
                "config": {
                    "mode": "outbound_email_logger",
                    "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
                    "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
                    "schema": "public",
                    "table_name": "outbound_email_messages",
                    "message_type": "initial",
                    "outreach_step": 0,
                    "sales_approach": "warm intro",
                    "sales_approach_version": "v1",
                    "metadata_json": "{\"campaign\":\"spring-launch\",\"run_id\":\"{run_id}\"}",
                },
            },
        ],
        "edges": [
            {
                "id": "edge-start-model",
                "source_id": "start",
                "target_id": "model",
                "label": "draft",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-model-draft",
                "source_id": "model",
                "target_id": "draft",
                "label": "draft",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-logger-draft",
                "source_id": "logger",
                "target_id": "draft",
                "label": "email log",
                "kind": "binding",
                "priority": 0,
                "condition": None,
            },
        ],
    }


def build_spreadsheet_outlook_draft_graph_payload(*, csv_path: str, duplicate_loop_edges: bool = False) -> dict[str, object]:
    sheet_to_draft_edges: list[dict[str, object]] = [
        {
            "id": "edge-sheet-draft-1",
            "source_id": "sheet",
            "source_handle_id": "control-flow-loop-body",
            "target_id": "draft",
            "label": "row",
            "kind": "standard",
            "priority": 100,
            "condition": None,
        }
    ]
    if duplicate_loop_edges:
        sheet_to_draft_edges.append(
            {
                "id": "edge-sheet-draft-2",
                "source_id": "sheet",
                "source_handle_id": "control-flow-loop-body",
                "target_id": "draft",
                "label": "row-duplicate",
                "kind": "standard",
                "priority": 90,
                "condition": None,
            }
        )

    return {
        "graph_id": "outlook-draft-spreadsheet-agent",
        "name": "Outlook Draft Spreadsheet Agent",
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
                "description": "",
                "position": {"x": 0, "y": 0},
                "config": {"input_binding": {"type": "input_payload"}},
            },
            {
                "id": "sheet",
                "kind": "control_flow_unit",
                "category": "control_flow_unit",
                "label": "Spreadsheet Rows",
                "provider_id": "core.spreadsheet_rows",
                "provider_label": "Spreadsheet Rows",
                "description": "",
                "position": {"x": 160, "y": 0},
                "config": {
                    "mode": "spreadsheet_rows",
                    "file_format": "csv",
                    "file_path": csv_path,
                    "sheet_name": "",
                    "empty_row_policy": "skip",
                },
            },
            {
                "id": "draft",
                "kind": "output",
                "category": "end",
                "label": "Outlook Draft End",
                "provider_id": "end.outlook_draft",
                "provider_label": "Outlook Draft End",
                "description": "",
                "position": {"x": 360, "y": 0},
                "config": {
                    "to": "alex@example.com",
                    "subject": "Follow-up row {row_index}",
                    "require_to": True,
                    "require_subject": True,
                    "require_body": True,
                },
            },
        ],
        "edges": [
            {
                "id": "edge-start-sheet",
                "source_id": "start",
                "target_id": "sheet",
                "label": "sheet",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            *sheet_to_draft_edges,
        ],
    }


def build_spreadsheet_context_builder_outlook_graph_payload(*, csv_path: str) -> dict[str, object]:
    return {
        "graph_id": "outlook-draft-context-builder-agent",
        "name": "Outlook Draft Context Builder Agent",
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
                "description": "",
                "position": {"x": 0, "y": 0},
                "config": {"input_binding": {"type": "input_payload"}},
            },
            {
                "id": "sheet",
                "kind": "control_flow_unit",
                "category": "control_flow_unit",
                "label": "Spreadsheet Rows",
                "provider_id": "core.spreadsheet_rows",
                "provider_label": "Spreadsheet Rows",
                "description": "",
                "position": {"x": 160, "y": 0},
                "config": {
                    "mode": "spreadsheet_rows",
                    "file_format": "csv",
                    "file_path": csv_path,
                    "sheet_name": "",
                    "empty_row_policy": "skip",
                },
            },
            {
                "id": "split",
                "kind": "control_flow_unit",
                "category": "control_flow_unit",
                "label": "Parallel Split",
                "provider_id": "core.parallel_splitter",
                "provider_label": "Parallel Splitter",
                "description": "",
                "position": {"x": 320, "y": 0},
                "config": {},
            },
            {
                "id": "left",
                "kind": "data",
                "category": "data",
                "label": "Left Branch",
                "provider_id": "core.data",
                "provider_label": "Core Data Node",
                "description": "",
                "position": {"x": 500, "y": -80},
                "config": {"mode": "passthrough"},
            },
            {
                "id": "right",
                "kind": "data",
                "category": "data",
                "label": "Right Branch",
                "provider_id": "core.data",
                "provider_label": "Core Data Node",
                "description": "",
                "position": {"x": 500, "y": 80},
                "config": {"mode": "passthrough"},
            },
            {
                "id": "compose",
                "kind": "data",
                "category": "data",
                "label": "Compose",
                "provider_id": "core.context_builder",
                "provider_label": "Context Builder",
                "description": "",
                "position": {"x": 700, "y": 0},
                "config": {"mode": "context_builder", "template": "", "input_bindings": []},
            },
            {
                "id": "display",
                "kind": "data",
                "category": "data",
                "label": "Envelope Display Node 7",
                "provider_id": "core.data_display",
                "provider_label": "Envelope Display Node",
                "description": "",
                "position": {"x": 880, "y": 0},
                "config": {"show_input_envelope": False},
            },
            {
                "id": "draft",
                "kind": "output",
                "category": "end",
                "label": "Outlook Draft End",
                "provider_id": "end.outlook_draft",
                "provider_label": "Outlook Draft End",
                "description": "",
                "position": {"x": 1080, "y": 0},
                "config": {
                    "to": "alex@example.com",
                    "subject": "Follow-up",
                    "require_to": True,
                    "require_subject": True,
                    "require_body": True,
                },
            },
        ],
        "edges": [
            {
                "id": "edge-start-sheet",
                "source_id": "start",
                "target_id": "sheet",
                "label": "sheet",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-sheet-split",
                "source_id": "sheet",
                "source_handle_id": "control-flow-loop-body",
                "target_id": "split",
                "label": "row",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-split-left",
                "source_id": "split",
                "target_id": "left",
                "label": "left",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-split-right",
                "source_id": "split",
                "target_id": "right",
                "label": "right",
                "kind": "standard",
                "priority": 110,
                "condition": None,
            },
            {
                "id": "edge-left-compose",
                "source_id": "left",
                "target_id": "compose",
                "label": "left to compose",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-right-compose",
                "source_id": "right",
                "target_id": "compose",
                "label": "right to compose",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-compose-display",
                "source_id": "compose",
                "target_id": "display",
                "label": "next",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-display-draft",
                "source_id": "display",
                "target_id": "draft",
                "label": "next",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
        ],
    }


class OutlookDraftNodeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def test_catalog_includes_outlook_draft_provider(self) -> None:
        provider_ids = {provider.provider_id for provider in self.services.node_provider_registry.list_definitions()}
        self.assertIn("end.outlook_draft", provider_ids)
        self.assertIn("core.outbound_email_logger", provider_ids)

    def test_outlook_draft_node_uses_connected_microsoft_account(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph = GraphDefinition.from_dict(build_outlook_draft_graph_payload())
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        state = runtime.run(graph, "Draft this exact email body.")

        self.assertEqual(state.status, "completed")
        self.assertEqual(fake_auth.acquire_calls, 1)
        self.assertEqual(len(fake_client.calls), 1)
        self.assertEqual(fake_client.calls[0]["access_token"], "graph-token")
        self.assertEqual(fake_client.calls[0]["to_recipients"], ["alex@example.com", "taylor@example.com"])
        self.assertEqual(fake_client.calls[0]["subject"], "Follow-up for outlook-draft-agent")
        self.assertEqual(fake_client.calls[0]["body"], "Draft this exact email body.")
        self.assertEqual(fake_client.calls[0]["signature"], "")
        self.assertEqual(state.node_outputs["draft"]["delivery_status"], "draft_saved")
        self.assertEqual(state.node_outputs["draft"]["draft_id"], "draft-123")
        self.assertEqual(state.node_outputs["draft"]["account_username"], "morgan@example.com")
        self.assertEqual(state.final_output["subject"], "Follow-up for outlook-draft-agent")
        self.assertEqual(state.final_output["body"], "Draft this exact email body.")

    def test_outlook_draft_node_retries_access_token_timeout(self) -> None:
        from graph_agent.runtime import core as core_module

        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        fake_auth.failures_before_success = 2
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph = GraphDefinition.from_dict(build_outlook_draft_graph_payload())
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        sleep_calls: list[float] = []
        with patch.object(core_module.time, "sleep", side_effect=sleep_calls.append):
            state = runtime.run(graph, "Draft this exact email body.")

        self.assertEqual(state.status, "completed")
        self.assertEqual(fake_auth.acquire_calls, 3)
        self.assertEqual(sleep_calls, [1.0, 2.0])
        self.assertEqual(len(fake_client.calls), 1)

    def test_outlook_draft_node_appends_signature_from_config(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph_payload = build_outlook_draft_graph_payload()
        draft_node = graph_payload["nodes"][1]
        assert isinstance(draft_node, dict)
        draft_node["config"] = {
            **draft_node["config"],
            "signature": "Best,\nMorgan",
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        state = runtime.run(graph, "Draft this exact email body.")

        self.assertEqual(state.status, "completed")
        self.assertEqual(fake_client.calls[0]["signature"], "Best,\nMorgan")
        self.assertEqual(state.node_outputs["draft"]["body"], "Draft this exact email body.\n\nBest,\nMorgan")
        self.assertEqual(state.final_output["body"], "Draft this exact email body.\n\nBest,\nMorgan")

    def test_outlook_draft_node_allows_optional_fields_when_toggles_are_disabled(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph_payload = build_outlook_draft_graph_payload()
        draft_node = graph_payload["nodes"][1]
        assert isinstance(draft_node, dict)
        draft_node["config"] = {
            "to": "",
            "subject": "",
            "require_to": False,
            "require_subject": False,
            "require_body": False,
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        state = runtime.run(graph, "")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(fake_client.calls), 1)
        self.assertEqual(fake_client.calls[0]["to_recipients"], [])
        self.assertEqual(fake_client.calls[0]["subject"], "")
        self.assertEqual(fake_client.calls[0]["body"], "")
        self.assertEqual(state.node_outputs["draft"]["to_recipients"], [])
        self.assertEqual(state.node_outputs["draft"]["subject"], "")
        self.assertEqual(state.node_outputs["draft"]["body"], "")

    def test_outlook_draft_node_parses_subject_and_unescapes_body_from_message_payload(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph_payload = build_outlook_draft_graph_payload()
        draft_node = graph_payload["nodes"][1]
        assert isinstance(draft_node, dict)
        draft_node["config"] = {
            "to": "brian@example.com",
            "subject": "",
            "require_to": True,
            "require_subject": True,
            "require_body": True,
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        payload = (
            "Subject: Fellow Cornellian -> Exploring OpenAI + Strong Engineering Pipeline\\n\\n"
            "Hi Brian,\\n\\n"
            "I came across your profile and wanted to reach out.\\n\\n"
            "Would love to grab 15 minutes to learn what you're hiring for."
        )
        state = runtime.run(graph, payload)

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(fake_client.calls), 1)
        self.assertEqual(
            fake_client.calls[0]["subject"],
            "Fellow Cornellian -> Exploring OpenAI + Strong Engineering Pipeline",
        )
        self.assertEqual(
            fake_client.calls[0]["body"],
            "Hi Brian,\n\n"
            "I came across your profile and wanted to reach out.\n\n"
            "Would love to grab 15 minutes to learn what you're hiring for.",
        )
        self.assertEqual(
            state.final_output["subject"],
            "Fellow Cornellian -> Exploring OpenAI + Strong Engineering Pipeline",
        )
        self.assertEqual(
            state.final_output["body"],
            "Hi Brian,\n\n"
            "I came across your profile and wanted to reach out.\n\n"
            "Would love to grab 15 minutes to learn what you're hiring for.",
        )

    def test_outlook_draft_node_uses_payload_email_when_to_field_is_blank(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph_payload = build_outlook_draft_graph_payload()
        draft_node = graph_payload["nodes"][1]
        assert isinstance(draft_node, dict)
        draft_node["config"] = {
            "to": "",
            "subject": "Hello {name}",
            "require_to": True,
            "require_subject": True,
            "require_body": True,
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        state = runtime.run(
            graph,
            {
                "name": "Taylor",
                "email": "taylor@example.com",
                "body": "Checking in on next steps.",
            },
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(fake_client.calls[0]["to_recipients"], ["taylor@example.com"])
        self.assertEqual(fake_client.calls[0]["subject"], "Hello Taylor")
        self.assertEqual(fake_client.calls[0]["body"], "Checking in on next steps.")
        self.assertEqual(state.final_output["to_recipients"], ["taylor@example.com"])

    def test_outlook_draft_node_templates_to_field_from_payload_email(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph_payload = build_outlook_draft_graph_payload()
        draft_node = graph_payload["nodes"][1]
        assert isinstance(draft_node, dict)
        draft_node["config"] = {
            "to": "{email}",
            "subject": "Follow-up",
            "require_to": True,
            "require_subject": True,
            "require_body": True,
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        state = runtime.run(
            graph,
            {
                "email": "sam@example.com",
                "body": "Wanted to share a quick update.",
            },
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(fake_client.calls[0]["to_recipients"], ["sam@example.com"])
        self.assertEqual(state.final_output["to_recipients"], ["sam@example.com"])

    def test_outlook_draft_node_skips_invalid_recipient_and_writes_payload_file(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph_payload = build_outlook_draft_graph_payload()
        draft_node = graph_payload["nodes"][1]
        assert isinstance(draft_node, dict)
        draft_node["config"] = {
            "to": "{email}",
            "subject": "Hello",
            "require_to": True,
            "require_subject": True,
            "require_body": True,
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        row_payload = {
            "email": "no_email",
            "name": "Skipped Sam",
            "body": "Body text that will not be drafted.",
        }

        with TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {"GRAPH_AGENT_WORKSPACE_DIR": temp_dir},
                clear=False,
            ):
                state = runtime.run(graph, row_payload, run_id="run-outlook-skip-invalid")

            self.assertEqual(state.status, "completed")
            self.assertEqual(len(fake_client.calls), 0)
            self.assertEqual(fake_auth.acquire_calls, 0)
            self.assertEqual(
                state.final_output["delivery_status"],
                "draft_skipped_invalid_recipient",
            )
            self.assertEqual(state.final_output["to_recipients"], [])
            self.assertIn("no_email", state.final_output["reason"])
            self.assertEqual(state.final_output["source_payload"], row_payload)
            file_record = state.final_output["skipped_draft_file"]
            self.assertIsInstance(file_record, dict)
            self.assertTrue(file_record["path"].startswith("outputs/skipped-drafts/"))

            workspace_dir = Path(temp_dir) / "run-outlook-skip-invalid" / "agents" / "default" / "workspace"
            written_files = list(workspace_dir.rglob("*.json"))
            self.assertEqual(len(written_files), 1)
            written = json.loads(written_files[0].read_text(encoding="utf-8"))
            self.assertEqual(written["delivery_status"], "draft_skipped_invalid_recipient")
            self.assertEqual(written["source_payload"], row_payload)
            self.assertIn("no_email", written["reason"])

    def test_outlook_client_uses_draft_endpoint_only(self) -> None:
        with OutlookHttpStubServer() as server_url:
            client = OutlookDraftClient(api_base_url=server_url)
            result = client.create_draft(
                access_token="graph-token",
                to_recipients=["alex@example.com", "taylor@example.com"],
                subject="Quarterly update",
                body="Please review the attached progress summary.",
            )

        self.assertEqual(result.draft_id, "draft-http-1")
        self.assertEqual(len(_OutlookDraftStubHandler.requests), 1)
        request = _OutlookDraftStubHandler.requests[0]
        self.assertEqual(request["path"], "/me/messages")
        self.assertEqual(request["authorization"], "Bearer graph-token")
        self.assertNotIn("/sendMail", str(request["path"]))
        self.assertEqual(
            request["body"]["toRecipients"],
            [
                {"emailAddress": {"address": "alex@example.com"}},
                {"emailAddress": {"address": "taylor@example.com"}},
            ],
        )
        self.assertEqual(request["body"]["subject"], "Quarterly update")
        self.assertEqual(request["body"]["body"]["contentType"], "Text")
        self.assertEqual(request["body"]["body"]["content"], "Please review the attached progress summary.")

    def test_outlook_client_retries_on_timeout_and_succeeds(self) -> None:
        from graph_agent.providers import outlook as outlook_module

        sleep_calls: list[float] = []
        urlopen_calls: list[object] = []

        class _FakeResponse:
            def __init__(self, body: bytes) -> None:
                self._body = body

            def __enter__(self) -> "_FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return self._body

        def fake_urlopen(request: object, timeout: float = 0.0) -> _FakeResponse:  # noqa: ANN001
            urlopen_calls.append(request)
            if len(urlopen_calls) <= 2:
                raise TimeoutError("The read operation timed out")
            return _FakeResponse(
                json.dumps(
                    {
                        "id": "draft-retry",
                        "subject": "Retry subject",
                        "webLink": "https://outlook.office.com/mail/draft-retry",
                        "createdDateTime": "2026-04-26T10:00:00Z",
                        "lastModifiedDateTime": "2026-04-26T10:00:00Z",
                    }
                ).encode("utf-8")
            )

        with patch.object(outlook_module.urllib_request, "urlopen", side_effect=fake_urlopen):
            with patch.object(outlook_module.time, "sleep", side_effect=sleep_calls.append):
                client = OutlookDraftClient(retry_backoff_seconds=0.1)
                result = client.create_draft(
                    access_token="graph-token",
                    to_recipients=["alex@example.com"],
                    subject="Retry subject",
                    body="Retry body.",
                )

        self.assertEqual(result.draft_id, "draft-retry")
        self.assertEqual(len(urlopen_calls), 3)
        self.assertEqual(sleep_calls, [0.1, 0.2])

    def test_outlook_client_raises_after_exhausting_retries_on_timeout(self) -> None:
        from graph_agent.providers import outlook as outlook_module

        sleep_calls: list[float] = []
        urlopen_calls: list[object] = []

        def always_timeout(request: object, timeout: float = 0.0) -> None:  # noqa: ANN001
            urlopen_calls.append(request)
            raise TimeoutError("The read operation timed out")

        with patch.object(outlook_module.urllib_request, "urlopen", side_effect=always_timeout):
            with patch.object(outlook_module.time, "sleep", side_effect=sleep_calls.append):
                client = OutlookDraftClient(retry_backoff_seconds=0.1, max_retries=2)
                with self.assertRaises(RuntimeError) as ctx:
                    client.create_draft(
                        access_token="graph-token",
                        to_recipients=["alex@example.com"],
                        subject="Subject",
                        body="Body.",
                    )

        self.assertEqual(len(urlopen_calls), 3)
        self.assertEqual(sleep_calls, [0.1, 0.2])
        self.assertIn("timed out after 3 attempts", str(ctx.exception))

    def test_outlook_client_retries_on_socket_timeout(self) -> None:
        import socket as socket_module

        from graph_agent.providers import outlook as outlook_module

        sleep_calls: list[float] = []
        urlopen_calls: list[object] = []

        def always_socket_timeout(request: object, timeout: float = 0.0) -> None:  # noqa: ANN001
            urlopen_calls.append(request)
            raise socket_module.timeout("The read operation timed out")

        with patch.object(outlook_module.urllib_request, "urlopen", side_effect=always_socket_timeout):
            with patch.object(outlook_module.time, "sleep", side_effect=sleep_calls.append):
                client = OutlookDraftClient(retry_backoff_seconds=0.1, max_retries=2)
                with self.assertRaises(RuntimeError) as ctx:
                    client.create_draft(
                        access_token="graph-token",
                        to_recipients=["alex@example.com"],
                        subject="Subject",
                        body="Body.",
                    )

        self.assertEqual(len(urlopen_calls), 3)
        self.assertEqual(sleep_calls, [0.1, 0.2])
        self.assertIn("timed out after 3 attempts", str(ctx.exception))

    def test_outlook_draft_node_logs_outbound_email_row_when_logger_is_bound(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph = GraphDefinition.from_dict(build_outlook_draft_graph_with_logger_payload())
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with SupabaseEmailLogStubServer() as base_url, patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": base_url,
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            state = runtime.run(graph, "Draft this exact email body.", run_id="run-outlook-log")

        self.assertEqual(state.status, "completed")
        self.assertEqual(_SupabaseEmailLogStubHandler.last_path, "/rest/v1/outbound_email_messages")
        self.assertEqual(_SupabaseEmailLogStubHandler.last_headers.get("apikey"), "service-role-key")
        self.assertEqual(_SupabaseEmailLogStubHandler.last_headers.get("authorization"), "Bearer service-role-key")
        self.assertEqual(_SupabaseEmailLogStubHandler.last_headers.get("accept-profile"), "public")
        self.assertIsInstance(_SupabaseEmailLogStubHandler.last_json_body, dict)
        row = _SupabaseEmailLogStubHandler.last_json_body
        assert isinstance(row, dict)
        self.assertEqual(row["provider"], "outlook")
        self.assertEqual(row["mailbox_account"], "morgan@example.com")
        self.assertEqual(row["recipient_email"], "alex@example.com")
        self.assertEqual(row["subject"], "Follow-up for outlook-draft-agent")
        self.assertEqual(row["body_text"], "Draft this exact email body.")
        self.assertEqual(row["message_type"], "initial")
        self.assertEqual(row["outreach_step"], 0)
        self.assertEqual(row["sales_approach"], "warm intro")
        self.assertEqual(row["sales_approach_version"], "v1")
        self.assertEqual(row["provider_draft_id"], "draft-123")
        self.assertEqual(row["provider_message_id"], "draft-123")
        self.assertEqual(row["internet_message_id"], "internet-message-123")
        self.assertEqual(row["conversation_id"], "conversation-123")
        self.assertEqual(row["drafted_at"], "2026-04-10T12:00:00Z")
        self.assertEqual(row["source_run_id"], "run-outlook-log")
        self.assertEqual(row["metadata"]["campaign"], "spring-launch")
        self.assertEqual(row["metadata"]["run_id"], "run-outlook-log")
        self.assertEqual(row["metadata"]["logger_node_id"], "logger")
        self.assertEqual(row["raw_provider_payload"]["conversationId"], "conversation-123")
        self.assertEqual(state.final_output["outbound_email_log"]["table_name"], "outbound_email_messages")
        self.assertTrue(state.node_outputs["draft"]["outbound_email_log"])

    def test_outlook_draft_node_logs_generation_prompts_from_model_source(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        prompt_provider = EmailDraftCaptureProvider()
        self.services.model_providers[prompt_provider.name] = prompt_provider
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph = GraphDefinition.from_dict(build_model_outlook_draft_graph_with_logger_payload())
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with SupabaseEmailLogStubServer() as base_url, patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": base_url,
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            state = runtime.run(graph, "Alex runs growth at Example Co.", run_id="run-outlook-log-prompts")

        self.assertEqual(state.status, "completed")
        self.assertEqual(fake_client.calls[0]["subject"], "Prompt-generated subject")
        self.assertEqual(fake_client.calls[0]["body"], "Prompt-generated body.")
        self.assertIsInstance(_SupabaseEmailLogStubHandler.last_json_body, dict)
        row = _SupabaseEmailLogStubHandler.last_json_body
        assert isinstance(row, dict)
        self.assertIn("generation_prompt", row["metadata"])
        generation_prompt = row["metadata"]["generation_prompt"]
        self.assertEqual(generation_prompt["source_node_id"], "model")
        self.assertEqual(generation_prompt["prompt_name"], "compose_email")
        self.assertEqual(generation_prompt["system_prompt"], "Write a concise outreach email.")
        self.assertEqual(generation_prompt["user_prompt"], "Prospect context:\nAlex runs growth at Example Co.")
        self.assertEqual(
            generation_prompt["messages"],
            [
                {"role": "system", "content": "Write a concise outreach email."},
                {"role": "user", "content": "Prospect context:\nAlex runs growth at Example Co."},
            ],
        )
        self.assertIsNotNone(prompt_provider.last_request)
        assert prompt_provider.last_request is not None
        self.assertEqual(prompt_provider.last_request.messages[0].content, generation_prompt["system_prompt"])
        self.assertEqual(prompt_provider.last_request.messages[-1].content, generation_prompt["user_prompt"])
        self.assertEqual(state.final_output["outbound_email_log"]["inserted_row"]["metadata"]["generation_prompt"], generation_prompt)

    def test_outlook_draft_node_logs_outbound_email_row_when_sales_approach_is_blank(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph_payload = build_outlook_draft_graph_with_logger_payload()
        logger_node = graph_payload["nodes"][2]
        assert isinstance(logger_node, dict)
        logger_config = dict(logger_node.get("config", {}))
        logger_config["sales_approach"] = ""
        logger_node["config"] = logger_config
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with SupabaseEmailLogStubServer() as base_url, patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": base_url,
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            state = runtime.run(graph, "Draft this exact email body.", run_id="run-outlook-log-no-sales-approach")

        self.assertEqual(state.status, "completed")
        self.assertIsInstance(_SupabaseEmailLogStubHandler.last_json_body, dict)
        row = _SupabaseEmailLogStubHandler.last_json_body
        assert isinstance(row, dict)
        self.assertNotIn("sales_approach", row)
        self.assertEqual(row["source_run_id"], "run-outlook-log-no-sales-approach")
        self.assertEqual(row["metadata"]["run_id"], "run-outlook-log-no-sales-approach")
        self.assertEqual(state.final_output["outbound_email_log"]["table_name"], "outbound_email_messages")
        self.assertTrue(state.node_outputs["draft"]["outbound_email_log"])

    def test_outlook_draft_node_retries_without_source_run_id_when_run_fk_is_missing(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph = GraphDefinition.from_dict(build_outlook_draft_graph_with_logger_payload())
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with SupabaseEmailLogStubServer() as base_url, patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": base_url,
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            _SupabaseEmailLogStubHandler.fail_source_run_id_fk_once = True
            state = runtime.run(graph, "Draft this exact email body.", run_id="run-outlook-log-fk-retry")

        self.assertEqual(state.status, "completed")
        self.assertEqual(_SupabaseEmailLogStubHandler.source_run_id_fk_failures, 1)
        self.assertEqual(len(_SupabaseEmailLogStubHandler.request_bodies), 2)
        first_row = _SupabaseEmailLogStubHandler.request_bodies[0]
        second_row = _SupabaseEmailLogStubHandler.request_bodies[1]
        assert isinstance(first_row, dict)
        assert isinstance(second_row, dict)
        self.assertEqual(first_row["source_run_id"], "run-outlook-log-fk-retry")
        self.assertNotIn("source_run_id", second_row)
        self.assertEqual(second_row["metadata"]["run_id"], "run-outlook-log-fk-retry")
        self.assertNotIn("source_run_id", state.final_output["outbound_email_log"]["inserted_row"])
        self.assertEqual(state.final_output["outbound_email_log"]["table_name"], "outbound_email_messages")
        self.assertTrue(state.node_outputs["draft"]["outbound_email_log"])

    def test_outlook_draft_node_logs_outbound_email_with_sentinel_when_recipient_is_missing(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        graph_payload = build_outlook_draft_graph_with_logger_payload()
        draft_node = graph_payload["nodes"][1]
        assert isinstance(draft_node, dict)
        draft_node["config"] = {
            "to": "",
            "subject": "",
            "require_to": False,
            "require_subject": False,
            "require_body": False,
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with SupabaseEmailLogStubServer() as base_url, patch.dict(
            os.environ,
            {
                "GRAPH_AGENT_SUPABASE_URL": base_url,
                "GRAPH_AGENT_SUPABASE_SECRET_KEY": "service-role-key",
            },
            clear=False,
        ):
            state = runtime.run(graph, "", run_id="run-outlook-log-missing-recipient")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(fake_client.calls), 1)
        self.assertEqual(fake_client.calls[0]["to_recipients"], [])
        self.assertEqual(state.final_output["to_recipients"], [])

        outbound_log = state.final_output["outbound_email_log"]
        self.assertNotIn("skipped", outbound_log)
        self.assertTrue(outbound_log["recipient_missing"])
        self.assertEqual(outbound_log["table_name"], "outbound_email_messages")

        self.assertIsInstance(_SupabaseEmailLogStubHandler.last_json_body, dict)
        row = _SupabaseEmailLogStubHandler.last_json_body
        assert isinstance(row, dict)
        self.assertEqual(row["recipient_email"], "missing-recipient@graph-agent.invalid")
        self.assertTrue(row["metadata"]["recipient_missing"])
        self.assertEqual(row["metadata"]["recipient_substitution"], "missing-recipient@graph-agent.invalid")
        self.assertEqual(row["metadata"]["to_recipients"], [])

    def test_outlook_draft_node_dedupes_spreadsheet_rows_across_reruns(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["email", "name"])
                writer.writerow(["alex@example.com", "Alex"])
                writer.writerow(["taylor@example.com", "Taylor"])
            dedupe_db_path = Path(temp_dir) / "outlook-dedupe.sqlite3"
            graph = GraphDefinition.from_dict(
                build_spreadsheet_outlook_draft_graph_payload(csv_path=str(csv_path))
            )
            graph.validate_against_services(self.services)

            with patch.dict(
                os.environ,
                {"GRAPH_AGENT_OUTLOOK_DEDUPE_DB": str(dedupe_db_path)},
                clear=False,
            ):
                first_state = runtime.run(graph, {"request": "draft rows"}, run_id="run-outlook-dedupe-1")
                second_state = runtime.run(graph, {"request": "draft rows"}, run_id="run-outlook-dedupe-2")

        self.assertEqual(first_state.status, "completed")
        self.assertEqual(second_state.status, "completed")
        self.assertEqual(len(fake_client.calls), 2)
        self.assertEqual(fake_auth.acquire_calls, 2)
        self.assertEqual(first_state.final_output["delivery_status"], "draft_saved")
        self.assertEqual(second_state.final_output["delivery_status"], "draft_saved_deduped")
        self.assertEqual(second_state.final_output["draft_id"], "draft-123")
        self.assertTrue(second_state.node_outputs["draft"]["delivery_status"].endswith("deduped"))

    def test_outlook_draft_node_dedupes_duplicate_edges_in_same_iteration(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["email", "name"])
                writer.writerow(["alex@example.com", "Alex"])
            dedupe_db_path = Path(temp_dir) / "outlook-dedupe.sqlite3"
            graph = GraphDefinition.from_dict(
                build_spreadsheet_outlook_draft_graph_payload(
                    csv_path=str(csv_path),
                    duplicate_loop_edges=True,
                )
            )
            graph.validate_against_services(self.services)

            with patch.dict(
                os.environ,
                {"GRAPH_AGENT_OUTLOOK_DEDUPE_DB": str(dedupe_db_path)},
                clear=False,
            ):
                state = runtime.run(graph, {"request": "draft rows"}, run_id="run-outlook-dedupe-edge")

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.visit_counts.get("draft"), 2)
        self.assertEqual(len(fake_client.calls), 1)
        self.assertEqual(fake_auth.acquire_calls, 1)
        self.assertEqual(state.final_output["delivery_status"], "draft_saved_deduped")
        self.assertEqual(state.final_output["draft_id"], "draft-123")

    def test_spreadsheet_iteration_resets_downstream_runtime_state_to_prevent_repeated_transition(self) -> None:
        fake_client = FakeOutlookDraftClient()
        fake_auth = FakeMicrosoftAuthService()
        self.services.outlook_draft_client = fake_client
        self.services.microsoft_auth_service = fake_auth
        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "rows.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["email", "name"])
                writer.writerow(["alex@example.com", "Alex"])
                writer.writerow(["taylor@example.com", "Taylor"])
            dedupe_db_path = Path(temp_dir) / "outlook-dedupe.sqlite3"
            graph = GraphDefinition.from_dict(
                build_spreadsheet_context_builder_outlook_graph_payload(csv_path=str(csv_path))
            )
            graph.validate_against_services(self.services)

            with patch.dict(
                os.environ,
                {"GRAPH_AGENT_OUTLOOK_DEDUPE_DB": str(dedupe_db_path)},
                clear=False,
            ):
                state = runtime.run(graph, {"request": "draft rows"}, run_id="run-outlook-context-reset")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(fake_client.calls), 2)
        self.assertEqual(fake_auth.acquire_calls, 2)
        display_to_draft_transitions = [
            event
            for event in state.event_history
            if event.event_type == "edge.selected"
            and isinstance(event.payload, dict)
            and event.payload.get("id") == "edge-display-draft"
        ]
        self.assertEqual(len(display_to_draft_transitions), 2)
        self.assertEqual(
            [event.payload.get("iteration_id") for event in display_to_draft_transitions],
            ["sheet:row:1", "sheet:row:2"],
        )


if __name__ == "__main__":
    unittest.main()
