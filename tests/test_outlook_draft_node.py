from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.providers.outlook import OutlookDraftClient, OutlookDraftResult
from graph_agent.runtime.core import GraphDefinition
from graph_agent.runtime.engine import GraphRuntime
from graph_agent.runtime.microsoft_auth import MicrosoftAuthStatus


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
    ) -> OutlookDraftResult:
        self.calls.append(
            {
                "access_token": access_token,
                "to_recipients": list(to_recipients),
                "subject": subject,
                "body": body,
            }
        )
        return OutlookDraftResult(
            draft_id="draft-123",
            subject=subject,
            body=body,
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
        return "graph-token"


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
        if self.path == "/rest/v1/outbound_email_messages":
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
        self.assertEqual(state.node_outputs["draft"]["delivery_status"], "draft_saved")
        self.assertEqual(state.node_outputs["draft"]["draft_id"], "draft-123")
        self.assertEqual(state.node_outputs["draft"]["account_username"], "morgan@example.com")
        self.assertEqual(state.final_output["subject"], "Follow-up for outlook-draft-agent")
        self.assertEqual(state.final_output["body"], "Draft this exact email body.")

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

    def test_outlook_draft_node_skips_outbound_email_log_when_recipient_is_missing(self) -> None:
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
            state = runtime.run(graph, "", run_id="run-outlook-log-skip")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(fake_client.calls), 1)
        self.assertEqual(fake_client.calls[0]["to_recipients"], [])
        self.assertEqual(state.final_output["to_recipients"], [])
        self.assertEqual(state.final_output["outbound_email_log"]["reason"], "missing_recipient_email")
        self.assertTrue(state.final_output["outbound_email_log"]["skipped"])
        self.assertEqual(state.final_output["outbound_email_log"]["table_name"], "outbound_email_messages")
        self.assertEqual(_SupabaseEmailLogStubHandler.last_path, "/rest/v1/")
        self.assertIsNone(_SupabaseEmailLogStubHandler.last_json_body)


if __name__ == "__main__":
    unittest.main()
