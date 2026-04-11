from __future__ import annotations

import json
from pathlib import Path
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
import unittest

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
            raw_response={"id": "draft-123"},
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


class OutlookDraftNodeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def test_catalog_includes_outlook_draft_provider(self) -> None:
        provider_ids = {provider.provider_id for provider in self.services.node_provider_registry.list_definitions()}
        self.assertIn("end.outlook_draft", provider_ids)

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


if __name__ == "__main__":
    unittest.main()
