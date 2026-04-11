from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
from threading import Event
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.runtime.microsoft_auth import MicrosoftAuthService


class FakeMicrosoftApplication:
    def __init__(self, state: dict[str, object], client_id: str, authority: str) -> None:
        self._state = state
        self.client_id = client_id
        self.authority = authority

    def initiate_device_flow(self, *, scopes: list[str]) -> dict[str, object]:
        self._state["scopes"] = list(scopes)
        return {
            "user_code": "ABCD-EFGH",
            "verification_uri": "https://microsoft.com/devicelogin",
            "verification_uri_complete": "https://microsoft.com/devicelogin?otc=ABCD-EFGH",
            "message": "Use code ABCD-EFGH to sign in.",
            "expires_at": "2026-04-11T12:00:00Z",
        }

    def acquire_token_by_device_flow(self, flow: dict[str, object]) -> dict[str, object]:
        release_event = self._state["release_event"]
        assert isinstance(release_event, Event)
        release_event.wait(timeout=2)
        if flow.get("expires_at") == 0:
            return {"error": "authorization_declined", "error_description": "Device flow was cancelled."}
        self._state["accounts"] = [{"username": "morgan@example.com"}]
        return {
            "access_token": "device-token",
            "id_token_claims": {"preferred_username": "morgan@example.com"},
        }

    def get_accounts(self) -> list[dict[str, object]]:
        accounts = self._state.get("accounts", [])
        return list(accounts) if isinstance(accounts, list) else []

    def acquire_token_silent(self, scopes: list[str], account: dict[str, object]) -> dict[str, object] | None:
        if account:
            return {"access_token": str(self._state.get("silent_token", "silent-token"))}
        return None

    def acquire_token_silent_with_error(self, scopes: list[str], account: dict[str, object]) -> dict[str, object]:
        return {"error": "invalid_grant", "error_description": "No cached account."}


def build_test_graph_payload() -> dict[str, object]:
    return {
        "graph_id": "microsoft-security-graph",
        "name": "Microsoft Security Graph",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "env_vars": {
            "MICROSOFT_GRAPH_ACCESS_TOKEN": "super-secret-token",
            "OPENAI_API_KEY": "OPENAI_API_KEY",
        },
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
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "description": "",
                "position": {"x": 280, "y": 0},
                "config": {"source_binding": {"type": "input_payload"}},
            },
        ],
        "edges": [
            {
                "id": "edge-start-finish",
                "source_id": "start",
                "target_id": "finish",
                "label": "complete",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            }
        ],
    }


class MicrosoftAuthTests(unittest.TestCase):
    def test_device_code_flow_completes_and_silent_token_reuse_works(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            release_event = Event()
            shared_state: dict[str, object] = {
                "release_event": release_event,
                "accounts": [],
                "silent_token": "silent-token",
            }
            service = MicrosoftAuthService(
                state_dir=Path(temp_dir),
                application_factory=lambda client_id, authority, token_cache: FakeMicrosoftApplication(
                    shared_state,
                    client_id,
                    authority,
                ),
                token_cache_factory=lambda cache_path: {"cache_path": str(cache_path)},
            )

            status = service.start_device_code(client_id="client-123", tenant_id="tenant-456")
            self.assertTrue(status.pending)
            self.assertEqual(status.user_code, "ABCD-EFGH")
            self.assertEqual(status.verification_uri, "https://microsoft.com/devicelogin")

            release_event.set()
            for _ in range(40):
                current = service.connection_status()
                if current.connected:
                    break
                time.sleep(0.02)
            else:
                self.fail("Microsoft device-code flow did not reach connected status in time.")

            self.assertTrue(current.connected)
            self.assertEqual(current.account_username, "morgan@example.com")
            self.assertEqual(shared_state["scopes"], ["Mail.ReadWrite"])
            self.assertEqual(service.acquire_access_token(), "silent-token")

            cache_path = Path(temp_dir) / "token-cache.bin"
            cache_path.write_text("cached")
            disconnected = service.disconnect()
            self.assertEqual(disconnected.status, "disconnected")
            self.assertFalse((Path(temp_dir) / "settings.json").exists())
            self.assertFalse(cache_path.exists())

    def test_graph_store_strips_microsoft_token_from_persisted_graphs(self) -> None:
        services = build_example_services()
        with tempfile.TemporaryDirectory() as temp_dir:
            bundled_path = Path(temp_dir) / "bundled_graphs.json"
            bundled_path.write_text(json.dumps({"graphs": []}))
            store_path = Path(temp_dir) / "graphs.json"
            store = GraphStore(services=services, path=store_path, bundled_path=bundled_path)

            created = store.create_graph(build_test_graph_payload())

            self.assertNotIn("MICROSOFT_GRAPH_ACCESS_TOKEN", created.get("env_vars", {}))
            self.assertEqual(created.get("env_vars", {}).get("OPENAI_API_KEY"), "OPENAI_API_KEY")
            persisted_text = store_path.read_text()
            self.assertNotIn("super-secret-token", persisted_text)
            self.assertNotIn("MICROSOFT_GRAPH_ACCESS_TOKEN", persisted_text)


if __name__ == "__main__":
    unittest.main()
