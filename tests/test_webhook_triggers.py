"""Tests for start.webhook listener and HTTP ingress."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore  # noqa: E402
from graph_agent.api.manager import GraphRunManager  # noqa: E402
from graph_agent.examples.tool_schema_repair import build_example_services  # noqa: E402
from graph_agent.providers.webhook import (  # noqa: E402
    WEBHOOK_START_PROVIDER_ID,
    WebhookHttpError,
    WebhookStartResolved,
    verify_webhook_request,
)


def build_isolated_store(services):
    import json as json_lib

    temp_dir = tempfile.TemporaryDirectory()
    bundled_path = Path(temp_dir.name) / "bundled_graphs.json"
    bundled_path.write_text(json_lib.dumps({"graphs": []}))
    store = GraphStore(
        services,
        path=Path(temp_dir.name) / "graphs.json",
        bundled_path=bundled_path,
    )
    return store, temp_dir


def minimal_webhook_graph(graph_id: str, *, slug: str = "wh_testslug42") -> dict:
    return {
        "graph_id": graph_id,
        "name": "Webhook graph",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "env_vars": {},
        "nodes": [
            {
                "id": "start",
                "kind": "input",
                "category": "start",
                "label": "Webhook Start",
                "provider_id": WEBHOOK_START_PROVIDER_ID,
                "provider_label": "Webhook Start",
                "description": "",
                "position": {"x": 0, "y": 0},
                "config": {
                    "trigger_mode": "webhook",
                    "webhook_path_slug": slug,
                    "http_methods": ["POST"],
                    "verification_mode": "none",
                    "webhook_secret_env_var": "{WEBHOOK_SECRET}",
                    "webhook_shared_secret_header": "X-Webhook-Secret",
                    "signature_header": "X-Signature",
                    "signature_prefix": "",
                    "event_type_json_path": "",
                    "event_type_allowlist": "",
                    "prompt": "",
                    "input_binding": {"type": "input_payload"},
                },
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
                "config": {"source_binding": {"type": "latest_envelope", "source": "start"}},
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


class WebhookVerificationTests(unittest.TestCase):
    def test_shared_secret_header(self) -> None:
        os.environ["WEBHOOK_SECRET"] = "abc"
        self.addCleanup(lambda: os.environ.pop("WEBHOOK_SECRET", None))
        r = WebhookStartResolved(
            graph_id="g1",
            slug="s",
            http_methods=("POST",),
            verification_mode="shared_secret",
            webhook_secret_env_var="{WEBHOOK_SECRET}",
            webhook_shared_secret_header="X-Webhook-Secret",
            signature_header="X-Signature",
            signature_prefix="",
            event_type_json_path="",
            event_type_allowlist=(),
            prompt="",
        )
        verify_webhook_request(
            r,
            {},
            "POST",
            {"x-webhook-secret": "abc"},
            b"{}",
        )

    def test_hmac_hex(self) -> None:
        secret = "s3cr3t"
        os.environ["WEBHOOK_SECRET"] = secret
        self.addCleanup(lambda: os.environ.pop("WEBHOOK_SECRET", None))
        body = b'{"x":1}'
        digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        r = WebhookStartResolved(
            graph_id="g1",
            slug="s",
            http_methods=("POST",),
            verification_mode="hmac_sha256",
            webhook_secret_env_var="{WEBHOOK_SECRET}",
            webhook_shared_secret_header="X-Webhook-Secret",
            signature_header="X-Signature",
            signature_prefix="",
            event_type_json_path="",
            event_type_allowlist=(),
            prompt="",
        )
        verify_webhook_request(
            r,
            {},
            "POST",
            {"x-signature": digest},
            body,
        )


class WebhookSlugUniquenessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def test_duplicate_slug_across_graphs_rejected(self) -> None:
        store, tmp = build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        store.create_graph(minimal_webhook_graph("graph-a", slug="wh_unique_dup"))
        with self.assertRaises(ValueError) as ctx:
            store.create_graph(minimal_webhook_graph("graph-b", slug="wh_unique_dup"))
        self.assertIn("already used", str(ctx.exception))


class WebhookDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()
        self._prev_webhook_ingress = os.environ.get("GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED")
        os.environ["GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED"] = "1"

        def _restore_webhook_ingress() -> None:
            if self._prev_webhook_ingress is None:
                os.environ.pop("GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED", None)
            else:
                os.environ["GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED"] = self._prev_webhook_ingress

        self.addCleanup(_restore_webhook_ingress)

    def _manager_with_tunnel_mock(self, store: GraphStore) -> GraphRunManager:
        tunnel = MagicMock()
        tunnel.get_status.return_value = {}
        return GraphRunManager(services=self.services, store=store, cloudflare_tunnel=tunnel)

    def test_webhook_starts_child_run(self) -> None:
        store, tmp = build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        store.create_graph(minimal_webhook_graph("wh-graph-1", slug="wh_dispatch_slug"))
        mgr = self._manager_with_tunnel_mock(store)
        run_id = mgr.start_listener_session("wh-graph-1")
        result = mgr.handle_inbound_webhook(
            "wh_dispatch_slug",
            "POST",
            "/api/webhooks/wh_dispatch_slug",
            "",
            [(b"content-type", b"application/json")],
            b'{"hello":"world"}',
        )
        self.assertTrue(result.get("ok"))
        child_id = str(result.get("run_id") or "")
        self.assertTrue(child_id)
        child = mgr.get_run(child_id)
        payload = child.get("input_payload")
        self.assertIsInstance(payload, dict)
        self.assertEqual(payload.get("source"), "webhook")
        self.assertEqual(payload.get("body", {}).get("hello"), "world")

        mgr.stop_listener_session(run_id, reason="user_initiated")

    def test_unknown_slug_raises(self) -> None:
        store, tmp = build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        mgr = self._manager_with_tunnel_mock(store)
        with self.assertRaises(WebhookHttpError) as ctx:
            mgr.handle_inbound_webhook(
                "missing",
                "POST",
                "/api/webhooks/missing",
                "",
                [],
                b"",
            )
        self.assertEqual(ctx.exception.status_code, 404)


class WebhookIngressDisabledTests(unittest.TestCase):
    """Without GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED, only loopback/private LAN clients may hit webhooks."""

    def setUp(self) -> None:
        self.services = build_example_services()
        self._prev_webhook_ingress = os.environ.get("GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED")
        os.environ["GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED"] = "0"

        def _restore_webhook_ingress() -> None:
            if self._prev_webhook_ingress is None:
                os.environ.pop("GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED", None)
            else:
                os.environ["GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED"] = self._prev_webhook_ingress

        self.addCleanup(_restore_webhook_ingress)

    def _manager_with_tunnel_mock(self, store: GraphStore) -> GraphRunManager:
        tunnel = MagicMock()
        tunnel.get_status.return_value = {}
        return GraphRunManager(services=self.services, store=store, cloudflare_tunnel=tunnel)

    def test_handle_inbound_webhook_forbidden_from_public_ip_when_disabled(self) -> None:
        store, tmp = build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        mgr = self._manager_with_tunnel_mock(store)
        with self.assertRaises(WebhookHttpError) as ctx:
            mgr.handle_inbound_webhook(
                "any-slug",
                "POST",
                "/api/webhooks/any-slug",
                "",
                [],
                b"",
                client_host="8.8.8.8",
            )
        self.assertEqual(ctx.exception.status_code, 403)

    def test_handle_inbound_webhook_localhost_allowed_when_ingress_disabled(self) -> None:
        store, tmp = build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        store.create_graph(minimal_webhook_graph("wh-local-graph", slug="wh_local_slug"))
        mgr = self._manager_with_tunnel_mock(store)
        run_id = mgr.start_listener_session("wh-local-graph")
        try:
            result = mgr.handle_inbound_webhook(
                "wh_local_slug",
                "POST",
                "/api/webhooks/wh_local_slug",
                "",
                [(b"content-type", b"application/json")],
                b"{}",
                client_host="127.0.0.1",
            )
            self.assertTrue(result.get("ok"))
        finally:
            mgr.stop_listener_session(run_id, reason="user_initiated")

    def test_start_listener_session_webhook_skips_tunnel_when_ingress_disabled(self) -> None:
        store, tmp = build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        store.create_graph(minimal_webhook_graph("wh-disabled-graph", slug="wh_disabled_slug"))
        mgr = self._manager_with_tunnel_mock(store)
        run_id = mgr.start_listener_session("wh-disabled-graph")
        try:
            mgr._cloudflare_tunnel.acquire_for_inbound_webhook.assert_not_called()  # noqa: SLF001
        finally:
            mgr.stop_listener_session(run_id, reason="user_initiated")


if __name__ == "__main__":
    unittest.main()
