"""Tests for start.supabase_row_event listener and HTTP ingress."""

from __future__ import annotations

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
    SUPABASE_ROW_EVENT_PROVIDER_ID,
    WEBHOOK_START_PROVIDER_ID,
    WebhookHttpError,
    WebhookStartResolved,
    build_supabase_row_event_child_payload,
    parse_supabase_event_allowlist,
    parse_supabase_table_allowlist,
    passes_supabase_event_filter,
)
from graph_agent.runtime.core import GraphValidationError  # noqa: E402


def _build_isolated_store(services):
    temp_dir = tempfile.TemporaryDirectory()
    bundled_path = Path(temp_dir.name) / "bundled_graphs.json"
    bundled_path.write_text(json.dumps({"graphs": []}))
    store = GraphStore(
        services,
        path=Path(temp_dir.name) / "graphs.json",
        bundled_path=bundled_path,
    )
    return store, temp_dir


def _minimal_supabase_webhook_graph(
    graph_id: str,
    *,
    slug: str = "sb_rows_test",
    connection_id: str = "primary-db",
    events: list[str] | None = None,
    tables: list[dict] | list[str] | None = None,
    include_connection: bool = True,
) -> dict:
    events = events if events is not None else ["INSERT"]
    tables = tables if tables is not None else [{"schema": "public", "table": "leads"}]
    graph: dict = {
        "graph_id": graph_id,
        "name": "Supabase row event graph",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "env_vars": {},
        "nodes": [
            {
                "id": "start",
                "kind": "input",
                "category": "start",
                "label": "Supabase Row Event",
                "provider_id": SUPABASE_ROW_EVENT_PROVIDER_ID,
                "provider_label": "Supabase Row Event",
                "description": "",
                "position": {"x": 0, "y": 0},
                "config": {
                    "trigger_mode": "supabase_row_event",
                    "supabase_connection_id": connection_id,
                    "webhook_path_slug": slug,
                    "event_allowlist": events,
                    "table_allowlist": tables,
                    "verification_mode": "none",
                    "webhook_secret_env_var": "{SUPABASE_WEBHOOK_SECRET}",
                    "webhook_shared_secret_header": "x-supabase-signature",
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
    if include_connection:
        graph["supabase_connections"] = [
            {
                "connection_id": connection_id,
                "name": "Primary DB",
                "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
                "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
                "project_ref_env_var": "SUPABASE_PROJECT_REF",
                "access_token_env_var": "SUPABASE_ACCESS_TOKEN",
            }
        ]
        graph["default_supabase_connection_id"] = connection_id
    return graph


def _minimal_webhook_graph(graph_id: str, *, slug: str) -> dict:
    return {
        "graph_id": graph_id,
        "name": "Webhook",
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


def _resolved_supabase(events=("INSERT",), tables=(("public", "leads"),)) -> WebhookStartResolved:
    return WebhookStartResolved(
        graph_id="g1",
        slug="sb_test_slug",
        http_methods=("POST",),
        verification_mode="none",
        webhook_secret_env_var="",
        webhook_shared_secret_header="",
        signature_header="",
        signature_prefix="",
        event_type_json_path="",
        event_type_allowlist=(),
        prompt="",
        kind="supabase_row_event",
        supabase_connection_id="primary-db",
        supabase_event_allowlist=tuple(events),
        supabase_table_allowlist=tuple(tables),
    )


class SupabaseRowEventCatalogTests(unittest.TestCase):
    def test_provider_registered_with_inbound_webhook_transport(self) -> None:
        services = build_example_services()
        definition = services.node_provider_registry.get(SUPABASE_ROW_EVENT_PROVIDER_ID)
        self.assertEqual(definition.trigger_mode, "listener")
        self.assertEqual(definition.listener_transport, "inbound_webhook")
        self.assertEqual(definition.node_kind, "input")
        self.assertEqual(definition.default_config.get("verification_mode"), "shared_secret")


class SupabaseRowEventParseTests(unittest.TestCase):
    def test_parse_event_allowlist_normalizes_and_dedupes(self) -> None:
        self.assertEqual(parse_supabase_event_allowlist(["insert", "UPDATE", "insert"]), ("INSERT", "UPDATE"))
        self.assertEqual(parse_supabase_event_allowlist("INSERT, DELETE"), ("INSERT", "DELETE"))
        self.assertEqual(parse_supabase_event_allowlist(["bogus"]), ())

    def test_parse_table_allowlist_accepts_dicts_and_strings(self) -> None:
        self.assertEqual(
            parse_supabase_table_allowlist([{"schema": "public", "table": "leads"}]),
            (("public", "leads"),),
        )
        self.assertEqual(
            parse_supabase_table_allowlist("public.leads\norders"),
            (("public", "leads"), ("public", "orders")),
        )
        self.assertEqual(parse_supabase_table_allowlist([""]), ())


class SupabaseRowEventFilterTests(unittest.TestCase):
    def test_inserts_on_allowed_table_pass(self) -> None:
        resolved = _resolved_supabase()
        body = {"type": "INSERT", "schema": "public", "table": "leads", "record": {"id": 1}}
        matched, reason = passes_supabase_event_filter(resolved, body)
        self.assertTrue(matched)
        self.assertIsNone(reason)

    def test_update_drops_when_only_insert_allowed(self) -> None:
        resolved = _resolved_supabase(events=("INSERT",))
        body = {"type": "UPDATE", "schema": "public", "table": "leads"}
        matched, reason = passes_supabase_event_filter(resolved, body)
        self.assertFalse(matched)
        self.assertEqual(reason, "event_not_in_allowlist")

    def test_table_not_in_allowlist_drops(self) -> None:
        resolved = _resolved_supabase()
        body = {"type": "INSERT", "schema": "public", "table": "orders"}
        matched, reason = passes_supabase_event_filter(resolved, body)
        self.assertFalse(matched)
        self.assertEqual(reason, "table_not_in_allowlist")

    def test_missing_event_type_drops(self) -> None:
        resolved = _resolved_supabase()
        body = {"schema": "public", "table": "leads"}
        matched, reason = passes_supabase_event_filter(resolved, body)
        self.assertFalse(matched)
        self.assertEqual(reason, "missing_event_type")

    def test_non_dict_body_drops(self) -> None:
        matched, reason = passes_supabase_event_filter(_resolved_supabase(), "not-an-object")
        self.assertFalse(matched)
        self.assertEqual(reason, "body_not_json_object")


class SupabaseRowEventPayloadTests(unittest.TestCase):
    def test_flat_payload_shape(self) -> None:
        body = {
            "type": "INSERT",
            "schema": "public",
            "table": "leads",
            "record": {"id": 1, "email": "a@b.com"},
            "old_record": None,
        }
        payload = build_supabase_row_event_child_payload(
            graph_id="g1",
            parsed_body=body,
            supabase_connection_id="primary-db",
            prompt="say hi",
            listener_agent_id="agent-7",
        )
        self.assertEqual(payload["source"], "supabase_row_event")
        self.assertEqual(payload["event_type"], "INSERT")
        self.assertEqual(payload["schema"], "public")
        self.assertEqual(payload["table"], "leads")
        self.assertEqual(payload["record"], {"id": 1, "email": "a@b.com"})
        self.assertIsNone(payload["old_record"])
        self.assertEqual(payload["supabase_connection_id"], "primary-db")
        self.assertEqual(payload["prompt"], "say hi")
        self.assertEqual(payload["listener_agent_id"], "agent-7")
        self.assertEqual(payload["raw_webhook"], body)


class SupabaseRowEventValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def test_missing_connection_id_rejected(self) -> None:
        store, tmp = _build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        graph = _minimal_supabase_webhook_graph("sb-graph-missing-conn", connection_id="")
        graph["supabase_connections"] = []
        graph["default_supabase_connection_id"] = ""
        with self.assertRaises(ValueError) as ctx:
            store.create_graph(graph)
        self.assertIn("supabase_connection_id", str(ctx.exception))

    def test_unknown_connection_id_rejected(self) -> None:
        store, tmp = _build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        graph = _minimal_supabase_webhook_graph("sb-graph-bad-conn", connection_id="ghost")
        graph["supabase_connections"] = []
        graph["default_supabase_connection_id"] = ""
        with self.assertRaises(ValueError) as ctx:
            store.create_graph(graph)
        self.assertIn("ghost", str(ctx.exception))

    def test_empty_event_allowlist_rejected(self) -> None:
        store, tmp = _build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        graph = _minimal_supabase_webhook_graph("sb-graph-no-events", events=[])
        with self.assertRaises(ValueError) as ctx:
            store.create_graph(graph)
        self.assertIn("event", str(ctx.exception).lower())

    def test_empty_table_allowlist_rejected(self) -> None:
        store, tmp = _build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        graph = _minimal_supabase_webhook_graph("sb-graph-no-tables", tables=[])
        with self.assertRaises(ValueError) as ctx:
            store.create_graph(graph)
        self.assertIn("table", str(ctx.exception).lower())


class SupabaseRowEventSlugUniquenessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def test_collides_with_start_webhook_slug(self) -> None:
        store, tmp = _build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        store.create_graph(_minimal_webhook_graph("graph-a", slug="shared_slug_99"))
        with self.assertRaises(ValueError) as ctx:
            store.create_graph(_minimal_supabase_webhook_graph("graph-b", slug="shared_slug_99"))
        self.assertIn("already used", str(ctx.exception))


class SupabaseRowEventDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()
        self._prev = os.environ.get("GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED")
        os.environ["GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED"] = "1"

        def _restore() -> None:
            if self._prev is None:
                os.environ.pop("GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED", None)
            else:
                os.environ["GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED"] = self._prev

        self.addCleanup(_restore)

    def _manager(self, store: GraphStore) -> GraphRunManager:
        tunnel = MagicMock()
        tunnel.get_status.return_value = {}
        return GraphRunManager(services=self.services, store=store, cloudflare_tunnel=tunnel)

    def test_insert_event_starts_child_run_with_flat_payload(self) -> None:
        store, tmp = _build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        store.create_graph(_minimal_supabase_webhook_graph("sb-dispatch-1", slug="sb_dispatch_test"))
        mgr = self._manager(store)
        run_id = mgr.start_listener_session("sb-dispatch-1")
        try:
            body = json.dumps(
                {
                    "type": "INSERT",
                    "schema": "public",
                    "table": "leads",
                    "record": {"id": 42, "email": "a@b.com"},
                    "old_record": None,
                }
            ).encode("utf-8")
            result = mgr.handle_inbound_webhook(
                "sb_dispatch_test",
                "POST",
                "/api/webhooks/sb_dispatch_test",
                "",
                [(b"content-type", b"application/json")],
                body,
            )
            self.assertTrue(result.get("ok"))
            child_id = str(result.get("run_id") or "")
            self.assertTrue(child_id)
            child = mgr.get_run(child_id)
            payload = child.get("input_payload")
            self.assertIsInstance(payload, dict)
            self.assertEqual(payload.get("source"), "supabase_row_event")
            self.assertEqual(payload.get("event_type"), "INSERT")
            self.assertEqual(payload.get("table"), "leads")
            self.assertEqual(payload.get("record"), {"id": 42, "email": "a@b.com"})
            self.assertEqual(payload.get("supabase_connection_id"), "primary-db")
        finally:
            mgr.stop_listener_session(run_id, reason="user_initiated")

    def test_event_not_in_allowlist_returns_filtered(self) -> None:
        store, tmp = _build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        store.create_graph(
            _minimal_supabase_webhook_graph(
                "sb-dispatch-2",
                slug="sb_dispatch_filter",
                events=["INSERT"],
            )
        )
        mgr = self._manager(store)
        run_id = mgr.start_listener_session("sb-dispatch-2")
        try:
            body = json.dumps(
                {"type": "UPDATE", "schema": "public", "table": "leads", "record": {"id": 1}}
            ).encode("utf-8")
            result = mgr.handle_inbound_webhook(
                "sb_dispatch_filter",
                "POST",
                "/api/webhooks/sb_dispatch_filter",
                "",
                [(b"content-type", b"application/json")],
                body,
            )
            self.assertTrue(result.get("filtered"))
            self.assertEqual(result.get("reason"), "event_not_in_allowlist")
            self.assertNotIn("run_id", result)
        finally:
            mgr.stop_listener_session(run_id, reason="user_initiated")

    def test_table_not_in_allowlist_returns_filtered(self) -> None:
        store, tmp = _build_isolated_store(self.services)
        self.addCleanup(tmp.cleanup)
        store.create_graph(_minimal_supabase_webhook_graph("sb-dispatch-3", slug="sb_dispatch_tbl"))
        mgr = self._manager(store)
        run_id = mgr.start_listener_session("sb-dispatch-3")
        try:
            body = json.dumps(
                {"type": "INSERT", "schema": "public", "table": "orders", "record": {"id": 1}}
            ).encode("utf-8")
            result = mgr.handle_inbound_webhook(
                "sb_dispatch_tbl",
                "POST",
                "/api/webhooks/sb_dispatch_tbl",
                "",
                [(b"content-type", b"application/json")],
                body,
            )
            self.assertTrue(result.get("filtered"))
            self.assertEqual(result.get("reason"), "table_not_in_allowlist")
        finally:
            mgr.stop_listener_session(run_id, reason="user_initiated")


if __name__ == "__main__":
    unittest.main()
