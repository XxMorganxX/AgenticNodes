"""Tests for managed Cloudflare tunnel subprocess lifecycle."""

from __future__ import annotations

import io
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from graph_agent.api.cloudflare_store import CloudflareConfigStore
from graph_agent.api.cloudflare_tunnel import CloudflareTunnelManager


class BlockingCloudflaredProc:
    """Minimal subprocess stand-in: poll None until terminated; wait blocks until then."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self._done = threading.Event()
        self.stdout = io.BytesIO(b"")
        self.stderr = io.BytesIO(b"")

    def poll(self) -> int | None:
        return 0 if self._done.is_set() else None

    def wait(self) -> int:
        self._done.wait(timeout=60.0)
        return 0

    def terminate(self) -> None:
        self._done.set()

    def kill(self) -> None:
        self._done.set()


class CloudflareTunnelManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        store_path = Path(self._tmp.name) / "cloudflare_config.json"
        self.store = CloudflareConfigStore(path=store_path)

    def test_acquire_requires_token(self) -> None:
        mgr = CloudflareTunnelManager(
            self.store,
            environ={"PATH": "/usr/bin"},
            which_cloudflared=lambda: "/bin/cloudflared",
            subprocess_popen=MagicMock(),
        )
        with self.assertRaises(RuntimeError) as ctx:
            mgr.acquire_for_inbound_webhook("g1")
        self.assertIn("not set or empty", str(ctx.exception))

    def test_acquire_starts_once_shared_across_graphs(self) -> None:
        pops: list[BlockingCloudflaredProc] = []

        def fake_popen(*args: object, **kwargs: object) -> BlockingCloudflaredProc:
            proc = BlockingCloudflaredProc(4242 + len(pops))
            pops.append(proc)
            return proc

        mgr = CloudflareTunnelManager(
            self.store,
            environ={
                "CLOUDFLARE_TUNNEL_TOKEN": "fake-token",
                "PATH": "/usr/bin",
            },
            which_cloudflared=lambda: "/bin/cloudflared",
            subprocess_popen=fake_popen,
        )
        mgr.acquire_for_inbound_webhook("ga")
        mgr.acquire_for_inbound_webhook("gb")
        self.assertEqual(len(pops), 1)
        st = mgr.get_status()
        self.assertEqual(st["tunnel_state"], "running")
        self.assertEqual(st["tunnel_ref_count"], 2)
        self.assertEqual(sorted(st["tunnel_active_graph_ids"]), ["ga", "gb"])
        mgr.shutdown()

    def test_release_stops_when_last_graph(self) -> None:
        proc = BlockingCloudflaredProc(9001)

        mgr = CloudflareTunnelManager(
            self.store,
            environ={"CLOUDFLARE_TUNNEL_TOKEN": "x", "PATH": "/usr/bin"},
            which_cloudflared=lambda: "/bin/cloudflared",
            subprocess_popen=lambda *a, **k: proc,
        )
        mgr.acquire_for_inbound_webhook("g1")
        mgr.release_for_inbound_webhook("g1")
        self.assertTrue(proc._done.is_set())

    def test_acquire_failure_removes_graph_ref(self) -> None:
        def boom(*args: object, **kwargs: object) -> BlockingCloudflaredProc:
            raise OSError("spawn failed")

        mgr = CloudflareTunnelManager(
            self.store,
            environ={"CLOUDFLARE_TUNNEL_TOKEN": "x", "PATH": "/usr/bin"},
            which_cloudflared=lambda: "/bin/cloudflared",
            subprocess_popen=boom,
        )
        with self.assertRaises(RuntimeError):
            mgr.acquire_for_inbound_webhook("gx")
        self.assertEqual(mgr.get_status()["tunnel_ref_count"], 0)

    def test_shutdown_clears_refs(self) -> None:
        proc = BlockingCloudflaredProc(33)

        mgr = CloudflareTunnelManager(
            self.store,
            environ={"CLOUDFLARE_TUNNEL_TOKEN": "x", "PATH": "/usr/bin"},
            which_cloudflared=lambda: "/bin/cloudflared",
            subprocess_popen=lambda *a, **k: proc,
        )
        mgr.acquire_for_inbound_webhook("g1")
        mgr.shutdown()
        self.assertEqual(mgr.get_status()["tunnel_ref_count"], 0)
        self.assertTrue(proc._done.is_set())


if __name__ == "__main__":
    unittest.main()
