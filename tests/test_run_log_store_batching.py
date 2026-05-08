from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.run_log_store import FilesystemRunStore, _EVENT_APPEND_BATCH_MAX


def _minimal_event(run_id: str, index: int) -> dict[str, object]:
    return {
        "schema_version": "runtime.v1",
        "event_type": "node.started",
        "summary": f"evt-{index}",
        "payload": {"node_id": "n1", "visit_count": 1, "received_input": None},
        "run_id": run_id,
        "agent_id": None,
        "parent_run_id": None,
        "timestamp": f"2020-01-01T00:00:{index:02d}Z",
    }


class FilesystemRunStoreBatchingTests(unittest.TestCase):
    def test_batched_appends_preserve_order_after_flush(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FilesystemRunStore(root=root)
            run_id = "batch-order-run"
            store.initialize_run(
                {
                    "run_id": run_id,
                    "graph_id": "g1",
                    "input_payload": "",
                    "documents": [],
                },
            )
            total = _EVENT_APPEND_BATCH_MAX + 12
            for i in range(total):
                store.append_event(run_id, _minimal_event(run_id, i))
            store.flush()
            loaded = store.load_events(run_id)
            self.assertEqual(len(loaded), total)
            for i, event in enumerate(loaded):
                self.assertEqual(event["summary"], f"evt-{i}")

    def test_write_state_flushes_buffered_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = FilesystemRunStore(root=root)
            run_id = "flush-on-state-run"
            store.initialize_run(
                {
                    "run_id": run_id,
                    "graph_id": "g1",
                    "input_payload": "",
                    "documents": [],
                },
            )
            store.append_event(run_id, _minimal_event(run_id, 0))
            events_path = root / run_id / "events.jsonl"
            self.assertFalse(events_path.exists())
            store.write_state(
                run_id,
                {
                    "run_id": run_id,
                    "graph_id": "g1",
                    "status": "running",
                    "event_history": [],
                    "transition_history": [],
                    "event_count": 0,
                    "transition_count": 0,
                },
            )
            self.assertTrue(events_path.exists())
            lines = [line for line in events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual(len(lines), 1)
            row = json.loads(lines[0])
            self.assertEqual(row["summary"], "evt-0")


if __name__ == "__main__":
    unittest.main()
