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

from graph_agent.api.graph_store import GraphStore
from graph_agent.examples.tool_schema_repair import build_example_services


class GraphStorePersistenceTests(unittest.TestCase):
    def test_recovers_concatenated_user_store_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp_path = Path(directory)
            bundled_path = temp_path / "bundled_graphs.json"
            user_path = temp_path / "graphs_store.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            user_path.write_text(
                json.dumps({"graphs": [], "deleted_graph_ids": ["stale"]})
                + json.dumps({"graphs": [], "deleted_graph_ids": ["current"]}),
                encoding="utf-8",
            )

            store = GraphStore(build_example_services(), path=user_path, bundled_path=bundled_path)

            self.assertEqual(store._load_user_all()["deleted_graph_ids"], ["current"])
            self.assertEqual(json.loads(user_path.read_text(encoding="utf-8"))["deleted_graph_ids"], ["current"])

    def test_quarantines_unrecoverable_user_store_payload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            temp_path = Path(directory)
            bundled_path = temp_path / "bundled_graphs.json"
            user_path = temp_path / "graphs_store.json"
            bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
            user_path.write_text('{"graphs": []} trailing garbage', encoding="utf-8")

            store = GraphStore(build_example_services(), path=user_path, bundled_path=bundled_path)

            self.assertEqual(store.list_graphs(), [])
            self.assertEqual(json.loads(user_path.read_text(encoding="utf-8")), {"graphs": [], "deleted_graph_ids": []})
            self.assertEqual((temp_path / "graphs_store.json.invalid").read_text(encoding="utf-8"), '{"graphs": []} trailing garbage')


if __name__ == "__main__":
    unittest.main()
