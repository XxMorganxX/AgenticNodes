from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

from graph_agent.runtime.core import GraphValidationError, RuntimeServices
from graph_agent.runtime.documents import load_graph_document
from graph_agent.runtime.node_providers import DEFAULT_CATEGORY_CONTRACTS, list_connection_rules


class GraphStore:
    def __init__(
        self,
        services: RuntimeServices,
        path: Path | None = None,
        bundled_path: Path | None = None,
    ) -> None:
        self.services = services
        self.bundled_path = bundled_path or Path(__file__).resolve().with_name("graphs_store.json")
        self.path = path or Path(__file__).resolve().parents[3] / ".graph-agent" / "graphs_store.json"
        self._merged_graphs_cache: dict[str, dict[str, Any]] | None = None
        self._catalog_cache: dict[str, Any] | None = None
        self._ensure_user_store()

    def list_graphs(self) -> list[dict[str, Any]]:
        return [deepcopy(graph) for graph in self._merged_graphs().values()]

    def get_graph(self, graph_id: str) -> dict[str, Any]:
        graph = self._merged_graphs().get(graph_id)
        if graph is None:
            raise KeyError(graph_id)
        return deepcopy(graph)

    def create_graph(self, graph_payload: dict[str, Any]) -> dict[str, Any]:
        self._validate_graph_payload(graph_payload)
        normalized_graph = load_graph_document(graph_payload).to_dict()
        payload = self._load_user_all()
        existing_ids = set(self._merged_graphs())
        if normalized_graph["graph_id"] in existing_ids:
            raise ValueError(f"Graph '{normalized_graph['graph_id']}' already exists.")
        payload["graphs"].append(normalized_graph)
        self._save_user_all(payload)
        return deepcopy(normalized_graph)

    def update_graph(self, graph_id: str, graph_payload: dict[str, Any]) -> dict[str, Any]:
        self._validate_graph_payload(graph_payload)
        normalized_graph = load_graph_document(graph_payload).to_dict()
        if graph_id not in self._merged_graphs():
            raise KeyError(graph_id)

        payload = self._load_user_all()
        updated = False
        for index, graph in enumerate(payload["graphs"]):
            if graph["graph_id"] == graph_id:
                payload["graphs"][index] = normalized_graph
                updated = True
                break

        # Persist edits as user-local overrides so built-in sample graphs stay tracked.
        if not updated:
            payload["graphs"].append(normalized_graph)

        self._save_user_all(payload)
        return deepcopy(normalized_graph)

    def delete_graph(self, graph_id: str) -> None:
        payload = self._load_user_all()
        next_graphs = [graph for graph in payload["graphs"] if graph["graph_id"] != graph_id]
        if len(next_graphs) != len(payload["graphs"]):
            payload["graphs"] = next_graphs
            self._save_user_all(payload)
            return

        if graph_id in self._bundled_graph_ids():
            raise ValueError(f"Cannot delete built-in graph '{graph_id}'.")

        if len(next_graphs) == len(payload["graphs"]):
            raise KeyError(graph_id)

    def catalog(self) -> dict[str, Any]:
        if self._catalog_cache is None:
            self._catalog_cache = {
                "node_providers": [
                    provider.to_dict() for provider in self.services.node_provider_registry.list_definitions()
                ],
                "connection_rules": [rule.to_dict() for rule in list_connection_rules()],
                "contracts": {
                    category.value: contract.to_dict()
                    for category, contract in DEFAULT_CATEGORY_CONTRACTS.items()
                },
            }
        return {
            **deepcopy(self._catalog_cache),
            "tools": [tool.to_dict() for tool in self.services.tool_registry.list_definitions()],
        }

    def _validate_graph_payload(self, payload: dict[str, Any]) -> None:
        try:
            graph = load_graph_document(payload)
            graph.validate_against_services(self.services)
        except (GraphValidationError, KeyError, TypeError, ValueError) as exc:
            raise ValueError(str(exc)) from exc

    def _ensure_user_store(self) -> None:
        if self.path.exists():
            return
        self._save_user_all({"graphs": []})

    def _load_bundled_all(self) -> dict[str, Any]:
        return json.loads(self.bundled_path.read_text())

    def _load_user_all(self) -> dict[str, Any]:
        return json.loads(self.path.read_text())

    def _save_user_all(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, indent=2))
        self._invalidate_caches()

    def _bundled_graph_ids(self) -> set[str]:
        return {graph["graph_id"] for graph in self._load_bundled_all()["graphs"]}

    def _merged_graphs(self) -> dict[str, dict[str, Any]]:
        if self._merged_graphs_cache is None:
            merged = {
                graph["graph_id"]: load_graph_document(graph).to_dict() for graph in self._load_bundled_all()["graphs"]
            }
            for graph in self._load_user_all()["graphs"]:
                merged[graph["graph_id"]] = load_graph_document(graph).to_dict()
            self._merged_graphs_cache = merged
        return self._merged_graphs_cache

    def _invalidate_caches(self) -> None:
        self._merged_graphs_cache = None
        self._catalog_cache = None
