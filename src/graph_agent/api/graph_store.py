from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any

from graph_agent.providers.webhook import WEBHOOK_START_PROVIDER_ID, normalize_webhook_slug
from graph_agent.runtime.core import GraphDefinition, GraphValidationError, RuntimeServices
from graph_agent.runtime.documents import AgentDefinition, TestEnvironmentDefinition, load_graph_document


def _environment_agent_graph(document: TestEnvironmentDefinition, agent: AgentDefinition) -> GraphDefinition:
    return agent.to_graph(
        graph_id=document.graph_id,
        shared_env_vars=document.env_vars,
        supabase_connections=document.supabase_connections,
        default_supabase_connection_id=document.default_supabase_connection_id,
        run_store_supabase_connection_id=document.run_store_supabase_connection_id,
    )
from graph_agent.runtime.node_providers import DEFAULT_CATEGORY_CONTRACTS, list_connection_rules


NON_PERSISTED_GRAPH_ENV_KEYS = {"MICROSOFT_GRAPH_ACCESS_TOKEN"}


def _sanitize_env_var_mapping(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    return {
        str(key).strip(): str(value if isinstance(value, str) else value or "")
        for key, value in payload.items()
        if str(key).strip() and str(key).strip() not in NON_PERSISTED_GRAPH_ENV_KEYS
    }


def _sanitize_graph_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized = deepcopy(payload)
    if "env_vars" in sanitized:
        sanitized["env_vars"] = _sanitize_env_var_mapping(sanitized.get("env_vars"))
    agents = sanitized.get("agents")
    if isinstance(agents, list):
        sanitized["agents"] = [
            {
                **agent,
                "env_vars": _sanitize_env_var_mapping(agent.get("env_vars")),
            }
            if isinstance(agent, dict)
            else agent
            for agent in agents
        ]
    return sanitized


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
        self._sanitize_user_store()

    def list_graphs(self) -> list[dict[str, Any]]:
        return [deepcopy(graph) for graph in self._merged_graphs().values()]

    def get_graph(self, graph_id: str) -> dict[str, Any]:
        graph = self._merged_graphs().get(graph_id)
        if graph is None:
            raise KeyError(graph_id)
        return deepcopy(graph)

    def create_graph(self, graph_payload: dict[str, Any]) -> dict[str, Any]:
        sanitized_payload = _sanitize_graph_payload(graph_payload)
        self._validate_graph_payload(sanitized_payload)
        normalized_graph = load_graph_document(sanitized_payload).to_dict()
        payload = self._load_user_all()
        existing_ids = set(self._merged_graphs())
        if normalized_graph["graph_id"] in existing_ids:
            raise ValueError(f"Graph '{normalized_graph['graph_id']}' already exists.")
        payload["deleted_graph_ids"] = [
            graph_id for graph_id in payload["deleted_graph_ids"] if graph_id != normalized_graph["graph_id"]
        ]
        payload["graphs"].append(normalized_graph)
        self._save_user_all(payload)
        return deepcopy(normalized_graph)

    def update_graph(self, graph_id: str, graph_payload: dict[str, Any]) -> dict[str, Any]:
        sanitized_payload = _sanitize_graph_payload(graph_payload)
        self._validate_graph_payload(sanitized_payload)
        normalized_graph = load_graph_document(sanitized_payload).to_dict()
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

        payload["deleted_graph_ids"] = [
            deleted_graph_id
            for deleted_graph_id in payload["deleted_graph_ids"]
            if deleted_graph_id != normalized_graph["graph_id"]
        ]
        self._save_user_all(payload)
        return deepcopy(normalized_graph)

    def delete_graph(self, graph_id: str) -> None:
        if graph_id not in self._merged_graphs():
            raise KeyError(graph_id)

        payload = self._load_user_all()
        next_graphs = [graph for graph in payload["graphs"] if graph["graph_id"] != graph_id]
        if graph_id in self._bundled_graph_ids():
            deleted_graph_ids = set(payload["deleted_graph_ids"])
            deleted_graph_ids.add(graph_id)
            payload["deleted_graph_ids"] = sorted(deleted_graph_ids)

        payload["graphs"] = next_graphs
        self._save_user_all(payload)

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
            self._validate_webhook_slug_uniqueness(payload)
        except (GraphValidationError, KeyError, TypeError, ValueError) as exc:
            raise ValueError(str(exc)) from exc

    def _collect_webhook_slugs(self, doc: TestEnvironmentDefinition) -> list[str]:
        slugs: list[str] = []
        if doc.is_multi_agent:
            for agent in doc.agents:
                graph = _environment_agent_graph(doc, agent)
                if graph.start_node().provider_id != WEBHOOK_START_PROVIDER_ID:
                    continue
                slug = normalize_webhook_slug(graph.start_node().raw_config.get("webhook_path_slug"))
                if slug:
                    slugs.append(slug)
            return slugs
        graph = doc.as_graph()
        if graph.start_node().provider_id != WEBHOOK_START_PROVIDER_ID:
            return slugs
        slug = normalize_webhook_slug(graph.start_node().raw_config.get("webhook_path_slug"))
        if slug:
            slugs.append(slug)
        return slugs

    def _validate_webhook_slug_uniqueness(self, payload: dict[str, Any]) -> None:
        try:
            doc = load_graph_document(payload)
        except (KeyError, TypeError, ValueError):
            return
        current_slugs = self._collect_webhook_slugs(doc)
        if not current_slugs:
            return
        if len(current_slugs) != len(set(current_slugs)):
            raise ValueError(
                "Each agent using start.webhook in the same environment must have a distinct webhook_path_slug."
            )
        current_id = str(doc.graph_id or "").strip()
        for gid, gp in self._merged_graphs().items():
            if gid == current_id:
                continue
            try:
                other = load_graph_document(gp)
            except (KeyError, TypeError, ValueError):
                continue
            for other_slug in self._collect_webhook_slugs(other):
                if other_slug in current_slugs:
                    raise ValueError(
                        f"Webhook path slug '{other_slug}' is already used by graph '{gid}'. "
                        "Choose a different webhook_path_slug."
                    )

    def _ensure_user_store(self) -> None:
        if self.path.exists():
            return
        self._save_user_all({"graphs": [], "deleted_graph_ids": []})

    def _load_bundled_all(self) -> dict[str, Any]:
        payload = json.loads(self.bundled_path.read_text())
        return {
            "graphs": [
                _sanitize_graph_payload(graph)
                for graph in payload.get("graphs", [])
                if isinstance(graph, dict)
            ]
        }

    def _load_user_all(self) -> dict[str, Any]:
        payload = json.loads(self.path.read_text())
        return {
            "graphs": [
                _sanitize_graph_payload(graph)
                for graph in payload.get("graphs", [])
                if isinstance(graph, dict)
            ],
            "deleted_graph_ids": [
                str(graph_id).strip()
                for graph_id in payload.get("deleted_graph_ids", [])
                if str(graph_id).strip()
            ],
        }

    def _save_user_all(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(
                {
                    "graphs": [_sanitize_graph_payload(graph) for graph in payload.get("graphs", [])],
                    "deleted_graph_ids": sorted(
                        {
                            str(graph_id).strip()
                            for graph_id in payload.get("deleted_graph_ids", [])
                            if str(graph_id).strip()
                        }
                    ),
                },
                indent=2,
            )
        )
        self._invalidate_caches()

    def _sanitize_user_store(self) -> None:
        try:
            payload = json.loads(self.path.read_text())
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return
        sanitized = {
            "graphs": [_sanitize_graph_payload(graph) for graph in payload.get("graphs", []) if isinstance(graph, dict)],
            "deleted_graph_ids": [
                str(graph_id).strip()
                for graph_id in payload.get("deleted_graph_ids", [])
                if str(graph_id).strip()
            ],
        }
        if sanitized != payload:
            self._save_user_all(sanitized)

    def _bundled_graph_ids(self) -> set[str]:
        return {graph["graph_id"] for graph in self._load_bundled_all()["graphs"]}

    def _merged_graphs(self) -> dict[str, dict[str, Any]]:
        if self._merged_graphs_cache is None:
            deleted_graph_ids = set(self._load_user_all()["deleted_graph_ids"])
            merged = {
                graph["graph_id"]: load_graph_document(graph).to_dict()
                for graph in self._load_bundled_all()["graphs"]
                if graph["graph_id"] not in deleted_graph_ids
            }
            for graph in self._load_user_all()["graphs"]:
                merged[graph["graph_id"]] = load_graph_document(graph).to_dict()
            self._merged_graphs_cache = merged
        return self._merged_graphs_cache

    def _invalidate_caches(self) -> None:
        self._merged_graphs_cache = None
        self._catalog_cache = None
