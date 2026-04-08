from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from graph_agent.schema_validation import validation_error_payload


@dataclass
class ToolContext:
    run_id: str
    graph_id: str
    node_id: str
    state_snapshot: Mapping[str, Any]


@dataclass
class ToolResult:
    status: str
    output: Any = None
    error: dict[str, Any] | None = None
    summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolDefinition:
    name: str
    description: str
    input_schema: Mapping[str, Any]
    executor: Callable[[Mapping[str, Any], ToolContext], ToolResult] | None = None
    source_type: str = "builtin"
    server_id: str | None = None
    enabled: bool = True
    available: bool = True
    availability_error: str = ""
    schema_origin: str = "static"
    schema_warning: str = ""
    managed: bool = False
    canonical_name: str = ""
    display_name: str = ""
    aliases: list[str] = field(default_factory=list)
    capability_type: str = "tool"

    def __post_init__(self) -> None:
        canonical_name = str(self.canonical_name or self.name).strip()
        display_name = str(self.display_name or self.name).strip()
        raw_aliases: Sequence[str] | str | None = self.aliases
        aliases: list[str] = []
        if isinstance(raw_aliases, Sequence) and not isinstance(raw_aliases, (str, bytes)):
            aliases = [str(alias).strip() for alias in raw_aliases if str(alias).strip()]
        normalized_aliases: list[str] = []
        seen_aliases: set[str] = {canonical_name}
        for alias in aliases:
            if alias in seen_aliases:
                continue
            seen_aliases.add(alias)
            normalized_aliases.append(alias)
        self.name = canonical_name
        self.canonical_name = canonical_name
        self.display_name = display_name or canonical_name
        self.aliases = normalized_aliases

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "canonical_name": self.canonical_name,
            "display_name": self.display_name,
            "aliases": list(self.aliases),
            "description": self.description,
            "input_schema": dict(self.input_schema),
            "source_type": self.source_type,
            "capability_type": self.capability_type,
            "server_id": self.server_id,
            "enabled": self.enabled,
            "available": self.available,
            "availability_error": self.availability_error,
            "schema_origin": self.schema_origin,
            "schema_warning": self.schema_warning,
            "managed": self.managed,
        }


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolDefinition] = {}
        self._aliases: dict[str, set[str]] = {}

    def register(self, tool: ToolDefinition) -> None:
        canonical_name = tool.canonical_name
        if canonical_name in self._tools:
            raise ValueError(f"Tool '{canonical_name}' is already registered.")
        self._tools[canonical_name] = tool
        self._register_aliases(tool)

    def upsert(self, tool: ToolDefinition) -> None:
        existing = self._tools.get(tool.canonical_name)
        if existing is not None:
            self._unregister_aliases(existing)
        self._tools[tool.canonical_name] = tool
        self._register_aliases(tool)

    def remove(self, name: str) -> None:
        tool = self.get_optional(name)
        if tool is None:
            return
        self._tools.pop(tool.canonical_name, None)
        self._unregister_aliases(tool)

    def get(self, name: str) -> ToolDefinition:
        tool = self.get_optional(name)
        if tool is None:
            raise KeyError(f"Unknown tool '{name}'.")
        return tool

    def get_optional(self, name: str) -> ToolDefinition | None:
        lookup_name = str(name).strip()
        if not lookup_name:
            return None
        direct = self._tools.get(lookup_name)
        if direct is not None:
            return direct
        matching_canonical_names = sorted(self._aliases.get(lookup_name, set()))
        if not matching_canonical_names:
            return None
        if len(matching_canonical_names) > 1:
            raise ValueError(
                f"Tool reference '{lookup_name}' is ambiguous. Matches: {', '.join(matching_canonical_names)}."
            )
        return self._tools.get(matching_canonical_names[0])

    def list_definitions(self) -> list[ToolDefinition]:
        return list(self._tools.values())

    def list_server_tool_names(self, server_id: str) -> list[str]:
        return sorted(tool.name for tool in self._tools.values() if tool.server_id == server_id)

    def remove_server_tools(self, server_id: str) -> None:
        for tool_name in self.list_server_tool_names(server_id):
            self.remove(tool_name)

    def exposable_definitions(self, names: list[str]) -> list[ToolDefinition]:
        definitions: list[ToolDefinition] = []
        seen: set[str] = set()
        for name in names:
            tool = self.get_optional(str(name))
            if tool is None or not tool.enabled or not tool.available:
                continue
            if tool.canonical_name in seen:
                continue
            seen.add(tool.canonical_name)
            definitions.append(tool)
        return definitions

    def require_exposable(self, name: str) -> ToolDefinition:
        tool = self.get(name)
        if not tool.enabled:
            raise ValueError(f"Tool '{name}' is disabled.")
        if not tool.available:
            detail = f" {tool.availability_error}" if tool.availability_error else ""
            raise ValueError(f"Tool '{name}' is unavailable.{detail}")
        return tool

    def require_invocable(self, name: str) -> ToolDefinition:
        tool = self.require_exposable(name)
        if tool.executor is None:
            raise ValueError(f"Tool '{name}' is not executable.")
        return tool

    def require_graph_reference(self, name: str, *, require_executor: bool = False) -> ToolDefinition:
        tool = self.get(name)
        if tool.source_type == "mcp":
            return tool
        if require_executor:
            return self.require_invocable(name)
        return self.require_exposable(name)

    def set_tool_enabled(self, name: str, enabled: bool) -> ToolDefinition:
        tool = self.get(name)
        updated = ToolDefinition(
            name=tool.name,
            canonical_name=tool.canonical_name,
            display_name=tool.display_name,
            aliases=list(tool.aliases),
            description=tool.description,
            input_schema=tool.input_schema,
            executor=tool.executor,
            source_type=tool.source_type,
            capability_type=tool.capability_type,
            server_id=tool.server_id,
            enabled=enabled,
            available=tool.available,
            availability_error=tool.availability_error,
            schema_origin=tool.schema_origin,
            schema_warning=tool.schema_warning,
            managed=tool.managed,
        )
        self._tools[tool.canonical_name] = updated
        self._register_aliases(updated)
        return updated

    def mark_tool_unavailable(self, name: str, reason: str) -> None:
        tool = self.get_optional(name)
        if tool is None:
            return
        updated = ToolDefinition(
            name=tool.name,
            canonical_name=tool.canonical_name,
            display_name=tool.display_name,
            aliases=list(tool.aliases),
            description=tool.description,
            input_schema=tool.input_schema,
            executor=tool.executor,
            source_type=tool.source_type,
            capability_type=tool.capability_type,
            server_id=tool.server_id,
            enabled=tool.enabled,
            available=False,
            availability_error=reason,
            schema_origin=tool.schema_origin,
            schema_warning=tool.schema_warning,
            managed=tool.managed,
        )
        self._tools[tool.canonical_name] = updated
        self._register_aliases(updated)

    def mark_server_tools_unavailable(self, server_id: str, reason: str) -> None:
        for tool_name in self.list_server_tool_names(server_id):
            self.mark_tool_unavailable(tool_name, reason)

    def validate_input(self, schema: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any] | None:
        return validation_error_payload(payload, schema)

    def invoke(self, name: str, payload: Mapping[str, Any], context: ToolContext) -> ToolResult:
        tool = self.get(name)
        if not tool.enabled:
            return ToolResult(
                status="unavailable",
                error={"message": f"Tool '{name}' is disabled."},
                summary=f"Tool '{name}' is disabled.",
            )
        if not tool.available or tool.executor is None:
            detail = tool.availability_error or "Tool is unavailable."
            return ToolResult(
                status="unavailable",
                error={"message": detail},
                summary=f"Tool '{name}' is unavailable.",
            )
        validation_error = self.validate_input(tool.input_schema, payload)
        if validation_error is not None:
            return ToolResult(
                status="validation_error",
                error=validation_error,
                summary=f"Tool '{name}' rejected the payload.",
            )
        return tool.executor(payload, context)

    def canonical_name_for(self, name: str) -> str:
        return self.get(name).canonical_name

    def _register_aliases(self, tool: ToolDefinition) -> None:
        alias_values = {tool.display_name, *tool.aliases}
        for alias in alias_values:
            normalized_alias = str(alias).strip()
            if not normalized_alias or normalized_alias == tool.canonical_name:
                continue
            self._aliases.setdefault(normalized_alias, set()).add(tool.canonical_name)

    def _unregister_aliases(self, tool: ToolDefinition) -> None:
        alias_values = {tool.display_name, *tool.aliases}
        for alias in alias_values:
            normalized_alias = str(alias).strip()
            if not normalized_alias or normalized_alias == tool.canonical_name:
                continue
            matching = self._aliases.get(normalized_alias)
            if not matching:
                continue
            matching.discard(tool.canonical_name)
            if not matching:
                self._aliases.pop(normalized_alias, None)
