from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from graph_agent.schema_validation import normalize_object_json_schema, validation_error_payload


@dataclass
class ModelMessage:
    role: str
    content: str


@dataclass
class ModelToolDefinition:
    name: str
    description: str
    input_schema: Mapping[str, Any]


@dataclass
class ModelToolCall:
    tool_name: str
    arguments: Any
    provider_tool_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProviderPreflightResult:
    status: str
    ok: bool
    message: str = ""
    warnings: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelRequest:
    prompt_name: str
    messages: list[ModelMessage]
    response_schema: Mapping[str, Any] | None = None
    provider_config: Mapping[str, Any] | None = None
    available_tools: list[ModelToolDefinition] = field(default_factory=list)
    preferred_tool_name: str | None = None
    response_mode: str = "message"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelResponse:
    content: str
    structured_output: Any = None
    tool_calls: list[ModelToolCall] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class StructuredOutputValidationError(ValueError):
    def __init__(self, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = dict(details) if isinstance(details, Mapping) else {}


def _as_mapping_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        return dict(value)
    return None


def _normalize_tool_call_entry(value: Any) -> dict[str, Any] | None:
    if isinstance(value, ModelToolCall):
        return {
            "tool_name": value.tool_name,
            "arguments": value.arguments,
            "provider_tool_id": value.provider_tool_id,
            "metadata": dict(value.metadata),
        }
    mapping_value = _as_mapping_dict(value)
    if mapping_value is None:
        return None
    tool_name = mapping_value.get("tool_name")
    if not isinstance(tool_name, str) or not tool_name.strip():
        return None
    metadata = mapping_value.get("metadata")
    normalized_metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
    provider_tool_id = mapping_value.get("provider_tool_id")
    return {
        "tool_name": tool_name.strip(),
        "arguments": mapping_value.get("arguments"),
        "provider_tool_id": str(provider_tool_id) if provider_tool_id else None,
        "metadata": normalized_metadata,
    }


def _tool_lookup(
    available_tools: Sequence[ModelToolDefinition] | Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, ModelToolDefinition]:
    if isinstance(available_tools, Mapping):
        normalized_lookup: dict[str, ModelToolDefinition] = {}
        for tool_name, input_schema in available_tools.items():
            normalized_name = str(tool_name).strip()
            if not normalized_name or not isinstance(input_schema, Mapping):
                continue
            normalized_lookup[normalized_name] = ModelToolDefinition(
                name=normalized_name,
                description="",
                input_schema=dict(input_schema),
            )
        return normalized_lookup
    lookup: dict[str, ModelToolDefinition] = {}
    for tool in available_tools or []:
        normalized_name = str(tool.name).strip()
        if not normalized_name:
            continue
        lookup[normalized_name] = tool
    return lookup


def _tool_call_item_schema(tool_name: str, input_schema: Mapping[str, Any]) -> dict[str, Any]:
    normalized_arguments_schema = normalize_object_json_schema(input_schema).schema
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "tool_name": {"type": "string", "const": tool_name},
            "arguments": normalized_arguments_schema,
            "provider_tool_id": {"type": ["string", "null"]},
            "metadata": {"type": "object", "additionalProperties": True},
        },
        "required": ["tool_name", "arguments"],
    }


def _decision_validation_details(
    *,
    path: str,
    validator: str,
    expected: Any,
    received: Any,
    detail: str,
) -> dict[str, Any]:
    return {
        "validation_errors": [
            {
                "path": path,
                "schema_path": "#",
                "validator": validator,
                "expected": expected,
                "received": received,
                "detail": detail,
            }
        ]
    }


def api_decision_response_schema(
    *,
    final_message_schema: Mapping[str, Any] | None = None,
    available_tools: Sequence[ModelToolDefinition] | None = None,
    allow_tool_calls: bool = True,
    response_mode: str = "auto",
) -> dict[str, Any]:
    tool_lookup = _tool_lookup(available_tools)
    generic_tool_call_item_schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "tool_name": {"type": "string"},
            "arguments": normalize_object_json_schema({}).schema,
            "provider_tool_id": {"type": ["string", "null"]},
            "metadata": {"type": "object", "additionalProperties": True},
        },
        "required": ["tool_name", "arguments"],
    }
    tool_item_schemas = [
        _tool_call_item_schema(tool_name, tool.input_schema) for tool_name, tool in tool_lookup.items()
    ]
    tool_call_item_schema: dict[str, Any]
    if len(tool_item_schemas) == 1:
        tool_call_item_schema = tool_item_schemas[0]
    elif tool_item_schemas:
        tool_call_item_schema = {"oneOf": tool_item_schemas}
    else:
        tool_call_item_schema = generic_tool_call_item_schema

    final_payload_schema: dict[str, Any]
    if isinstance(final_message_schema, Mapping):
        final_payload_schema = dict(final_message_schema)
    else:
        final_payload_schema = {
            "type": ["string", "object", "array", "number", "boolean", "null"],
        }

    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "message": final_payload_schema,
            "need_tool": {"type": "boolean"},
            "tool_calls": {
                "type": "array",
                "items": tool_call_item_schema,
            },
        },
        "required": ["message", "need_tool", "tool_calls"],
    }
    if not allow_tool_calls:
        schema["properties"]["need_tool"] = {"type": "boolean", "const": False}
        schema["properties"]["tool_calls"] = {
            "type": "array",
            "items": tool_call_item_schema,
            "maxItems": 0,
        }
    elif response_mode == "tool_call":
        schema["properties"]["need_tool"] = {"type": "boolean", "const": True}
        schema["properties"]["tool_calls"] = {
            "type": "array",
            "items": tool_call_item_schema,
            "minItems": 1,
        }
    elif response_mode == "message":
        schema["properties"]["need_tool"] = {"type": "boolean", "const": False}
        schema["properties"]["tool_calls"] = {
            "type": "array",
            "items": tool_call_item_schema,
            "maxItems": 0,
        }
    return schema


def normalize_api_decision_output(
    structured_output: Any,
    *,
    content: str = "",
    tool_calls: Sequence[ModelToolCall | Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized_tool_calls = [
        normalized
        for normalized in (_normalize_tool_call_entry(tool_call) for tool_call in (tool_calls or []))
        if normalized is not None
    ]
    mapping_output = _as_mapping_dict(structured_output)
    if mapping_output is not None:
        need_tool_value = mapping_output.get("need_tool")
        if not isinstance(need_tool_value, bool):
            legacy_should_call_tools = mapping_output.get("should_call_tools")
            need_tool_value = legacy_should_call_tools if isinstance(legacy_should_call_tools, bool) else None
        if isinstance(need_tool_value, bool):
            raw_message = mapping_output.get("message", mapping_output.get("final_message"))
            if (raw_message is None or raw_message == "") and content.strip():
                raw_message = content
            raw_tool_calls = mapping_output.get("tool_calls", [])
            normalized_structured_tool_calls = (
                [
                    normalized
                    for normalized in (_normalize_tool_call_entry(tool_call) for tool_call in raw_tool_calls)
                    if normalized is not None
                ]
                if isinstance(raw_tool_calls, list)
                else []
            )
            return {
                "message": raw_message,
                "need_tool": bool(need_tool_value),
                "tool_calls": normalized_structured_tool_calls,
            }
    if normalized_tool_calls:
        fallback_message = content if content.strip() else ""
        return {
            "message": fallback_message,
            "need_tool": True,
            "tool_calls": normalized_tool_calls,
        }
    final_message = structured_output if structured_output is not None else (content if content.strip() else None)
    return {
        "message": final_message,
        "need_tool": False,
        "tool_calls": [],
    }


def validate_api_decision_output(
    decision: Mapping[str, Any],
    *,
    decision_schema: Mapping[str, Any] | None = None,
    available_tools: Sequence[ModelToolDefinition] | Mapping[str, Mapping[str, Any]] | None = None,
    callable_tool_names: set[str] | None = None,
    response_mode: str = "auto",
) -> dict[str, Any]:
    tool_lookup = _tool_lookup(available_tools)
    decision_schema_error = validation_error_payload(
        decision,
        decision_schema
        if isinstance(decision_schema, Mapping)
        else api_decision_response_schema(
            available_tools=list(tool_lookup.values()) if tool_lookup else None,
            allow_tool_calls=response_mode != "message",
            response_mode=response_mode,
        ),
        default_message="Structured API output does not match the decision schema.",
    )
    need_tool_value = decision.get("need_tool")
    if not isinstance(need_tool_value, bool):
        legacy_should_call_tools = decision.get("should_call_tools")
        need_tool_value = legacy_should_call_tools if isinstance(legacy_should_call_tools, bool) else None
    if not isinstance(need_tool_value, bool):
        raise StructuredOutputValidationError(
            "Structured API output must include boolean 'need_tool'.",
            details=_decision_validation_details(
                path="$.need_tool",
                validator="type",
                expected="boolean",
                received=type(decision.get("need_tool")).__name__,
                detail="Structured API output must include boolean 'need_tool'.",
            ),
        )
    should_call_tools = bool(need_tool_value)
    raw_tool_calls = decision.get("tool_calls", [])
    if not isinstance(raw_tool_calls, list):
        raise StructuredOutputValidationError(
            "Structured API output field 'tool_calls' must be a list.",
            details=_decision_validation_details(
                path="$.tool_calls",
                validator="type",
                expected="array",
                received=type(raw_tool_calls).__name__,
                detail="Structured API output field 'tool_calls' must be a list.",
            ),
        )
    message = decision.get("message", decision.get("final_message"))
    if message is None:
        raise StructuredOutputValidationError(
            "Structured API output must include 'message'.",
            details=_decision_validation_details(
                path="$.message",
                validator="required",
                expected="message",
                received=None,
                detail="Structured API output must include 'message'.",
            ),
        )
    normalized_tool_calls = [
        normalized
        for normalized in (_normalize_tool_call_entry(tool_call) for tool_call in raw_tool_calls)
        if normalized is not None
    ]
    if raw_tool_calls and len(normalized_tool_calls) != len(raw_tool_calls):
        raise StructuredOutputValidationError(
            "Structured API output does not match the decision schema.",
            details=decision_schema_error
            or _decision_validation_details(
                path="$.tool_calls",
                validator="schema",
                expected="valid tool call entries",
                received="invalid entries",
                detail="Structured API output includes malformed tool call entries.",
            ),
        )
    if should_call_tools:
        if not normalized_tool_calls:
            raise StructuredOutputValidationError(
                "Structured API output requires at least one tool call when 'need_tool' is true.",
                details=_decision_validation_details(
                    path="$.tool_calls",
                    validator="minItems",
                    expected="at least 1 tool call",
                    received=0,
                    detail="Structured API output requires at least one tool call when 'need_tool' is true.",
                ),
            )
        if response_mode == "message":
            raise StructuredOutputValidationError(
                "Structured API output requires 'need_tool' to be false in message mode.",
                details=_decision_validation_details(
                    path="$.need_tool",
                    validator="const",
                    expected=False,
                    received=True,
                    detail="Structured API output requires 'need_tool' to be false in message mode.",
                ),
            )
        if callable_tool_names:
            unknown_tool_names = sorted(
                {
                    str(tool_call["tool_name"])
                    for tool_call in normalized_tool_calls
                    if str(tool_call["tool_name"]) not in callable_tool_names
                }
            )
            if unknown_tool_names:
                joined = ", ".join(unknown_tool_names)
                raise StructuredOutputValidationError(
                    f"Structured API output requested unavailable tool(s): {joined}.",
                    details=_decision_validation_details(
                        path="$.tool_calls",
                        validator="enum",
                        expected=sorted(callable_tool_names),
                        received=unknown_tool_names,
                        detail=f"Structured API output requested unavailable tool(s): {joined}.",
                    ),
                )
        for tool_call in normalized_tool_calls:
            tool_name = str(tool_call.get("tool_name", "")).strip()
            tool = tool_lookup.get(tool_name)
            if tool is None:
                continue
            tool_call_error = validation_error_payload(
                tool_call,
                _tool_call_item_schema(tool_name, tool.input_schema),
                default_message=f"Structured API output tool call for '{tool_name}' does not match the tool schema.",
            )
            if tool_call_error is not None:
                raise StructuredOutputValidationError(
                    f"Structured API output tool call for '{tool_name}' does not match the tool schema.",
                    details=tool_call_error,
                )
        if decision_schema_error is not None:
            raise StructuredOutputValidationError(
                "Structured API output does not match the decision schema.",
                details=decision_schema_error,
            )
        return {
            "message": message,
            "need_tool": True,
            "should_call_tools": True,
            "tool_calls": normalized_tool_calls,
            "final_message": message,
        }
    if normalized_tool_calls:
        raise StructuredOutputValidationError(
            "Structured API output must leave 'tool_calls' empty when 'need_tool' is false.",
            details=_decision_validation_details(
                path="$.tool_calls",
                validator="maxItems",
                expected=0,
                received=len(normalized_tool_calls),
                detail="Structured API output must leave 'tool_calls' empty when 'need_tool' is false.",
            ),
        )
    if response_mode == "tool_call":
        raise StructuredOutputValidationError(
            "Structured API output requires 'need_tool' to be true in tool_call mode.",
            details=_decision_validation_details(
                path="$.need_tool",
                validator="const",
                expected=True,
                received=False,
                detail="Structured API output requires 'need_tool' to be true in tool_call mode.",
            ),
        )
    if decision_schema_error is not None:
        raise StructuredOutputValidationError(
            "Structured API output does not match the decision schema.",
            details=decision_schema_error,
        )
    return {
        "message": message,
        "need_tool": False,
        "should_call_tools": False,
        "tool_calls": [],
        "final_message": message,
    }


class ModelProvider(Protocol):
    name: str

    def generate(self, request: ModelRequest) -> ModelResponse:
        ...

    def preflight(self, provider_config: Mapping[str, Any] | None = None) -> ProviderPreflightResult:
        ...
