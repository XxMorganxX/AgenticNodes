from __future__ import annotations

from typing import Any, Mapping

from graph_agent.providers.base import ModelProvider, ModelRequest, ModelResponse, ModelToolCall, ProviderPreflightResult


def _request_text(request: ModelRequest, key: str) -> str:
    value = request.metadata.get(key, "")
    return str(value).strip()


def _tool_items(request: ModelRequest) -> list[dict[str, Any]]:
    tool_result = request.metadata.get("tool_result", {})
    if isinstance(tool_result, Mapping):
        items = tool_result.get("items", [])
        if isinstance(items, list):
            return [item for item in items if isinstance(item, Mapping)]
    return []


def _preferred_tool_name(request: ModelRequest) -> str:
    if isinstance(request.preferred_tool_name, str) and request.preferred_tool_name.strip():
        return request.preferred_tool_name.strip()
    if request.available_tools:
        return request.available_tools[0].name
    return "search_catalog"


def _catalog_query_arguments(request: ModelRequest, *, limit: Any) -> dict[str, Any]:
    return {
        "query": _request_text(request, "user_request"),
        "limit": limit,
    }


def _decision(
    *,
    message: Any = None,
    final_message: Any = None,
    need_tool: bool | None = None,
    tool_calls: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized_tool_calls = list(tool_calls or [])
    resolved_message = message if message is not None else final_message
    resolved_need_tool = bool(normalized_tool_calls) if need_tool is None else bool(need_tool)
    return {
        "message": "" if resolved_need_tool and resolved_message is None else resolved_message,
        "need_tool": resolved_need_tool,
        "tool_calls": normalized_tool_calls,
    }


class MockModelProvider(ModelProvider):
    name = "mock"

    def generate(self, request: ModelRequest) -> ModelResponse:
        mode = request.metadata.get("mode")

        if mode == "schema_proposal":
            tool_name = _preferred_tool_name(request)
            tool_call = ModelToolCall(
                tool_name=tool_name,
                arguments=_catalog_query_arguments(request, limit="three"),
            )
            return ModelResponse(
                content="Generated an initial tool payload.",
                structured_output=_decision(
                    tool_calls=[
                        {
                            "tool_name": tool_name,
                            "arguments": tool_call.arguments,
                        }
                    ]
                ),
                tool_calls=[tool_call],
                metadata={"mode": mode},
            )

        if mode == "schema_repair":
            tool_name = _preferred_tool_name(request)
            tool_call = ModelToolCall(
                tool_name=tool_name,
                arguments=_catalog_query_arguments(request, limit=3),
            )
            return ModelResponse(
                content="Generated a repaired tool payload.",
                structured_output=_decision(
                    tool_calls=[
                        {
                            "tool_name": tool_name,
                            "arguments": tool_call.arguments,
                        }
                    ]
                ),
                tool_calls=[tool_call],
                metadata={"mode": mode},
            )

        if mode == "final_response":
            items = _tool_items(request)
            item_count = len(items)
            return ModelResponse(
                content=f"I found {item_count} matching items and formatted the response.",
                structured_output=_decision(
                    final_message={
                        "message": f"Found {item_count} results for '{_request_text(request, 'user_request')}'.",
                        "items": items,
                    }
                ),
                metadata={"mode": mode},
            )

        if mode in {"research_query", "executor_plan"}:
            tool_name = _preferred_tool_name(request)
            tool_call = ModelToolCall(
                tool_name=tool_name,
                arguments=_catalog_query_arguments(request, limit=3),
            )
            return ModelResponse(
                content="Generated a valid tool payload for the catalog search.",
                structured_output=_decision(
                    tool_calls=[
                        {
                            "tool_name": tool_name,
                            "arguments": tool_call.arguments,
                        }
                    ]
                ),
                tool_calls=[tool_call],
                metadata={"mode": mode},
            )

        if mode == "research_summary":
            items = _tool_items(request)
            item_titles = [str(item.get("title", "")).strip() for item in items if str(item.get("title", "")).strip()]
            return ModelResponse(
                content="Summarized the research findings.",
                structured_output=_decision(
                    final_message={
                        "message": (
                            "Research found these useful references: "
                            + (", ".join(item_titles) if item_titles else "no matching references were returned.")
                        ),
                        "items": items,
                    }
                ),
                metadata={"mode": mode},
            )

        if mode == "planner_draft":
            request_text = _request_text(request, "user_request")
            return ModelResponse(
                content="Drafted a short execution plan.",
                structured_output=_decision(
                    final_message={
                        "message": f"Plan ready for: {request_text}" if request_text else "Plan ready.",
                        "steps": [
                            "Inspect the available graph and tool context.",
                            "Choose the next action with the highest confidence.",
                            "Return the proposed execution plan.",
                        ],
                    }
                ),
                metadata={"mode": mode},
            )

        if mode == "executor_validate":
            items = _tool_items(request)
            item_count = len(items)
            return ModelResponse(
                content="Validated the tool result.",
                structured_output=_decision(
                    final_message={
                        "message": f"Validated {item_count} execution result item{'s' if item_count != 1 else ''}.",
                        "items": items,
                        "status": "ready",
                    }
                ),
                metadata={"mode": mode},
            )

        return ModelResponse(
            content="Mock provider produced no structured output.",
            structured_output=_decision(final_message="Mock provider produced no structured output."),
            metadata={"mode": mode},
        )

    def preflight(self, provider_config: Mapping[str, Any] | None = None) -> ProviderPreflightResult:
        check_auth = bool(provider_config and provider_config.get("check_auth"))
        return ProviderPreflightResult(
            status="available",
            ok=True,
            message=(
                "Mock provider is always available in local development."
                if not check_auth
                else "Mock provider does not require live verification."
            ),
            details={
                "backend_type": "mock",
                "auth_mode": "not_applicable",
                "supports_live_verification": False,
            },
        )
