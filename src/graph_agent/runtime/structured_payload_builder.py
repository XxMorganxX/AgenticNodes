from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Mapping, Sequence

from graph_agent.runtime.runtime_normalizer import extract_field_candidates


@dataclass(frozen=True)
class StructuredPayloadBuilderConfig:
    template_json: str
    case_sensitive: bool = False
    max_matches_per_field: int = 25


@dataclass(frozen=True)
class StructuredPayloadBuildResult:
    payload: dict[str, Any]
    filled_paths: tuple[str, ...]
    preserved_paths: tuple[str, ...]
    unresolved_paths: tuple[str, ...]
    field_matches: tuple[dict[str, Any], ...]


def parse_structured_payload_template(value: Any) -> dict[str, Any]:
    candidate = str(value or "").strip() or "{}"
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise ValueError("Structured payload template_json must be valid JSON.") from exc
    if not isinstance(parsed, Mapping):
        raise ValueError("Structured payload template_json must be a JSON object.")
    return dict(parsed)


def _find_first_match(
    field_name: str,
    source_roots: Sequence[Any],
    *,
    case_sensitive: bool,
    max_matches: int,
) -> dict[str, Any] | None:
    for root in source_roots:
        matches = extract_field_candidates(
            root,
            field_names=[field_name],
            case_sensitive=case_sensitive,
            max_matches=max_matches,
        )
        if matches:
            return dict(matches[0])
    return None


def _is_missing_template_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, Mapping):
        return len(value) == 0
    if isinstance(value, list):
        return len(value) == 0
    return False


def build_structured_payload(
    template: Mapping[str, Any],
    source_roots: Sequence[Any],
    *,
    case_sensitive: bool = False,
    max_matches_per_field: int = 25,
) -> StructuredPayloadBuildResult:
    filled_paths: list[str] = []
    preserved_paths: list[str] = []
    unresolved_paths: list[str] = []
    field_matches: list[dict[str, Any]] = []

    def build_value(
        template_value: Any,
        *,
        field_name: str | None,
        path: str,
        active_source_roots: Sequence[Any],
    ) -> Any:
        contextual_roots = list(active_source_roots)
        if field_name:
            container_match = _find_first_match(
                field_name,
                active_source_roots,
                case_sensitive=case_sensitive,
                max_matches=max_matches_per_field,
            )
            if container_match is not None and isinstance(container_match.get("value"), Mapping):
                contextual_roots = [container_match["value"], *contextual_roots]

        if isinstance(template_value, Mapping):
            if not template_value and field_name:
                match = _find_first_match(
                    field_name,
                    active_source_roots,
                    case_sensitive=case_sensitive,
                    max_matches=max_matches_per_field,
                )
                if match is not None and isinstance(match.get("value"), Mapping):
                    filled_paths.append(path)
                    field_matches.append({"target_path": path, **match})
                    return match["value"]
                unresolved_paths.append(path)
                return {}
            built: dict[str, Any] = {}
            for child_key, child_value in template_value.items():
                child_path = f"{path}.{child_key}" if path else str(child_key)
                built[str(child_key)] = build_value(
                    child_value,
                    field_name=str(child_key),
                    path=child_path,
                    active_source_roots=contextual_roots,
                )
            if path:
                preserved_paths.append(path)
            return built

        if isinstance(template_value, list):
            if template_value:
                preserved_paths.append(path)
                return list(template_value)
            if field_name:
                match = _find_first_match(
                    field_name,
                    active_source_roots,
                    case_sensitive=case_sensitive,
                    max_matches=max_matches_per_field,
                )
                if match is not None and isinstance(match.get("value"), list):
                    filled_paths.append(path)
                    field_matches.append({"target_path": path, **match})
                    return match["value"]
            unresolved_paths.append(path)
            return []

        if not _is_missing_template_value(template_value):
            preserved_paths.append(path)
            return template_value

        if not field_name:
            unresolved_paths.append(path)
            return template_value

        match = _find_first_match(
            field_name,
            active_source_roots,
            case_sensitive=case_sensitive,
            max_matches=max_matches_per_field,
        )
        if match is None:
            unresolved_paths.append(path)
            return template_value
        filled_paths.append(path)
        field_matches.append({"target_path": path, **match})
        return match["value"]

    payload = {
        str(key): build_value(value, field_name=str(key), path=str(key), active_source_roots=source_roots)
        for key, value in template.items()
    }
    return StructuredPayloadBuildResult(
        payload=payload,
        filled_paths=tuple(filled_paths),
        preserved_paths=tuple(preserved_paths),
        unresolved_paths=tuple(unresolved_paths),
        field_matches=tuple(field_matches),
    )
