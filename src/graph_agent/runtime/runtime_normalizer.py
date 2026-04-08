from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class RuntimeFieldExtractorConfig:
    field_name: str
    fallback_field_names: tuple[str, ...] = ()
    preferred_path: str = ""
    case_sensitive: bool = False
    max_matches: int = 25


def parse_field_name_list(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        raw_candidates = value.replace("\n", ",").split(",")
        return tuple(candidate.strip() for candidate in raw_candidates if candidate.strip())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(candidate).strip() for candidate in value if str(candidate).strip())
    return ()


def field_match(candidate: str, target: str, *, case_sensitive: bool) -> bool:
    if case_sensitive:
        return candidate == target
    return candidate.strip().lower() == target.strip().lower()


def extract_field_candidates(
    value: Any,
    *,
    field_names: Sequence[str],
    case_sensitive: bool = False,
    max_matches: int = 25,
) -> list[dict[str, Any]]:
    normalized_field_names = [field for field in field_names if str(field).strip()]
    matches: list[dict[str, Any]] = []

    def walk(current: Any, path: str) -> None:
        if len(matches) >= max_matches:
            return
        if isinstance(current, Mapping):
            for key, candidate in current.items():
                next_path = f"{path}.{key}" if path else str(key)
                if any(field_match(str(key), target, case_sensitive=case_sensitive) for target in normalized_field_names):
                    matches.append(
                        {
                            "field": str(key),
                            "path": next_path,
                            "value": candidate,
                        }
                    )
                    if len(matches) >= max_matches:
                        return
                walk(candidate, next_path)
                if len(matches) >= max_matches:
                    return
        elif isinstance(current, Sequence) and not isinstance(current, (str, bytes, bytearray)):
            for index, candidate in enumerate(current):
                next_path = f"{path}.{index}" if path else str(index)
                walk(candidate, next_path)
                if len(matches) >= max_matches:
                    return

    walk(value, "")
    return matches
