from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Mapping, Sequence

SEARCH_SECTION_PAYLOAD = "payload"
SEARCH_SECTION_METADATA = "metadata"
SEARCH_SECTION_ARTIFACTS = "artifacts"
SEARCH_SECTIONS: tuple[str, ...] = (
    SEARCH_SECTION_PAYLOAD,
    SEARCH_SECTION_METADATA,
    SEARCH_SECTION_ARTIFACTS,
)


def normalize_search_section(value: Any, *, default: str = SEARCH_SECTION_PAYLOAD) -> str:
    text = str(value or "").strip().lower()
    if text in SEARCH_SECTIONS:
        return text
    return default


@dataclass(frozen=True)
class StructuredPayloadBuilderConfig:
    template_json: str
    case_sensitive: bool = False
    max_matches_per_field: int = 25
    field_aliases: tuple[tuple[str, tuple[str, ...]], ...] = ()
    # Default section for entries that don't specify one. Single section per match.
    default_search_section: str = SEARCH_SECTION_PAYLOAD
    # Per-entry overrides: ((field_name, "payload" | "metadata" | "artifacts"), ...)
    field_search_scopes: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class StructuredPayloadBuildResult:
    payload: dict[str, Any]
    filled_paths: tuple[str, ...]
    preserved_paths: tuple[str, ...]
    unresolved_paths: tuple[str, ...]
    field_matches: tuple[dict[str, Any], ...]


_WORD_BOUNDARY_PATTERN = re.compile(r"([a-z0-9])([A-Z])")
_ACRONYM_BOUNDARY_PATTERN = re.compile(r"([A-Z]+)([A-Z][a-z])")
_NON_ALNUM_PATTERN = re.compile(r"[^A-Za-z0-9]+")
_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "name": ("full name", "person name", "contact name"),
    "first name": ("given name", "forename"),
    "last name": ("family name", "surname"),
    "email": ("email address", "mail", "work email", "business email"),
    "linkedin url": ("linkedin", "linkedin profile", "linkedin profile url"),
    "twitter url": ("twitter", "twitter profile", "twitter profile url", "x", "x profile", "x url"),
    "organization name": ("organization", "organization title", "company", "company name", "employer", "account name"),
    "domain": ("company domain", "website domain", "email domain", "company website"),
    "headline": ("title", "job title", "role"),
}


@dataclass(frozen=True)
class _FieldCandidate:
    field: str
    path: str
    value: Any
    field_phrase: str
    field_tokens: tuple[str, ...]
    path_tokens: tuple[str, ...]
    depth: int
    discovery_index: int


def parse_structured_payload_template(value: Any) -> dict[str, Any]:
    candidate = str(value or "").strip() or "{}"
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise ValueError("Structured payload template_json must be valid JSON.") from exc
    if not isinstance(parsed, Mapping):
        raise ValueError("Structured payload template_json must be a JSON object.")
    return dict(parsed)


def _tokenize_keywords(value: Any, *, case_sensitive: bool) -> tuple[str, ...]:
    text = str(value or "").strip()
    if not text:
        return ()
    text = _WORD_BOUNDARY_PATTERN.sub(r"\1 \2", text)
    text = _ACRONYM_BOUNDARY_PATTERN.sub(r"\1 \2", text)
    raw_parts = _NON_ALNUM_PATTERN.split(text)
    tokens = []
    for part in raw_parts:
        normalized_part = str(part or "").strip()
        if not normalized_part:
            continue
        tokens.append(normalized_part if case_sensitive else normalized_part.lower())
    return tuple(tokens)


def _token_phrase(tokens: Sequence[str]) -> str:
    return " ".join(str(token).strip() for token in tokens if str(token).strip())


def _target_keyword_variants(
    field_name: str,
    *,
    case_sensitive: bool,
    extra_aliases: Sequence[str] = (),
) -> tuple[tuple[str, ...], ...]:
    variants: list[tuple[str, ...]] = []
    seen: set[tuple[str, ...]] = set()

    def add_variant(tokens: tuple[str, ...]) -> None:
        if not tokens or tokens in seen:
            return
        variants.append(tokens)
        seen.add(tokens)

    if extra_aliases:
        # User-provided search keys fully replace the output field label —
        # the matcher searches for those keys only, not the entry's output name
        # or any built-in aliases.
        for alias in extra_aliases:
            add_variant(_tokenize_keywords(alias, case_sensitive=case_sensitive))
        return tuple(variants)

    direct_tokens = _tokenize_keywords(field_name, case_sensitive=case_sensitive)
    direct_phrase = _token_phrase(direct_tokens)
    add_variant(direct_tokens)
    for alias in _FIELD_ALIASES.get(direct_phrase, ()):
        add_variant(_tokenize_keywords(alias, case_sensitive=case_sensitive))
    return tuple(variants)


def _walk_field_candidates(
    value: Any,
    *,
    case_sensitive: bool,
    max_matches: int,
) -> list[_FieldCandidate]:
    matches: list[_FieldCandidate] = []
    discovery_index = 0

    def walk(current: Any, path: str) -> None:
        nonlocal discovery_index
        if len(matches) >= max_matches:
            return
        if isinstance(current, Mapping):
            for key, candidate_value in current.items():
                next_path = f"{path}.{key}" if path else str(key)
                matches.append(
                    _FieldCandidate(
                        field=str(key),
                        path=next_path,
                        value=candidate_value,
                        field_phrase=_token_phrase(_tokenize_keywords(key, case_sensitive=case_sensitive)),
                        field_tokens=_tokenize_keywords(key, case_sensitive=case_sensitive),
                        path_tokens=_tokenize_keywords(next_path, case_sensitive=case_sensitive),
                        depth=next_path.count("."),
                        discovery_index=discovery_index,
                    )
                )
                discovery_index += 1
                if len(matches) >= max_matches:
                    return
                walk(candidate_value, next_path)
                if len(matches) >= max_matches:
                    return
        elif isinstance(current, Sequence) and not isinstance(current, (str, bytes, bytearray)):
            for index, item in enumerate(current):
                next_path = f"{path}.{index}" if path else str(index)
                walk(item, next_path)
                if len(matches) >= max_matches:
                    return

    walk(value, "")
    return matches


def _value_matches_expected_shape(value: Any, expected_shape: str | None) -> bool:
    if expected_shape == "mapping":
        return isinstance(value, Mapping)
    if expected_shape == "list":
        return isinstance(value, list)
    if expected_shape == "scalar":
        return not isinstance(value, (Mapping, list))
    return True


def _match_details_for_candidate(
    candidate: _FieldCandidate,
    *,
    field_name: str,
    case_sensitive: bool,
    extra_aliases: Sequence[str] = (),
) -> dict[str, Any] | None:
    target_variants = _target_keyword_variants(
        field_name, case_sensitive=case_sensitive, extra_aliases=extra_aliases
    )
    if not target_variants:
        return None

    candidate_field_value = str(candidate.field or "").strip()
    target_raw_values: list[str] = []
    if extra_aliases:
        for alias in extra_aliases:
            alias_text = str(alias or "").strip()
            if alias_text and alias_text not in target_raw_values:
                target_raw_values.append(alias_text)
    else:
        field_text = str(field_name or "").strip()
        if field_text:
            target_raw_values.append(field_text)

    for raw_target in target_raw_values:
        if case_sensitive:
            is_raw_exact = candidate_field_value == raw_target
        else:
            is_raw_exact = candidate_field_value.lower() == raw_target.lower()
        if is_raw_exact:
            return {
                "match_type": "exact_key",
                "matched_keywords": list(_tokenize_keywords(raw_target, case_sensitive=False)),
                "score": (0, 0, 0, candidate.depth, candidate.discovery_index),
            }

    primary_phrase = _token_phrase(target_variants[0])
    if candidate.field_phrase == primary_phrase:
        return {
            "match_type": "normalized_key",
            "matched_keywords": list(target_variants[0]),
            "score": (1, 0, 0, candidate.depth, candidate.discovery_index),
        }

    candidate_field_token_set = set(candidate.field_tokens)
    candidate_path_token_set = set(candidate.path_tokens)
    seen_alias_keywords: set[tuple[str, ...]] = set()
    for alias_index, alias_tokens in enumerate(target_variants[1:], start=1):
        alias_phrase = _token_phrase(alias_tokens)
        if candidate.field_phrase == alias_phrase:
            return {
                "match_type": "alias_key",
                "matched_keywords": list(alias_tokens),
                "score": (2, alias_index, 0, candidate.depth, candidate.discovery_index),
            }
        alias_token_set = set(alias_tokens)
        if alias_token_set and alias_tokens not in seen_alias_keywords and alias_token_set.issubset(candidate_field_token_set):
            seen_alias_keywords.add(alias_tokens)
            return {
                "match_type": "alias_keywords",
                "matched_keywords": list(alias_tokens),
                "score": (3, alias_index, len(candidate_field_token_set) - len(alias_token_set), candidate.depth, candidate.discovery_index),
            }
        if alias_token_set and alias_tokens not in seen_alias_keywords and alias_token_set.issubset(candidate_path_token_set):
            seen_alias_keywords.add(alias_tokens)
            return {
                "match_type": "alias_path_keywords",
                "matched_keywords": list(alias_tokens),
                "score": (4, alias_index, len(candidate_path_token_set) - len(alias_token_set), candidate.depth, candidate.discovery_index),
            }

    direct_token_set = set(target_variants[0])
    if direct_token_set and direct_token_set.issubset(candidate_field_token_set):
        return {
            "match_type": "keyword_key",
            "matched_keywords": list(target_variants[0]),
            "score": (5, 0, len(candidate_field_token_set) - len(direct_token_set), candidate.depth, candidate.discovery_index),
        }
    if direct_token_set and direct_token_set.issubset(candidate_path_token_set):
        return {
            "match_type": "keyword_path",
            "matched_keywords": list(target_variants[0]),
            "score": (6, 0, len(candidate_path_token_set) - len(direct_token_set), candidate.depth, candidate.discovery_index),
        }
    return None


def _is_empty_source_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _find_first_match(
    field_name: str,
    source_roots: Sequence[Any],
    *,
    case_sensitive: bool,
    max_matches: int,
    expected_shape: str | None = None,
    extra_aliases: Sequence[str] = (),
) -> dict[str, Any] | None:
    best_match: dict[str, Any] | None = None
    best_score: tuple[Any, ...] | None = None
    for root_index, root in enumerate(source_roots):
        candidates = _walk_field_candidates(root, case_sensitive=case_sensitive, max_matches=max_matches)
        for candidate in candidates:
            if not _value_matches_expected_shape(candidate.value, expected_shape):
                continue
            if _is_empty_source_value(candidate.value):
                continue
            match_details = _match_details_for_candidate(
                candidate,
                field_name=field_name,
                case_sensitive=case_sensitive,
                extra_aliases=extra_aliases,
            )
            if match_details is None:
                continue
            candidate_score = (root_index, *tuple(match_details["score"]))
            if best_score is not None and candidate_score >= best_score:
                continue
            best_score = candidate_score
            best_match = {
                "field": candidate.field,
                "path": candidate.path,
                "value": candidate.value,
                "match_type": match_details["match_type"],
                "matched_keywords": list(match_details["matched_keywords"]),
                "source_root_index": root_index,
            }
    return best_match


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


def _normalize_field_aliases(
    field_aliases: Mapping[str, Sequence[str]] | None,
) -> dict[str, tuple[str, ...]]:
    if not field_aliases:
        return {}
    normalized: dict[str, tuple[str, ...]] = {}
    for raw_key, raw_values in field_aliases.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if isinstance(raw_values, str):
            raw_iterable: Sequence[Any] = [raw_values]
        elif isinstance(raw_values, Sequence):
            raw_iterable = raw_values
        else:
            continue
        cleaned: list[str] = []
        seen: set[str] = set()
        for entry in raw_iterable:
            text = str(entry or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            cleaned.append(text)
        if cleaned:
            normalized[key] = tuple(cleaned)
    return normalized


def _normalize_field_search_scopes(
    field_search_scopes: Mapping[str, Any] | None,
) -> dict[str, str]:
    """Normalize per-entry scope overrides to ``{key: "payload" | "metadata" | "artifacts"}``.

    Accepts plain strings (``{key: "metadata"}``), as well as the legacy mapping/sequence
    shapes that were briefly written to disk (``{key: {"metadata": bool, "artifacts": bool}}``
    or ``{key: [bool, bool]}``). Legacy shapes are best-effort migrated by picking the first
    enabled section in the order metadata → artifacts → payload.
    """
    if not field_search_scopes:
        return {}
    normalized: dict[str, str] = {}
    for raw_key, raw_scope in field_search_scopes.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if isinstance(raw_scope, str):
            normalized[key] = normalize_search_section(raw_scope)
            continue
        if isinstance(raw_scope, Mapping):
            sm = bool(raw_scope.get("metadata", True))
            sa = bool(raw_scope.get("artifacts", True))
            if sm and not sa:
                normalized[key] = SEARCH_SECTION_METADATA
            elif sa and not sm:
                normalized[key] = SEARCH_SECTION_ARTIFACTS
            else:
                normalized[key] = SEARCH_SECTION_PAYLOAD
            continue
        if isinstance(raw_scope, Sequence) and not isinstance(raw_scope, (str, bytes, bytearray)):
            scope_list = list(raw_scope)
            sm = bool(scope_list[0]) if len(scope_list) >= 1 else True
            sa = bool(scope_list[1]) if len(scope_list) >= 2 else True
            if sm and not sa:
                normalized[key] = SEARCH_SECTION_METADATA
            elif sa and not sm:
                normalized[key] = SEARCH_SECTION_ARTIFACTS
            else:
                normalized[key] = SEARCH_SECTION_PAYLOAD
    return normalized


def _section_for_field(
    field_name: str | None,
    *,
    normalized_scopes: Mapping[str, str],
    case_sensitive: bool,
    default_search_section: str,
) -> str:
    if not field_name:
        return default_search_section
    if field_name in normalized_scopes:
        return normalized_scopes[field_name]
    if not case_sensitive:
        lowered = field_name.lower()
        for key, scope in normalized_scopes.items():
            if key.lower() == lowered:
                return scope
    return default_search_section


def build_structured_payload(
    template: Mapping[str, Any],
    source_roots: Sequence[Any],
    *,
    case_sensitive: bool = False,
    max_matches_per_field: int = 25,
    field_aliases: Mapping[str, Sequence[str]] | None = None,
    source_root_kinds: Sequence[str] | None = None,
    field_search_scopes: Mapping[str, Any] | None = None,
    default_search_section: str = SEARCH_SECTION_PAYLOAD,
) -> StructuredPayloadBuildResult:
    filled_paths: list[str] = []
    preserved_paths: list[str] = []
    unresolved_paths: list[str] = []
    field_matches: list[dict[str, Any]] = []
    normalized_aliases = _normalize_field_aliases(field_aliases)
    normalized_scopes = _normalize_field_search_scopes(field_search_scopes)

    if source_root_kinds is not None and len(source_root_kinds) != len(source_roots):
        raise ValueError(
            "source_root_kinds must be parallel to source_roots when provided."
        )

    def aliases_for(field_name: str | None) -> tuple[str, ...]:
        if not field_name:
            return ()
        if normalized_aliases:
            direct = normalized_aliases.get(field_name)
            if direct:
                return direct
            if not case_sensitive:
                lowered = field_name.lower()
                for key, values in normalized_aliases.items():
                    if key.lower() == lowered:
                        return values
        return ()

    normalized_default_section = normalize_search_section(default_search_section)

    def scoped_roots_for(field_name: str | None, fallback_roots: Sequence[Any]) -> Sequence[Any]:
        if source_root_kinds is None:
            return fallback_roots
        section = _section_for_field(
            field_name,
            normalized_scopes=normalized_scopes,
            case_sensitive=case_sensitive,
            default_search_section=normalized_default_section,
        )
        # Prefix extras (contextual mappings derived from a parent match) have no kind
        # in the original lookup; preserve them regardless so nested templates still
        # see their container.
        prefix_extras: list[Any] = []
        original_subset: list[Any] = []
        original_lookup = {id(root): kind for root, kind in zip(source_roots, source_root_kinds)}
        for value in fallback_roots:
            kind = original_lookup.get(id(value))
            if kind is None:
                prefix_extras.append(value)
                continue
            if kind != section:
                continue
            original_subset.append(value)
        return [*prefix_extras, *original_subset]

    def build_value(
        template_value: Any,
        *,
        field_name: str | None,
        path: str,
        active_source_roots: Sequence[Any],
    ) -> Any:
        scoped_roots = scoped_roots_for(field_name, active_source_roots)
        contextual_roots = list(scoped_roots)
        field_aliases_for_call = aliases_for(field_name)
        if field_name:
            container_match = _find_first_match(
                field_name,
                scoped_roots,
                case_sensitive=case_sensitive,
                max_matches=max_matches_per_field,
                extra_aliases=field_aliases_for_call,
            )
            if container_match is not None and isinstance(container_match.get("value"), Mapping):
                contextual_roots = [container_match["value"], *contextual_roots]

        if isinstance(template_value, Mapping):
            if not template_value and field_name:
                match = _find_first_match(
                    field_name,
                    scoped_roots,
                    case_sensitive=case_sensitive,
                    max_matches=max_matches_per_field,
                    expected_shape="mapping",
                    extra_aliases=field_aliases_for_call,
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
                    scoped_roots,
                    case_sensitive=case_sensitive,
                    max_matches=max_matches_per_field,
                    expected_shape="list",
                    extra_aliases=field_aliases_for_call,
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
            scoped_roots,
            case_sensitive=case_sensitive,
            max_matches=max_matches_per_field,
            expected_shape="scalar",
            extra_aliases=field_aliases_for_call,
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
