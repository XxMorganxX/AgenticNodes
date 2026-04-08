from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import re
from typing import Any

from jsonschema import exceptions as jsonschema_exceptions
from jsonschema.validators import validator_for


_GENERIC_OBJECT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
}
_REQUIRED_PROPERTY_PATTERN = re.compile(r"'(.+)' is a required property")


@dataclass(frozen=True)
class NormalizedObjectSchema:
    schema: dict[str, Any]
    used_fallback: bool = False
    warning: str | None = None


def _json_compatible_copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_compatible_copy(child) for key, child in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_json_compatible_copy(child) for child in value]
    return value


def _looks_like_object_schema(schema: Mapping[str, Any]) -> bool:
    schema_type = schema.get("type")
    if schema_type == "object":
        return True
    if isinstance(schema_type, Sequence) and not isinstance(schema_type, (str, bytes)):
        return "object" in {str(entry).strip() for entry in schema_type}
    return any(key in schema for key in ("properties", "required", "additionalProperties"))


def _fallback_schema_warning(reason: str) -> str:
    return (
        "Tool input schema could not be validated as a JSON object schema; "
        f"falling back to a generic object contract. Reason: {reason}"
    )


def normalize_object_json_schema(schema: Mapping[str, Any] | Any) -> NormalizedObjectSchema:
    if not isinstance(schema, Mapping):
        return NormalizedObjectSchema(
            schema=dict(_GENERIC_OBJECT_SCHEMA),
            used_fallback=True,
            warning=_fallback_schema_warning("schema is not an object"),
        )

    normalized = _json_compatible_copy(schema)
    if not isinstance(normalized, dict):
        return NormalizedObjectSchema(
            schema=dict(_GENERIC_OBJECT_SCHEMA),
            used_fallback=True,
            warning=_fallback_schema_warning("schema normalization failed"),
        )

    if "type" not in normalized and _looks_like_object_schema(normalized):
        normalized["type"] = "object"

    if not _looks_like_object_schema(normalized):
        return NormalizedObjectSchema(
            schema=dict(_GENERIC_OBJECT_SCHEMA),
            used_fallback=True,
            warning=_fallback_schema_warning("top-level schema does not describe an object"),
        )

    try:
        validator_cls = validator_for(normalized)
        validator_cls.check_schema(normalized)
    except jsonschema_exceptions.SchemaError as exc:
        return NormalizedObjectSchema(
            schema=dict(_GENERIC_OBJECT_SCHEMA),
            used_fallback=True,
            warning=_fallback_schema_warning(exc.message),
        )
    return NormalizedObjectSchema(schema=normalized)


def _format_instance_path(path: Sequence[Any]) -> str:
    if not path:
        return "$"
    formatted = "$"
    for segment in path:
        if isinstance(segment, int):
            formatted += f"[{segment}]"
            continue
        formatted += f".{segment}"
    return formatted


def _format_schema_path(path: Sequence[Any]) -> str:
    if not path:
        return "#"
    escaped_segments = [str(segment).replace("~", "~0").replace("/", "~1") for segment in path]
    return "#/" + "/".join(escaped_segments)


def _json_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return "array"
    return type(value).__name__


def _preview_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Mapping):
        keys = [str(key) for key in list(value.keys())[:5]]
        return {"type": "object", "keys": keys}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return {"type": "array", "length": len(value)}
    return repr(value)


def _expected_value(error: jsonschema_exceptions.ValidationError) -> Any:
    if error.validator == "type":
        return error.validator_value
    if error.validator == "enum":
        return list(error.validator_value) if isinstance(error.validator_value, Sequence) else error.validator_value
    if error.validator == "required":
        return error.validator_value
    if error.validator == "const":
        return error.validator_value
    if error.validator == "additionalProperties":
        return "no additional properties"
    return error.validator_value


def _missing_required_field(error: jsonschema_exceptions.ValidationError) -> str | None:
    if error.validator != "required":
        return None
    match = _REQUIRED_PROPERTY_PATTERN.search(error.message)
    if match is None:
        return None
    field_name = match.group(1).strip()
    if not field_name:
        return None
    return field_name


def _field_name_from_path(path: Sequence[Any]) -> str:
    if not path:
        return "$"
    return ".".join(str(segment) for segment in path)


def format_validation_errors(errors: Sequence[jsonschema_exceptions.ValidationError]) -> list[dict[str, Any]]:
    normalized_errors: list[dict[str, Any]] = []
    for error in errors:
        normalized_errors.append(
            {
                "path": _format_instance_path(error.path),
                "schema_path": _format_schema_path(error.schema_path),
                "validator": error.validator,
                "expected": _expected_value(error),
                "received": _json_type_name(error.instance) if error.validator == "type" else _preview_value(error.instance),
                "detail": error.message,
            }
        )
    return normalized_errors


def summarize_validation_errors(
    errors: Sequence[jsonschema_exceptions.ValidationError],
) -> tuple[list[str], list[dict[str, str]]]:
    missing_fields: list[str] = []
    seen_missing_fields: set[str] = set()
    type_errors: list[dict[str, str]] = []
    for error in errors:
        missing_field = _missing_required_field(error)
        if missing_field is not None and missing_field not in seen_missing_fields:
            seen_missing_fields.add(missing_field)
            missing_fields.append(missing_field)
        if error.validator == "type":
            type_errors.append(
                {
                    "field": _field_name_from_path(error.path),
                    "expected": str(error.validator_value),
                    "received": _json_type_name(error.instance),
                }
            )
    return missing_fields, type_errors


def validate_json_instance(
    instance: Any,
    schema: Mapping[str, Any] | Any,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, str]], str | None]:
    normalized_schema = normalize_object_json_schema(schema)
    validator_cls = validator_for(normalized_schema.schema)
    validator = validator_cls(normalized_schema.schema)
    errors = sorted(validator.iter_errors(instance), key=lambda error: (_format_instance_path(error.path), error.message))
    formatted_errors = format_validation_errors(errors)
    missing_fields, type_errors = summarize_validation_errors(errors)
    return formatted_errors, missing_fields, type_errors, normalized_schema.warning


def validation_error_payload(
    instance: Any,
    schema: Mapping[str, Any] | Any,
    *,
    default_message: str = "Payload does not match the tool input schema.",
) -> dict[str, Any] | None:
    formatted_errors, missing_fields, type_errors, schema_warning = validate_json_instance(instance, schema)
    if not formatted_errors:
        return None
    message = default_message
    if missing_fields and not type_errors and len(formatted_errors) == len(missing_fields):
        message = "Missing required fields."
    elif type_errors and not missing_fields and len(formatted_errors) == len(type_errors):
        message = "One or more fields have invalid types."
    payload: dict[str, Any] = {
        "message": message,
        "validation_errors": formatted_errors,
    }
    if missing_fields:
        payload["missing_fields"] = missing_fields
    if type_errors:
        payload["type_errors"] = type_errors
    if schema_warning:
        payload["schema_warning"] = schema_warning
    return payload
