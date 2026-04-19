from __future__ import annotations

import re
from typing import Any, Mapping

VARIABLE_PATTERN = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_.-]*)\s*\}\}")


def resolve_template_value(key: str, variables: Mapping[str, Any] | None = None, default: Any = None) -> Any:
    if not variables:
        return default
    current: Any = variables
    for part in key.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
            continue
        return default
    return current


def render_template(template: str, variables: Mapping[str, Any] | None = None, keep_missing: bool = True) -> str:
    variables = variables or {}

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = resolve_template_value(key, variables, default=match.group(0) if keep_missing else "")
        return "" if value is None else str(value)

    return VARIABLE_PATTERN.sub(replace, template)


def render_structure(payload: Any, variables: Mapping[str, Any] | None = None, keep_missing: bool = True) -> Any:
    if isinstance(payload, str):
        return render_template(payload, variables, keep_missing=keep_missing)
    if isinstance(payload, dict):
        return {key: render_structure(value, variables, keep_missing=keep_missing) for key, value in payload.items()}
    if isinstance(payload, list):
        return [render_structure(item, variables, keep_missing=keep_missing) for item in payload]
    if isinstance(payload, tuple):
        return tuple(render_structure(item, variables, keep_missing=keep_missing) for item in payload)
    return payload


def collect_template_variables(payload: Any) -> set[str]:
    if isinstance(payload, str):
        return set(VARIABLE_PATTERN.findall(payload))
    if isinstance(payload, dict):
        result: set[str] = set()
        for value in payload.values():
            result.update(collect_template_variables(value))
        return result
    if isinstance(payload, (list, tuple)):
        result: set[str] = set()
        for item in payload:
            result.update(collect_template_variables(item))
        return result
    return set()
