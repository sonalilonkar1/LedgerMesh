"""Offline validation helpers for LedgerMesh contracts."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml
from jsonschema.validators import validator_for
from openapi_spec_validator import validate


class ContractValidationError(ValueError):
    """Raised when a contract cannot be validated safely and offline."""


def load_document(path: Path) -> dict[str, Any]:
    try:
        document = (
            json.loads(path.read_text())
            if path.suffix == ".json"
            else yaml.safe_load(path.read_text())
        )
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise ContractValidationError(f"{path}: cannot parse document: {exc}") from exc
    if not isinstance(document, dict):
        raise ContractValidationError(f"{path}: document root must be an object")
    return document


def iter_references(value: Any, location: str = "$") -> Iterator[tuple[str, str]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_location = f"{location}.{key}"
            if key == "$ref" and isinstance(child, str):
                yield child_location, child
            else:
                yield from iter_references(child, child_location)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from iter_references(child, f"{location}[{index}]")


def resolve_json_pointer(document: Any, reference: str) -> Any:
    if reference == "#":
        return document
    if not reference.startswith("#/"):
        raise ContractValidationError(
            f"external reference {reference!r} is forbidden; contracts must validate offline"
        )
    current = document
    for raw_token in reference[2:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        try:
            current = current[int(token)] if isinstance(current, list) else current[token]
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ContractValidationError(f"unresolved reference {reference!r}") from exc
    return current


def validate_local_references(document: dict[str, Any], path: Path) -> None:
    for location, reference in iter_references(document):
        try:
            resolve_json_pointer(document, reference)
        except ContractValidationError as exc:
            raise ContractValidationError(f"{path}:{location}: {exc}") from exc


def operation_ids(document: dict[str, Any]) -> Iterator[tuple[str, str]]:
    for route, path_item in document.get("paths", {}).items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method.lower() not in {"get", "put", "post", "delete", "patch", "head", "options"}:
                continue
            if isinstance(operation, dict) and "operationId" in operation:
                yield f"{method.upper()} {route}", operation["operationId"]


def validate_openapi_document(document: dict[str, Any], path: Path) -> None:
    validate_local_references(document, path)
    try:
        validate(document)
    except Exception as exc:
        raise ContractValidationError(f"{path}: invalid OpenAPI document: {exc}") from exc


def validate_json_schema(document: dict[str, Any], path: Path) -> None:
    validate_local_references(document, path)
    try:
        validator_for(document).check_schema(document)
    except Exception as exc:
        raise ContractValidationError(f"{path}: invalid JSON Schema: {exc}") from exc
