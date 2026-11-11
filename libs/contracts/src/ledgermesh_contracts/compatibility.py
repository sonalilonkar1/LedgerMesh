"""Conservative backward-compatibility checks for published v1 contracts."""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

HTTP_METHODS = {"get", "put", "post", "delete", "patch", "head", "options"}
PUBLISHED_PREFIXES = ("libs/contracts/openapi/", "libs/contracts/schemas/")


@dataclass(frozen=True)
class Violation:
    location: str
    reason: str

    def __str__(self) -> str:
        return f"{self.location}: {self.reason}"


def _types(schema: dict[str, Any]) -> set[str] | None:
    value = schema.get("type")
    if value is None:
        return None
    return {value} if isinstance(value, str) else set(value)


def compare_schema(old: Any, new: Any, location: str = "schema") -> list[Violation]:
    if not isinstance(old, dict) or not isinstance(new, dict):
        return [] if old == new else [Violation(location, "schema changed incompatibly")]

    violations: list[Violation] = []
    if "$ref" in old and old.get("$ref") != new.get("$ref"):
        violations.append(
            Violation(
                location,
                f"schema reference changed from {old.get('$ref')!r} to {new.get('$ref')!r}",
            )
        )
    old_types, new_types = _types(old), _types(new)
    if old_types is not None and new_types is not None and not old_types <= new_types:
        violations.append(
            Violation(location, f"type changed from {sorted(old_types)} to {sorted(new_types)}")
        )

    old_properties = old.get("properties", {})
    new_properties = new.get("properties", {})
    if isinstance(old_properties, dict) and isinstance(new_properties, dict):
        for name in sorted(set(old_properties) - set(new_properties)):
            violations.append(
                Violation(f"{location}.properties.{name}", "published field was removed or renamed")
            )
        newly_required = set(new.get("required", [])) - set(old.get("required", []))
        for name in sorted(newly_required):
            violations.append(Violation(f"{location}.required", f"field {name!r} became required"))
        for name in sorted(set(old_properties) & set(new_properties)):
            violations.extend(
                compare_schema(
                    old_properties[name],
                    new_properties[name],
                    f"{location}.properties.{name}",
                )
            )

    old_enum, new_enum = old.get("enum"), new.get("enum")
    if isinstance(old_enum, list) and isinstance(new_enum, list):
        removed = [value for value in old_enum if value not in new_enum]
        if removed:
            violations.append(Violation(location, f"enum values were removed: {removed!r}"))
    if "const" in old and old.get("const") != new.get("const"):
        violations.append(
            Violation(location, f"const changed from {old.get('const')!r} to {new.get('const')!r}")
        )
    if (
        old.get("additionalProperties", True) is not False
        and new.get("additionalProperties") is False
    ):
        violations.append(Violation(location, "additional properties are no longer accepted"))

    for keyword in ("minimum", "minLength", "minItems"):
        if keyword in new and (keyword not in old or new[keyword] > old[keyword]):
            violations.append(Violation(location, f"{keyword} became more restrictive"))
    for keyword in ("maximum", "maxLength", "maxItems"):
        if keyword in new and (keyword not in old or new[keyword] < old[keyword]):
            violations.append(Violation(location, f"{keyword} became more restrictive"))
    if "pattern" in new and old.get("pattern") != new.get("pattern"):
        violations.append(Violation(location, "pattern was added or changed"))
    if "format" in new and old.get("format") != new.get("format"):
        violations.append(Violation(location, "format was added or changed"))
    if "items" in old:
        if "items" not in new:
            violations.append(Violation(f"{location}.items", "array item schema was removed"))
        else:
            violations.extend(compare_schema(old["items"], new["items"], f"{location}.items"))
    return violations


def _parameter_key(parameter: dict[str, Any]) -> tuple[Any, Any]:
    if "$ref" in parameter:
        return "$ref", parameter["$ref"]
    return parameter.get("in"), parameter.get("name")


def _compare_parameters(old: list[Any], new: list[Any], location: str) -> list[Violation]:
    old_parameters = {_parameter_key(item): item for item in old if isinstance(item, dict)}
    new_parameters = {_parameter_key(item): item for item in new if isinstance(item, dict)}
    violations = []
    for key in sorted(set(old_parameters) - set(new_parameters), key=str):
        violations.append(Violation(location, f"parameter {key!r} was removed or renamed"))
    for key in sorted(set(new_parameters) - set(old_parameters), key=str):
        if key[0] == "$ref" or new_parameters[key].get("required"):
            violations.append(Violation(location, f"required parameter {key!r} was added"))
    for key in set(old_parameters) & set(new_parameters):
        if key[0] == "$ref":
            continue
        if not old_parameters[key].get("required") and new_parameters[key].get("required"):
            violations.append(Violation(location, f"parameter {key!r} became required"))
        violations.extend(
            compare_schema(
                old_parameters[key].get("schema", {}),
                new_parameters[key].get("schema", {}),
                f"{location}.{key}",
            )
        )
    return violations


def _compare_content(old: dict[str, Any], new: dict[str, Any], location: str) -> list[Violation]:
    violations = []
    for media_type in sorted(set(old) - set(new)):
        violations.append(Violation(location, f"media type {media_type!r} was removed"))
    for media_type in set(old) & set(new):
        violations.extend(
            compare_schema(
                old[media_type].get("schema", {}),
                new[media_type].get("schema", {}),
                f"{location}.{media_type}",
            )
        )
    return violations


def compare_openapi(old: dict[str, Any], new: dict[str, Any], filename: str) -> list[Violation]:
    violations: list[Violation] = []
    old_paths, new_paths = old.get("paths", {}), new.get("paths", {})
    for route in sorted(set(old_paths) - set(new_paths)):
        violations.append(Violation(f"{filename}:paths.{route}", "published endpoint was removed"))
    for route in set(old_paths) & set(new_paths):
        old_item, new_item = old_paths[route], new_paths[route]
        old_methods = {key for key in old_item if key.lower() in HTTP_METHODS}
        new_methods = {key for key in new_item if key.lower() in HTTP_METHODS}
        for method in sorted(old_methods - new_methods):
            violations.append(
                Violation(f"{filename}:{method.upper()} {route}", "published operation was removed")
            )
        for method in old_methods & new_methods:
            location = f"{filename}:{method.upper()} {route}"
            old_operation, new_operation = old_item[method], new_item[method]
            if old_operation.get("operationId") != new_operation.get("operationId"):
                violations.append(Violation(location, "operationId was removed or changed"))
            violations.extend(
                _compare_parameters(
                    old_item.get("parameters", []) + old_operation.get("parameters", []),
                    new_item.get("parameters", []) + new_operation.get("parameters", []),
                    f"{location}.parameters",
                )
            )
            old_body, new_body = old_operation.get("requestBody"), new_operation.get("requestBody")
            if old_body and not new_body:
                violations.append(Violation(f"{location}.requestBody", "request body was removed"))
            elif old_body and new_body:
                if not old_body.get("required") and new_body.get("required"):
                    violations.append(
                        Violation(f"{location}.requestBody", "request body became required")
                    )
                violations.extend(
                    _compare_content(
                        old_body.get("content", {}),
                        new_body.get("content", {}),
                        f"{location}.requestBody",
                    )
                )
            old_responses = old_operation.get("responses", {})
            new_responses = new_operation.get("responses", {})
            for status in sorted(set(old_responses) - set(new_responses)):
                violations.append(
                    Violation(f"{location}.responses.{status}", "response was removed")
                )
            for status in set(old_responses) & set(new_responses):
                response_location = f"{location}.responses.{status}"
                if old_responses[status].get("$ref") != new_responses[status].get("$ref"):
                    violations.append(
                        Violation(response_location, "response reference was removed or changed")
                    )
                else:
                    violations.extend(
                        _compare_content(
                            old_responses[status].get("content", {}),
                            new_responses[status].get("content", {}),
                            response_location,
                        )
                    )

    old_schemas = old.get("components", {}).get("schemas", {})
    new_schemas = new.get("components", {}).get("schemas", {})
    for name in sorted(set(old_schemas) - set(new_schemas)):
        violations.append(
            Violation(
                f"{filename}:components.schemas.{name}",
                "published schema was removed or renamed",
            )
        )
    for name in set(old_schemas) & set(new_schemas):
        violations.extend(
            compare_schema(
                old_schemas[name],
                new_schemas[name],
                f"{filename}:components.schemas.{name}",
            )
        )
    old_parameters = old.get("components", {}).get("parameters", {})
    new_parameters = new.get("components", {}).get("parameters", {})
    for name in sorted(set(old_parameters) - set(new_parameters)):
        violations.append(
            Violation(f"{filename}:components.parameters.{name}", "published parameter was removed")
        )
    for name in set(old_parameters) & set(new_parameters):
        old_parameter, new_parameter = old_parameters[name], new_parameters[name]
        if (old_parameter.get("name"), old_parameter.get("in")) != (
            new_parameter.get("name"),
            new_parameter.get("in"),
        ):
            violations.append(
                Violation(
                    f"{filename}:components.parameters.{name}", "parameter name or location changed"
                )
            )
        if not old_parameter.get("required") and new_parameter.get("required"):
            violations.append(
                Violation(f"{filename}:components.parameters.{name}", "parameter became required")
            )
        violations.extend(
            compare_schema(
                old_parameter.get("schema", {}),
                new_parameter.get("schema", {}),
                f"{filename}:components.parameters.{name}.schema",
            )
        )
    return violations


def compare_documents(old: dict[str, Any], new: dict[str, Any], filename: str) -> list[Violation]:
    if filename.startswith("libs/contracts/openapi/"):
        return compare_openapi(old, new, filename)
    return compare_schema(old, new, filename)


def _git(*args: str) -> str:
    result = subprocess.run(["git", *args], check=True, capture_output=True, text=True)
    return result.stdout


def _load_text(text: str, filename: str) -> dict[str, Any]:
    document = json.loads(text) if filename.endswith(".json") else yaml.safe_load(text)
    if not isinstance(document, dict):
        raise ValueError(f"{filename}: document root must be an object")
    return document


def check_base_ref(base_ref: str, repository_root: Path) -> list[Violation]:
    files = _git(
        "-C",
        str(repository_root),
        "ls-tree",
        "-r",
        "--name-only",
        base_ref,
        "--",
        *PUBLISHED_PREFIXES,
    ).splitlines()
    published = [
        name for name in files if ".v1." in name and name.endswith((".yaml", ".yml", ".json"))
    ]
    violations: list[Violation] = []
    for name in published:
        current_path = repository_root / name
        if not current_path.exists():
            violations.append(Violation(name, "published v1 contract file was removed or renamed"))
            continue
        old = _load_text(_git("-C", str(repository_root), "show", f"{base_ref}:{name}"), name)
        new = _load_text(current_path.read_text(), name)
        violations.extend(compare_documents(old, new, name))
    return violations


def render_report(base_ref: str, violations: list[Violation]) -> str:
    if not violations:
        return (
            "# Contract compatibility\n\n"
            f"✅ No backward-incompatible v1 changes relative to `{base_ref}`.\n"
        )
    lines = [
        "# Contract compatibility\n\n"
        f"❌ Found {len(violations)} backward-incompatible v1 change(s) "
        f"relative to `{base_ref}`:\n"
    ]
    lines.extend(f"- `{violation.location}` — {violation.reason}" for violation in violations)
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    repository_root = Path(__file__).resolve().parents[4]
    violations = check_base_ref(args.base_ref, repository_root)
    report = render_report(args.base_ref, violations)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report)
    print(report, end="")
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
