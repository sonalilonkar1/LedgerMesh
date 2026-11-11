import sys
from copy import deepcopy
from pathlib import Path

import pytest

CONTRACTS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CONTRACTS_ROOT / "src"))

from ledgermesh_contracts.compatibility import (  # noqa: E402
    compare_openapi,
    compare_schema,
)


@pytest.fixture
def object_schema():
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["id"],
        "properties": {"id": {"type": "string"}},
    }


def test_optional_field_addition_is_backward_compatible(object_schema):
    changed = deepcopy(object_schema)
    changed["properties"]["note"] = {"type": "string"}
    assert compare_schema(object_schema, changed) == []


def test_new_required_field_is_breaking(object_schema):
    changed = deepcopy(object_schema)
    changed["properties"]["status"] = {"type": "string"}
    changed["required"].append("status")
    violations = compare_schema(object_schema, changed)
    assert any("became required" in violation.reason for violation in violations)


@pytest.mark.parametrize("change", ["remove", "rename", "type"])
def test_removed_renamed_or_retyped_field_is_breaking(object_schema, change):
    changed = deepcopy(object_schema)
    if change == "remove":
        del changed["properties"]["id"]
    elif change == "rename":
        changed["properties"]["identifier"] = changed["properties"].pop("id")
    else:
        changed["properties"]["id"]["type"] = "integer"
    assert compare_schema(object_schema, changed)


def test_removed_enum_value_is_breaking():
    violations = compare_schema(
        {"type": "string", "enum": ["PENDING", "DONE"]},
        {"type": "string", "enum": ["DONE"]},
    )
    assert any("enum values were removed" in violation.reason for violation in violations)


def test_removed_endpoint_is_breaking():
    old = {"paths": {"/v1/items": {"get": {"operationId": "listItems", "responses": {"200": {}}}}}}
    violations = compare_openapi(old, {"paths": {}}, "items.v1.yaml")
    assert any("endpoint was removed" in violation.reason for violation in violations)


def test_removed_response_is_breaking():
    old = {
        "paths": {
            "/v1/items": {
                "get": {
                    "operationId": "listItems",
                    "responses": {"200": {}, "404": {}},
                }
            }
        }
    }
    new = deepcopy(old)
    del new["paths"]["/v1/items"]["get"]["responses"]["404"]
    violations = compare_openapi(old, new, "items.v1.yaml")
    assert any("response was removed" in violation.reason for violation in violations)


def test_changed_response_reference_is_breaking():
    old = {
        "paths": {
            "/v1/items": {
                "get": {
                    "operationId": "listItems",
                    "responses": {"200": {"$ref": "#/components/responses/Items"}},
                }
            }
        }
    }
    new = deepcopy(old)
    new["paths"]["/v1/items"]["get"]["responses"]["200"]["$ref"] = "#/components/responses/Other"
    violations = compare_openapi(old, new, "items.v1.yaml")
    assert any("response reference" in violation.reason for violation in violations)


def test_optional_response_field_passes_but_required_response_field_fails(object_schema):
    old = {"paths": {}, "components": {"schemas": {"Response": object_schema}}}
    optional = deepcopy(old)
    optional["components"]["schemas"]["Response"]["properties"]["status"] = {"type": "string"}
    assert compare_openapi(old, optional, "items.v1.yaml") == []

    required = deepcopy(optional)
    required["components"]["schemas"]["Response"]["required"].append("status")
    assert compare_openapi(old, required, "items.v1.yaml")
