import sys
from copy import deepcopy
from pathlib import Path

import pytest

CONTRACTS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CONTRACTS_ROOT / "src"))

from ledgermesh_contracts.compatibility import (  # noqa: E402
    compare_documents,
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
    old = {
        "paths": {
            "/v1/items": {
                "get": {
                    "operationId": "listItems",
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Response"}
                                }
                            }
                        }
                    },
                }
            }
        },
        "components": {"schemas": {"Response": object_schema}},
    }
    optional = deepcopy(old)
    optional["components"]["schemas"]["Response"]["properties"]["status"] = {"type": "string"}
    assert compare_openapi(old, optional, "items.v1.yaml") == []

    required = deepcopy(optional)
    required["components"]["schemas"]["Response"]["required"].append("status")
    assert compare_openapi(old, required, "items.v1.yaml")


def test_strict_event_envelope_rejects_a_new_property(object_schema):
    changed = deepcopy(object_schema)
    changed["properties"]["tenant_id"] = {"type": "string"}
    violations = compare_documents(
        object_schema,
        changed,
        "libs/contracts/schemas/events/event-envelope.v1.json",
    )
    assert any("existing readers reject it" in violation.reason for violation in violations)


def test_event_field_becoming_nullable_is_breaking(object_schema):
    changed = deepcopy(object_schema)
    changed["properties"]["id"]["type"] = ["string", "null"]
    violations = compare_documents(
        object_schema,
        changed,
        "libs/contracts/schemas/events/event-envelope.v1.json",
    )
    assert any("produced type widened" in violation.reason for violation in violations)


def test_response_required_field_becoming_optional_is_breaking(object_schema):
    old = {
        "paths": {
            "/v1/items": {
                "get": {
                    "operationId": "listItems",
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Response"}
                                }
                            }
                        }
                    },
                }
            }
        },
        "components": {"schemas": {"Response": object_schema}},
    }
    changed = deepcopy(old)
    changed["components"]["schemas"]["Response"]["required"].remove("id")
    violations = compare_openapi(old, changed, "items.v1.yaml")
    assert any("no longer guaranteed" in violation.reason for violation in violations)


@pytest.mark.parametrize("keyword", ["oneOf", "anyOf", "$defs", "dependentRequired"])
def test_changed_unsupported_keyword_fails_closed(keyword):
    old = {"type": "object", keyword: {"old": {"type": "string"}}}
    new = {"type": "object", keyword: {"new": {"type": "integer"}}}
    violations = compare_schema(old, new)
    assert any("manual review" in violation.reason for violation in violations)
