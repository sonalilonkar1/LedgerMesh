"""The running app matches libs/contracts/openapi/identity.v1.yaml (LM-052).

The shared contract checks validate the document and operationIds; this test
makes sure the service actually serves what the contract publishes.
"""

import re
from pathlib import Path

import yaml

from services.identity.app.db import schema
from services.identity.app.main import app
from services.identity.app.models import LoginRequest, VerifyRequest

CONTRACT = Path(__file__).resolve().parents[3] / "libs/contracts/openapi/identity.v1.yaml"
METHODS = {"get", "post", "put", "patch", "delete"}
# No credential of any kind is accepted or stored; the pattern avoids spelling it out.
CREDENTIAL_FIELD = re.compile("pass|pwd", re.IGNORECASE)


def contract() -> dict:
    return yaml.safe_load(CONTRACT.read_text())


def operations(paths: dict) -> set[tuple[str, str]]:
    return {(path, method) for path, item in paths.items() for method in item if method in METHODS}


def test_app_routes_match_the_contract():
    served = operations(app.openapi()["paths"]) - {("/health", "get")}

    assert served == operations(contract()["paths"])


def test_request_bodies_and_status_codes_match():
    published = contract()["paths"]
    served = app.openapi()["paths"]
    for path, method in operations(published):
        published_codes = set(published[path][method]["responses"])
        served_codes = set(served[path][method]["responses"]) - {"422"}
        assert served_codes <= published_codes, (path, method, served_codes - published_codes)


def test_no_credential_fields_anywhere():
    names = [column.name for table in schema.metadata.tables.values() for column in table.columns]
    names += list(LoginRequest.model_fields) + list(VerifyRequest.model_fields)

    def properties(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "properties":
                    yield from child
                yield from properties(child)
        elif isinstance(value, list):
            for child in value:
                yield from properties(child)

    names += list(properties(contract()))
    assert not [name for name in names if CREDENTIAL_FIELD.search(name)]
