import json
from pathlib import Path

import pytest


def test_scheduler_job_spec_example_validates_against_schema():
    schema_path = Path("schemas/scheduler_job_spec.schema.json")
    example_path = Path("schemas/examples/scheduler_job_spec.example.json")

    assert schema_path.exists(), "schema file missing"
    assert example_path.exists(), "example file missing"

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    example = json.loads(example_path.read_text(encoding="utf-8"))

    try:
        import jsonschema
    except ImportError as e:
        pytest.fail(
            "Missing dependency: jsonschema. Install it (for now): pip install jsonschema",
            pytrace=False,
        )

    jsonschema.validate(instance=example, schema=schema)
