import pytest

from semantic_model_generator.clickzetta_utils.utils import create_fqn_table
from semantic_model_generator.data_processing.data_types import FQNParts


def test_fqn_creation():
    input_name = "database.schema.table"

    fqn_parts = create_fqn_table(input_name)

    assert fqn_parts == FQNParts(
        database="DATABASE", schema_name="SCHEMA", table="table"
    )


def test_fqn_creation_invalid_name():
    input_name = "database.schema table"
    with pytest.raises(ValueError):
        create_fqn_table(input_name)
