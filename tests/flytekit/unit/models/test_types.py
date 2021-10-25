import pytest
from flyteidl.core import types_pb2

import flytekit.models.core.types
from tests.flytekit.common import parameterizers


def test_simple_type():
    assert flytekit.models.core.types.SimpleType.NONE == types_pb2.NONE
    assert flytekit.models.core.types.SimpleType.INTEGER == types_pb2.INTEGER
    assert flytekit.models.core.types.SimpleType.FLOAT == types_pb2.FLOAT
    assert flytekit.models.core.types.SimpleType.STRING == types_pb2.STRING
    assert flytekit.models.core.types.SimpleType.BOOLEAN == types_pb2.BOOLEAN
    assert flytekit.models.core.types.SimpleType.DATETIME == types_pb2.DATETIME
    assert flytekit.models.core.types.SimpleType.DURATION == types_pb2.DURATION
    assert flytekit.models.core.types.SimpleType.BINARY == types_pb2.BINARY
    assert flytekit.models.core.types.SimpleType.ERROR == types_pb2.ERROR


def test_schema_column():
    assert (
        flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.INTEGER
        == types_pb2.SchemaType.SchemaColumn.INTEGER
    )
    assert (
        flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.FLOAT
        == types_pb2.SchemaType.SchemaColumn.FLOAT
    )
    assert (
        flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.STRING
        == types_pb2.SchemaType.SchemaColumn.STRING
    )
    assert (
        flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DATETIME
        == types_pb2.SchemaType.SchemaColumn.DATETIME
    )
    assert (
        flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DURATION
        == types_pb2.SchemaType.SchemaColumn.DURATION
    )
    assert (
        flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN
        == types_pb2.SchemaType.SchemaColumn.BOOLEAN
    )


def test_schema_type():
    obj = flytekit.models.core.types.SchemaType(
        [
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "a", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.INTEGER
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "b", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.FLOAT
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "c", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.STRING
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "d", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DATETIME
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "e", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DURATION
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "f", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN
            ),
        ]
    )

    assert obj.columns[0].name == "a"
    assert obj.columns[1].name == "b"
    assert obj.columns[2].name == "c"
    assert obj.columns[3].name == "d"
    assert obj.columns[4].name == "e"
    assert obj.columns[5].name == "f"

    assert obj.columns[0].type == flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.INTEGER
    assert obj.columns[1].type == flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.FLOAT
    assert obj.columns[2].type == flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.STRING
    assert obj.columns[3].type == flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DATETIME
    assert obj.columns[4].type == flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DURATION
    assert obj.columns[5].type == flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN

    assert obj == flytekit.models.core.types.SchemaType.from_flyte_idl(obj.to_flyte_idl())


def test_literal_types():
    obj = flytekit.models.core.types.LiteralType(simple=flytekit.models.core.types.SimpleType.INTEGER)
    assert obj.simple == flytekit.models.core.types.SimpleType.INTEGER
    assert obj.schema is None
    assert obj.collection_type is None
    assert obj.map_value_type is None
    assert obj == flytekit.models.core.types.LiteralType.from_flyte_idl(obj.to_flyte_idl())

    schema_type = flytekit.models.core.types.SchemaType(
        [
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "a", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.INTEGER
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "b", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.FLOAT
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "c", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.STRING
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "d", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DATETIME
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "e", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.DURATION
            ),
            flytekit.models.core.types.SchemaType.SchemaColumn(
                "f", flytekit.models.core.types.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN
            ),
        ]
    )
    obj = flytekit.models.core.types.LiteralType(schema=schema_type)
    assert obj.simple is None
    assert obj.schema == schema_type
    assert obj.collection_type is None
    assert obj.map_value_type is None
    assert obj == flytekit.models.core.types.LiteralType.from_flyte_idl(obj.to_flyte_idl())


@pytest.mark.parametrize("literal_type", parameterizers.LIST_OF_ALL_LITERAL_TYPES)
def test_literal_collections(literal_type):
    obj = flytekit.models.core.types.LiteralType(collection_type=literal_type)
    assert obj.collection_type == literal_type
    assert obj.simple is None
    assert obj.schema is None
    assert obj.map_value_type is None
    assert obj == flytekit.models.core.types.LiteralType.from_flyte_idl(obj.to_flyte_idl())


def test_output_reference():
    obj = flytekit.models.core.types.OutputReference(node_id="node1", var="var1")
    assert obj.node_id == "node1"
    assert obj.var == "var1"
    obj2 = flytekit.models.core.types.OutputReference.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
