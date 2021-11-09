import pytest
from flyteidl.core import types_pb2

from flytekit.models.core import types as type_models
from tests.flytekit.common import parameterizers


def test_simple_type():
    assert type_models.SimpleType.NONE == types_pb2.NONE
    assert type_models.SimpleType.INTEGER == types_pb2.INTEGER
    assert type_models.SimpleType.FLOAT == types_pb2.FLOAT
    assert type_models.SimpleType.STRING == types_pb2.STRING
    assert type_models.SimpleType.BOOLEAN == types_pb2.BOOLEAN
    assert type_models.SimpleType.DATETIME == types_pb2.DATETIME
    assert type_models.SimpleType.DURATION == types_pb2.DURATION
    assert type_models.SimpleType.BINARY == types_pb2.BINARY
    assert type_models.SimpleType.ERROR == types_pb2.ERROR


def test_schema_column():
    assert type_models.SchemaType.SchemaColumn.SchemaColumnType.INTEGER == types_pb2.SchemaType.SchemaColumn.INTEGER
    assert type_models.SchemaType.SchemaColumn.SchemaColumnType.FLOAT == types_pb2.SchemaType.SchemaColumn.FLOAT
    assert type_models.SchemaType.SchemaColumn.SchemaColumnType.STRING == types_pb2.SchemaType.SchemaColumn.STRING
    assert type_models.SchemaType.SchemaColumn.SchemaColumnType.DATETIME == types_pb2.SchemaType.SchemaColumn.DATETIME
    assert type_models.SchemaType.SchemaColumn.SchemaColumnType.DURATION == types_pb2.SchemaType.SchemaColumn.DURATION
    assert type_models.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN == types_pb2.SchemaType.SchemaColumn.BOOLEAN


def test_schema_type():
    obj = type_models.SchemaType(
        [
            type_models.SchemaType.SchemaColumn("a", type_models.SchemaType.SchemaColumn.SchemaColumnType.INTEGER),
            type_models.SchemaType.SchemaColumn("b", type_models.SchemaType.SchemaColumn.SchemaColumnType.FLOAT),
            type_models.SchemaType.SchemaColumn("c", type_models.SchemaType.SchemaColumn.SchemaColumnType.STRING),
            type_models.SchemaType.SchemaColumn("d", type_models.SchemaType.SchemaColumn.SchemaColumnType.DATETIME),
            type_models.SchemaType.SchemaColumn("e", type_models.SchemaType.SchemaColumn.SchemaColumnType.DURATION),
            type_models.SchemaType.SchemaColumn("f", type_models.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN),
        ]
    )

    assert obj.columns[0].name == "a"
    assert obj.columns[1].name == "b"
    assert obj.columns[2].name == "c"
    assert obj.columns[3].name == "d"
    assert obj.columns[4].name == "e"
    assert obj.columns[5].name == "f"

    assert obj.columns[0].type == type_models.SchemaType.SchemaColumn.SchemaColumnType.INTEGER
    assert obj.columns[1].type == type_models.SchemaType.SchemaColumn.SchemaColumnType.FLOAT
    assert obj.columns[2].type == type_models.SchemaType.SchemaColumn.SchemaColumnType.STRING
    assert obj.columns[3].type == type_models.SchemaType.SchemaColumn.SchemaColumnType.DATETIME
    assert obj.columns[4].type == type_models.SchemaType.SchemaColumn.SchemaColumnType.DURATION
    assert obj.columns[5].type == type_models.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN

    assert obj == type_models.SchemaType.from_flyte_idl(obj.to_flyte_idl())


def test_literal_types():
    obj = type_models.LiteralType(simple=type_models.SimpleType.INTEGER)
    assert obj.simple == type_models.SimpleType.INTEGER
    assert obj.schema is None
    assert obj.collection_type is None
    assert obj.map_value_type is None
    assert obj == type_models.LiteralType.from_flyte_idl(obj.to_flyte_idl())

    schema_type = type_models.SchemaType(
        [
            type_models.SchemaType.SchemaColumn("a", type_models.SchemaType.SchemaColumn.SchemaColumnType.INTEGER),
            type_models.SchemaType.SchemaColumn("b", type_models.SchemaType.SchemaColumn.SchemaColumnType.FLOAT),
            type_models.SchemaType.SchemaColumn("c", type_models.SchemaType.SchemaColumn.SchemaColumnType.STRING),
            type_models.SchemaType.SchemaColumn("d", type_models.SchemaType.SchemaColumn.SchemaColumnType.DATETIME),
            type_models.SchemaType.SchemaColumn("e", type_models.SchemaType.SchemaColumn.SchemaColumnType.DURATION),
            type_models.SchemaType.SchemaColumn("f", type_models.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN),
        ]
    )
    obj = type_models.LiteralType(schema=schema_type)
    assert obj.simple is None
    assert obj.schema == schema_type
    assert obj.collection_type is None
    assert obj.map_value_type is None
    assert obj == type_models.LiteralType.from_flyte_idl(obj.to_flyte_idl())


@pytest.mark.parametrize("literal_type", parameterizers.LIST_OF_ALL_LITERAL_TYPES)
def test_literal_collections(literal_type):
    obj = type_models.LiteralType(collection_type=literal_type)
    assert obj.collection_type == literal_type
    assert obj.simple is None
    assert obj.schema is None
    assert obj.map_value_type is None
    assert obj == type_models.LiteralType.from_flyte_idl(obj.to_flyte_idl())


def test_output_reference():
    obj = type_models.OutputReference(node_id="node1", var="var1")
    assert obj.node_id == "node1"
    assert obj.var == "var1"
    obj2 = type_models.OutputReference.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
