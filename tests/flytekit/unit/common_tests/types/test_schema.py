from __future__ import absolute_import
from flytekit.common.types import schema, primitives
from flytekit.common.types.impl import schema as schema_impl
from flytekit.sdk import test_utils


_ALL_COLUMN_TYPES = [
    ('a', primitives.Integer),
    ('b', primitives.String),
    ('c', primitives.Float),
    ('d', primitives.Datetime),
    ('e', primitives.Timedelta),
    ('f', primitives.Boolean),
]


def test_generic_schema_instantiator():
    instantiator = schema.schema_instantiator()
    b = instantiator.create_at_known_location("abc")
    assert isinstance(b, schema_impl.Schema)
    assert b.remote_location == "abc/"
    assert b.mode == "wb"
    assert len(b.type.columns) == 0


def test_typed_schema_instantiator():
    instantiator = schema.schema_instantiator(_ALL_COLUMN_TYPES)
    b = instantiator.create_at_known_location("abc")
    assert isinstance(b, schema_impl.Schema)
    assert b.remote_location == "abc/"
    assert b.mode == "wb"
    assert len(b.type.columns) == len(_ALL_COLUMN_TYPES)
    assert list(b.type.sdk_columns.items()) == _ALL_COLUMN_TYPES


def test_generic_schema():
    with test_utils.LocalTestFileSystem() as t:
        instantiator = schema.schema_instantiator()
        b = instantiator()
        assert isinstance(b, schema_impl.Schema)
        assert b.mode == "wb"
        assert len(b.type.columns) == 0
        assert b.remote_location.startswith(t.name)


def test_typed_schema():
    with test_utils.LocalTestFileSystem() as t:
        instantiator = schema.schema_instantiator(_ALL_COLUMN_TYPES)
        b = instantiator()
        assert isinstance(b, schema_impl.Schema)
        assert b.mode == "wb"
        assert len(b.type.columns) == len(_ALL_COLUMN_TYPES)
        assert list(b.type.sdk_columns.items()) == _ALL_COLUMN_TYPES
        assert b.remote_location.startswith(t.name)


# Ensures that subclassing types works inside a schema.
def test_casting():
    class MyDateTime(primitives.Datetime):
        ...

    with test_utils.LocalTestFileSystem() as t:
        test_columns_1 = [('altered', MyDateTime)]
        test_columns_2 = [('altered', primitives.Datetime)]

        instantiator_1 = schema.schema_instantiator(test_columns_1)
        a = instantiator_1()

        instantiator_2 = schema.schema_instantiator(test_columns_2)

        a.cast_to(instantiator_2._schema_type)
