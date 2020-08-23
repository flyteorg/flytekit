import typing
from datetime import timedelta

from flytekit.annotated import type_engine
from flytekit.models import types as model_types


def test_type_engine():
    e = type_engine.BaseEngine()

    t = int
    lt = e.native_type_to_literal_type(t)
    assert lt.simple == model_types.SimpleType.INTEGER

    t = typing.Dict[str, typing.List[typing.Dict[str, timedelta]]]
    lt = e.native_type_to_literal_type(t)
    assert lt.map_value_type.collection_type.map_value_type.simple == model_types.SimpleType.DURATION


def test_named_tuple():
    e = type_engine.BaseEngine()
    t = typing.NamedTuple("Outputs", [('x_str', str), ('y_int', int)])
    var_map = e.named_tuple_to_variable_map(t)
    assert var_map.variables["x_str"].type.simple == model_types.SimpleType.STRING
    assert var_map.variables["y_int"].type.simple == model_types.SimpleType.INTEGER
