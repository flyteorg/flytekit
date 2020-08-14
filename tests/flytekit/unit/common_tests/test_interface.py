from __future__ import absolute_import
from flytekit.common import interface
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import primitives, containers
import pytest


def test_binding_data_primitive_static():
    upstream_nodes = set()
    bd = interface.BindingData.from_python_std(
        primitives.Float.to_flyte_literal_type(), 3.0, upstream_nodes=upstream_nodes
    )

    assert len(upstream_nodes) == 0
    assert bd.promise is None
    assert bd.collection is None
    assert bd.map is None
    assert bd.scalar.primitive.float_value == 3.0

    assert interface.BindingData.from_flyte_idl(bd.to_flyte_idl()) == bd

    with pytest.raises(_user_exceptions.FlyteTypeException):
        interface.BindingData.from_python_std(
            primitives.Float.to_flyte_literal_type(), "abc",
        )

    with pytest.raises(_user_exceptions.FlyteTypeException):
        interface.BindingData.from_python_std(
            primitives.Float.to_flyte_literal_type(), [1.0, 2.0, 3.0],
        )


def test_binding_data_list_static():
    upstream_nodes = set()
    bd = interface.BindingData.from_python_std(
        containers.List(primitives.String).to_flyte_literal_type(),
        ["abc", "cde"],
        upstream_nodes=upstream_nodes,
    )

    assert len(upstream_nodes) == 0
    assert bd.promise is None
    assert bd.collection.bindings[0].scalar.primitive.string_value == "abc"
    assert bd.collection.bindings[1].scalar.primitive.string_value == "cde"
    assert bd.map is None
    assert bd.scalar is None

    assert interface.BindingData.from_flyte_idl(bd.to_flyte_idl()) == bd

    with pytest.raises(_user_exceptions.FlyteTypeException):
        interface.BindingData.from_python_std(
            containers.List(primitives.String).to_flyte_literal_type(), "abc",
        )

    with pytest.raises(_user_exceptions.FlyteTypeException):
        interface.BindingData.from_python_std(
            containers.List(primitives.String).to_flyte_literal_type(), [1.0, 2.0, 3.0]
        )


def test_binding_generic_map_static():
    upstream_nodes = set()
    bd = interface.BindingData.from_python_std(
        primitives.Generic.to_flyte_literal_type(),
        {"a": "hi", "b": [1, 2, 3], "c": {"d": "e"}},
        upstream_nodes=upstream_nodes,
    )

    assert len(upstream_nodes) == 0
    assert bd.promise is None
    assert bd.map is None
    assert bd.scalar.generic["a"] == "hi"
    assert bd.scalar.generic["b"].values[0].number_value == 1.0
    assert bd.scalar.generic["b"].values[1].number_value == 2.0
    assert bd.scalar.generic["b"].values[2].number_value == 3.0
    assert bd.scalar.generic["c"]["d"] == "e"
    assert interface.BindingData.from_flyte_idl(bd.to_flyte_idl()) == bd
