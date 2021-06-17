import pytest

from flytekit import FlyteContextManager
from flytekit.common import promise
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types, primitives
from flytekit.core.interface import Interface
from flytekit.core.promise import Promise, create_native_named_tuple
from flytekit.core.type_engine import TypeEngine
from flytekit.models.types import LiteralType, SimpleType


def test_input():
    i = promise.Input("name", primitives.Integer, help="blah", default=None)
    assert i.name == "name"
    assert i.sdk_default is None
    assert i.default == base_sdk_types.Void()
    assert i.sdk_required is False
    assert i.help == "blah"
    assert i.var.description == "blah"
    assert i.sdk_type == primitives.Integer

    i = promise.Input("name2", primitives.Integer, default=1)
    assert i.name == "name2"
    assert i.sdk_default == 1
    assert i.default == primitives.Integer(1)
    assert i.required is None
    assert i.sdk_required is False
    assert i.help is None
    assert i.var.description == ""
    assert i.sdk_type == primitives.Integer

    with pytest.raises(_user_exceptions.FlyteAssertion):
        promise.Input("abc", primitives.Integer, required=True, default=1)


def test_create_native_named_tuple():
    ctx = FlyteContextManager.current_context()
    t = create_native_named_tuple(ctx, promises=None, entity_interface=Interface())
    assert t is None

    p1 = Promise(var="x", val=TypeEngine.to_literal(ctx, 1, int, LiteralType(simple=SimpleType.INTEGER)))
    p2 = Promise(var="y", val=TypeEngine.to_literal(ctx, 2, int, LiteralType(simple=SimpleType.INTEGER)))

    t = create_native_named_tuple(ctx, promises=p1, entity_interface=Interface(outputs={"x": int}))
    assert t
    assert t == 1

    t = create_native_named_tuple(ctx, promises=[], entity_interface=Interface())
    assert t is None

    t = create_native_named_tuple(ctx, promises=[p1, p2], entity_interface=Interface(outputs={"x": int, "y": int}))
    assert t
    assert t == (1, 2)

    t = create_native_named_tuple(
        ctx, promises=[p1, p2], entity_interface=Interface(outputs={"x": int, "y": int}, output_tuple_name="Tup")
    )
    assert t
    assert t == (1, 2)
    assert t.__class__.__name__ == "Tup"

    with pytest.raises(KeyError):
        create_native_named_tuple(
            ctx, promises=[p1, p2], entity_interface=Interface(outputs={"x": int}, output_tuple_name="Tup")
        )
