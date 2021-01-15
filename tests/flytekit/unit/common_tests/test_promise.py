import pytest

import flytekit.legacy.runnables
from flytekit.common import promise
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types, primitives


def test_input():
    i = flytekit.legacy.runnables.Input("name", primitives.Integer, help="blah", default=None)
    assert i.name == "name"
    assert i.sdk_default is None
    assert i.default == base_sdk_types.Void()
    assert i.sdk_required is False
    assert i.help == "blah"
    assert i.var.description == "blah"
    assert i.sdk_type == primitives.Integer

    i = flytekit.legacy.runnables.Input("name2", primitives.Integer, default=1)
    assert i.name == "name2"
    assert i.sdk_default == 1
    assert i.default == primitives.Integer(1)
    assert i.required is None
    assert i.sdk_required is False
    assert i.help is None
    assert i.var.description == ""
    assert i.sdk_type == primitives.Integer

    with pytest.raises(_user_exceptions.FlyteAssertion):
        flytekit.legacy.runnables.Input("abc", primitives.Integer, required=True, default=1)
