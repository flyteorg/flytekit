import pytest as _pytest

from flytekit.common import constants as _common_constants
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks import sdk_runnable
from flytekit.common.types import primitives
from flytekit.models import interface


def test_basic_unit_test():
    def add_one(wf_params, value_in, value_out):
        value_out.set(value_in + 1)

    t = sdk_runnable.SdkRunnableTask(
        add_one,
        _common_constants.SdkTaskType.PYTHON_TASK,
        "1",
        1,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        False,
        None,
        {},
        None,
    )
    t.add_inputs({"value_in": interface.Variable(primitives.Integer.to_flyte_literal_type(), "")})
    t.add_outputs({"value_out": interface.Variable(primitives.Integer.to_flyte_literal_type(), "")})
    out = t.unit_test(value_in=1)
    assert out["value_out"] == 2

    with _pytest.raises(_user_exceptions.FlyteAssertion) as e:
        t()

    assert "value_in" in str(e.value)
    assert "INTEGER" in str(e.value)
