from __future__ import absolute_import

from flytekit.common import constants as _common_constants
from flytekit.common.tasks import sdk_runnable
from flytekit.common.types import primitives
from flytekit.models import interface


def test_basic_unit_test():

    def add_one(wf_params, value_in, value_out):
        value_out.set(value_in + 1)

    t = sdk_runnable.RunnablePythonFunctionTask(
        task_function=add_one,
        task_type=_common_constants.SdkTaskType.PYTHON_TASK,
    )
    t.add_inputs({'value_in': interface.Variable(primitives.Integer.to_flyte_literal_type(), "")})
    t.add_outputs({'value_out': interface.Variable(primitives.Integer.to_flyte_literal_type(), "")})
    out = t.unit_test(value_in=1)
    assert out['value_out'] == 2