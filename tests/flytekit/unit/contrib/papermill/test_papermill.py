from __future__ import absolute_import
from flytekit.common import constants
from flytekit.contrib.papermill import notebook
from flytekit.common.types import helpers
from flytekit.models import interface
from flytekit.sdk import types
import six


GOOD_INPUTS = {
    'a': types.Types.Integer,
    'name': types.Types.String,
}
GOOD_OUTPUTS = {
    'x': types.Types.Integer,
}
GOOD_NOTEBOOK = notebook.PapermillNotebookTask(
    notebook_path="notebooks/good.ipynb",
    inputs={
        k: interface.Variable(
            helpers.python_std_to_sdk_type(v).to_flyte_literal_type(),
            ''
        )
        for k, v in six.iteritems(GOOD_INPUTS)
    },
    outputs={
        k: interface.Variable(
            helpers.python_std_to_sdk_type(v).to_flyte_literal_type(),
            ''
        )
        for k, v in six.iteritems(GOOD_OUTPUTS)
    },
    task_type=constants.SdkTaskType.PYTHON_TASK,
)


def test_good_notebook():
    outputs = GOOD_NOTEBOOK.unit_test(a=50, name="Person")
    assert outputs['x'] == 151
    assert len(outputs) == 2
