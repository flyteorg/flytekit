from __future__ import absolute_import
from flytekit.sdk.tasks import python_task, inputs, outputs
from flytekit.sdk.types import Types
from flytekit.common import constants as _common_constants
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.models import types as _type_models
from flytekit.models.core import identifier as _identifier
import datetime as _datetime


@inputs(in1=Types.Integer)
@outputs(out1=Types.String)
@python_task
def default_task(wf_params, in1, out1):
    pass


default_task._id = _identifier.Identifier(_identifier.ResourceType.TASK, "project", "domain", "name", "version")


def test_default_python_task():
    assert isinstance(default_task, _sdk_runnable.SdkRunnableTask)
    assert default_task.interface.inputs['in1'].description == ''
    assert default_task.interface.inputs['in1'].type == \
        _type_models.LiteralType(simple=_type_models.SimpleType.INTEGER)
    assert default_task.interface.outputs['out1'].description == ''
    assert default_task.interface.outputs['out1'].type == \
        _type_models.LiteralType(simple=_type_models.SimpleType.STRING)
    assert default_task.type == _common_constants.SdkTaskType.PYTHON_TASK
    assert default_task.task_function_name == 'default_task'
    assert default_task.task_module == __name__
    assert default_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert default_task.metadata.deprecated_error_message == ''
    assert default_task.metadata.discoverable is False
    assert default_task.metadata.discovery_version == ''
    assert default_task.metadata.retries.retries == 0
