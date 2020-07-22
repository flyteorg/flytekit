from __future__ import absolute_import
from flytekit.sdk.tasks import tensorflow_task, inputs, outputs
from flytekit.sdk.types import Types
from flytekit.common import constants as _common_constants
from flytekit.common.tasks import sdk_runnable as _sdk_runnable, tensorflow_task as _tensorflow_task
from flytekit.models import types as _type_models
from flytekit.models.core import identifier as _identifier
import datetime as _datetime


@inputs(in1=Types.Integer)
@outputs(out1=Types.String)
@tensorflow_task(workers_count=2, ps_replicas_count=1, chief_replicas_count=1)
def simple_tensorflow_task(wf_params, sc, in1, out1):
    pass


simple_tensorflow_task._id = _identifier.Identifier(_identifier.ResourceType.TASK, "project", "domain", "name", "version")


def test_simple_tensorflow_task():
    assert isinstance(simple_tensorflow_task, _tensorflow_task.SdkTensorFlowTask)
    assert isinstance(simple_tensorflow_task, _sdk_runnable.SdkRunnableTask)
    assert simple_tensorflow_task.interface.inputs['in1'].description == ''
    assert simple_tensorflow_task.interface.inputs['in1'].type == \
           _type_models.LiteralType(simple=_type_models.SimpleType.INTEGER)
    assert simple_tensorflow_task.interface.outputs['out1'].description == ''
    assert simple_tensorflow_task.interface.outputs['out1'].type == \
           _type_models.LiteralType(simple=_type_models.SimpleType.STRING)
    assert simple_tensorflow_task.type == _common_constants.SdkTaskType.TENSORFLOW_TASK
    assert simple_tensorflow_task.task_function_name == 'simple_tensorflow_task'
    assert simple_tensorflow_task.task_module == __name__
    assert simple_tensorflow_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert simple_tensorflow_task.metadata.deprecated_error_message == ''
    assert simple_tensorflow_task.metadata.discoverable is False
    assert simple_tensorflow_task.metadata.discovery_version == ''
    assert simple_tensorflow_task.metadata.retries.retries == 0
    assert len(simple_tensorflow_task.container.resources.limits) == 0
    assert len(simple_tensorflow_task.container.resources.requests) == 0
    assert simple_tensorflow_task.custom['workers'] == 2
    assert simple_tensorflow_task.custom['ps_replicas'] == 1
    assert simple_tensorflow_task.custom['chief_replicas'] == 1
    
    

    # Should strip out the venv component of the args.
    assert simple_tensorflow_task._get_container_definition().args[0] == 'pyflyte-execute'

    pb2 = simple_tensorflow_task.to_flyte_idl()
    assert pb2.custom['workers'] == 2
    assert pb2.custom['ps_replicas'] == 1
    assert pb2.custom['chief_replicas'] == 1