import datetime as _datetime

import flytekit.legacy.runnables
from flytekit.common import constants as _common_constants
from flytekit.legacy.tasks import pytorch_task as _pytorch_task
from flytekit.models import types as _type_models
from flytekit.models.core import identifier as _identifier
from flytekit.legacy.sdk.tasks import inputs, outputs, pytorch_task
from flytekit.legacy.sdk import Types


@inputs(in1=Types.Integer)
@outputs(out1=Types.String)
@pytorch_task(workers_count=1)
def simple_pytorch_task(wf_params, sc, in1, out1):
    pass


simple_pytorch_task._id = _identifier.Identifier(_identifier.ResourceType.TASK, "project", "domain", "name", "version")


def test_simple_pytorch_task():
    assert isinstance(simple_pytorch_task, _pytorch_task.SdkPyTorchTask)
    assert isinstance(simple_pytorch_task, flytekit.legacy.runnables.SdkRunnableTask)
    assert simple_pytorch_task.interface.inputs["in1"].description == ""
    assert simple_pytorch_task.interface.inputs["in1"].type == _type_models.LiteralType(
        simple=_type_models.SimpleType.INTEGER
    )
    assert simple_pytorch_task.interface.outputs["out1"].description == ""
    assert simple_pytorch_task.interface.outputs["out1"].type == _type_models.LiteralType(
        simple=_type_models.SimpleType.STRING
    )
    assert simple_pytorch_task.type == _common_constants.SdkTaskType.PYTORCH_TASK
    assert simple_pytorch_task.task_function_name == "simple_pytorch_task"
    assert simple_pytorch_task.task_module == __name__
    assert simple_pytorch_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert simple_pytorch_task.metadata.deprecated_error_message == ""
    assert simple_pytorch_task.metadata.discoverable is False
    assert simple_pytorch_task.metadata.discovery_version == ""
    assert simple_pytorch_task.metadata.retries.retries == 0
    assert len(simple_pytorch_task.container.resources.limits) == 0
    assert len(simple_pytorch_task.container.resources.requests) == 0
    assert simple_pytorch_task.custom["workers"] == 1
    # Should strip out the venv component of the args.
    assert simple_pytorch_task._get_container_definition().args[0] == "pyflyte-execute"

    pb2 = simple_pytorch_task.to_flyte_idl()
    assert pb2.custom["workers"] == 1
