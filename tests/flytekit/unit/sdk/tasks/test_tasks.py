import datetime as _datetime
import os as _os

from flytekit import configuration as _configuration
from flytekit.common import constants as _common_constants
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.models import task as _task_models
from flytekit.models import types as _type_models
from flytekit.models.core import identifier as _identifier
from flytekit.sdk.tasks import inputs, outputs, python_task
from flytekit.sdk.types import Types


@inputs(in1=Types.Integer)
@outputs(out1=Types.String)
@python_task
def default_task(wf_params, in1, out1):
    pass


default_task._id = _identifier.Identifier(_identifier.ResourceType.TASK, "project", "domain", "name", "version")


def test_default_python_task():
    assert isinstance(default_task, _sdk_runnable.SdkRunnableTask)
    assert default_task.interface.inputs["in1"].description == ""
    assert default_task.interface.inputs["in1"].type == _type_models.LiteralType(simple=_type_models.SimpleType.INTEGER)
    assert default_task.interface.outputs["out1"].description == ""
    assert default_task.interface.outputs["out1"].type == _type_models.LiteralType(
        simple=_type_models.SimpleType.STRING
    )
    assert default_task.type == _common_constants.SdkTaskType.PYTHON_TASK
    assert default_task.task_function_name == "default_task"
    assert default_task.task_module == __name__
    assert default_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert default_task.metadata.deprecated_error_message == ""
    assert default_task.metadata.discoverable is False
    assert default_task.metadata.discovery_version == ""
    assert default_task.metadata.retries.retries == 0
    assert len(default_task.container.resources.limits) == 0
    assert len(default_task.container.resources.requests) == 0


def test_default_resources():
    with _configuration.TemporaryConfiguration(
        _os.path.join(_os.path.dirname(_os.path.realpath(__file__)), "../../configuration/configs/good.config",)
    ):

        @inputs(in1=Types.Integer)
        @outputs(out1=Types.String)
        @python_task()
        def default_task2(wf_params, in1, out1):
            pass

        request_map = {r.name: r.value for r in default_task2.container.resources.requests}

        limit_map = {l.name: l.value for l in default_task2.container.resources.limits}

        assert request_map[_task_models.Resources.ResourceName.CPU] == "500m"
        assert request_map[_task_models.Resources.ResourceName.MEMORY] == "500Gi"
        assert request_map[_task_models.Resources.ResourceName.GPU] == "1"
        assert request_map[_task_models.Resources.ResourceName.STORAGE] == "500Gi"

        assert limit_map[_task_models.Resources.ResourceName.CPU] == "501m"
        assert limit_map[_task_models.Resources.ResourceName.MEMORY] == "501Gi"
        assert limit_map[_task_models.Resources.ResourceName.GPU] == "2"
        assert limit_map[_task_models.Resources.ResourceName.STORAGE] == "501Gi"


def test_overriden_resources():
    with _configuration.TemporaryConfiguration(
        _os.path.join(_os.path.dirname(_os.path.realpath(__file__)), "../../configuration/configs/good.config",)
    ):

        @inputs(in1=Types.Integer)
        @outputs(out1=Types.String)
        @python_task(
            memory_limit="100Gi",
            memory_request="50Gi",
            cpu_limit="1000m",
            cpu_request="500m",
            gpu_limit="1",
            gpu_request="0",
            storage_request="100Gi",
            storage_limit="200Gi",
        )
        def default_task2(wf_params, in1, out1):
            pass

        request_map = {r.name: r.value for r in default_task2.container.resources.requests}

        limit_map = {l.name: l.value for l in default_task2.container.resources.limits}

        assert request_map[_task_models.Resources.ResourceName.CPU] == "500m"
        assert request_map[_task_models.Resources.ResourceName.MEMORY] == "50Gi"
        assert request_map[_task_models.Resources.ResourceName.GPU] == "0"
        assert request_map[_task_models.Resources.ResourceName.STORAGE] == "100Gi"

        assert limit_map[_task_models.Resources.ResourceName.CPU] == "1000m"
        assert limit_map[_task_models.Resources.ResourceName.MEMORY] == "100Gi"
        assert limit_map[_task_models.Resources.ResourceName.GPU] == "1"
        assert limit_map[_task_models.Resources.ResourceName.STORAGE] == "200Gi"
