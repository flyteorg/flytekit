from __future__ import absolute_import

import datetime as _datetime
import os as _os
import sys as _sys

from flytekit.bin import entrypoint as _entrypoint
from flytekit.common import constants as _common_constants
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.tasks import spark_task as _spark_task
from flytekit.models import types as _type_models
from flytekit.models.core import identifier as _identifier
from flytekit.sdk.tasks import inputs, outputs, spark_task
from flytekit.sdk.types import Types


@inputs(in1=Types.Integer)
@outputs(out1=Types.String)
@spark_task(spark_conf={"A": "B"}, hadoop_conf={"C": "D"})
def default_task(wf_params, sc, in1, out1):
    pass


default_task._id = _identifier.Identifier(
    _identifier.ResourceType.TASK, "project", "domain", "name", "version"
)


def test_default_python_task():
    assert isinstance(default_task, _spark_task.SdkSparkTask)
    assert isinstance(default_task, _sdk_runnable.SdkRunnableTask)
    assert default_task.interface.inputs["in1"].description == ""
    assert default_task.interface.inputs["in1"].type == _type_models.LiteralType(
        simple=_type_models.SimpleType.INTEGER
    )
    assert default_task.interface.outputs["out1"].description == ""
    assert default_task.interface.outputs["out1"].type == _type_models.LiteralType(
        simple=_type_models.SimpleType.STRING
    )
    assert default_task.type == _common_constants.SdkTaskType.SPARK_TASK
    assert default_task.task_function_name == "default_task"
    assert default_task.task_module == __name__
    assert default_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert default_task.metadata.deprecated_error_message == ""
    assert default_task.metadata.discoverable is False
    assert default_task.metadata.discovery_version == ""
    assert default_task.metadata.retries.retries == 0
    assert len(default_task.container.resources.limits) == 0
    assert len(default_task.container.resources.requests) == 0
    assert default_task.custom["sparkConf"]["A"] == "B"
    assert default_task.custom["hadoopConf"]["C"] == "D"
    assert (
        _os.path.abspath(_entrypoint.__file__)[:-1]
        in default_task.custom["mainApplicationFile"]
    )
    assert default_task.custom["executorPath"] == _sys.executable

    pb2 = default_task.to_flyte_idl()
    assert pb2.custom["sparkConf"]["A"] == "B"
    assert pb2.custom["hadoopConf"]["C"] == "D"
