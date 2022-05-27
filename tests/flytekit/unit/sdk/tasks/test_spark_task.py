import datetime as _datetime
import os as _os
import sys as _sys

import flytekit.models.core.types
from flytekit.bin import entrypoint as _entrypoint
from flytekit.common import constants as _common_constants
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.tasks import spark_task as _spark_task
from flytekit.models.core import identifier as _identifier
from flytekit.sdk.tasks import inputs, outputs, spark_task
from flytekit.sdk.types import Types


@inputs(in1=Types.Integer)
@outputs(out1=Types.String)
@spark_task(spark_conf={"A": "B"}, hadoop_conf={"C": "D"})
def default_task(wf_params, sc, in1, out1):
    out1.set("hello")


default_task._id = _identifier.Identifier(_identifier.ResourceType.TASK, "project", "domain", "name", "version")


def test_default_python_task():
    assert isinstance(default_task, _spark_task.SdkSparkTask)
    assert isinstance(default_task, _sdk_runnable.SdkRunnableTask)
    assert default_task.interface.inputs["in1"].description == ""
    assert default_task.interface.inputs["in1"].type == flytekit.models.core.types.LiteralType(
        simple=flytekit.models.core.types.SimpleType.INTEGER
    )
    assert default_task.interface.outputs["out1"].description == ""
    assert default_task.interface.outputs["out1"].type == flytekit.models.core.types.LiteralType(
        simple=flytekit.models.core.types.SimpleType.STRING
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
    assert default_task.hadoop_conf["C"] == "D"
    assert default_task.spark_conf["A"] == "B"
    assert _os.path.abspath(_entrypoint.__file__)[:-1] in default_task.custom["mainApplicationFile"]
    assert default_task.custom["executorPath"] == _sys.executable

    pb2 = default_task.to_flyte_idl()
    assert pb2.custom["sparkConf"]["A"] == "B"
    assert pb2.custom["hadoopConf"]["C"] == "D"


def test_overrides_spark_task():
    assert default_task.id.name == "name"
    new_task = default_task.with_overrides(new_spark_conf={"x": "1"}, new_hadoop_conf={"y": "2"})
    assert isinstance(new_task, _spark_task.SdkSparkTask)
    assert new_task.id.name.startswith("name-")
    assert new_task.custom["sparkConf"]["x"] == "1"
    assert new_task.custom["hadoopConf"]["y"] == "2"

    assert default_task.custom["sparkConf"]["A"] == "B"
    assert default_task.custom["hadoopConf"]["C"] == "D"

    assert default_task.has_valid_name is False
    default_task.assign_name("my-task")
    assert default_task.has_valid_name
    assert new_task.interface == default_task.interface

    assert default_task.__hash__() != new_task.__hash__()

    new_task2 = default_task.with_overrides(new_spark_conf={"x": "1"}, new_hadoop_conf={"y": "2"})
    assert new_task2.id.name == new_task.id.name

    t = new_task(in1=1)
    assert t.outputs["out1"] is not None
