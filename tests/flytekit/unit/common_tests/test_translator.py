import typing
from collections import OrderedDict

from flytekit import ContainerTask
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.base_task import kwtypes
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.launch_plan import LaunchPlan, ReferenceLaunchPlan
from flytekit.core.task import ReferenceTask, task
from flytekit.core.workflow import ReferenceWorkflow, workflow

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = context_manager.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


def test_references():
    rlp = ReferenceLaunchPlan("media", "stg", "some.name", "cafe", inputs=kwtypes(in1=str), outputs=kwtypes())
    sdk_lp = get_serializable(OrderedDict(), serialization_settings, rlp)
    assert sdk_lp.has_registered

    rt = ReferenceTask("media", "stg", "some.name", "cafe", inputs=kwtypes(in1=str), outputs=kwtypes())
    sdk_task = get_serializable(OrderedDict(), serialization_settings, rt)
    assert sdk_task.has_registered

    rw = ReferenceWorkflow("media", "stg", "some.name", "cafe", inputs=kwtypes(in1=str), outputs=kwtypes())
    sdk_wf = get_serializable(OrderedDict(), serialization_settings, rw)
    assert sdk_wf.has_registered


def test_basics():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = t2(a=y, b=b)
        return x, d

    sdk_wf = get_serializable(OrderedDict(), serialization_settings, my_wf, False)
    assert len(sdk_wf.interface.inputs) == 2
    assert len(sdk_wf.interface.outputs) == 2
    assert len(sdk_wf.nodes) == 2

    # Gets cached the first time around so it's not actually fast.
    sdk_task = get_serializable(OrderedDict(), serialization_settings, t1, True)
    assert "pyflyte-execute" in sdk_task.container.args

    lp = LaunchPlan.create(
        "testlp",
        my_wf,
    )
    sdk_lp = get_serializable(OrderedDict(), serialization_settings, lp)
    assert sdk_lp.id.name == "testlp"


def test_fast():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    sdk_task = get_serializable(OrderedDict(), serialization_settings, t1, True)
    assert "pyflyte-fast-execute" in sdk_task.container.args


def test_container():
    @task
    def t1(a: int) -> (int, str):
        return a + 2, str(a) + "-HELLO"

    t2 = ContainerTask(
        "raw",
        image="alpine",
        inputs=kwtypes(a=int, b=str),
        input_data_dir="/tmp",
        output_data_dir="/tmp",
        command=["cat"],
        arguments=["/tmp/a"],
    )

    sdk_task = get_serializable(OrderedDict(), serialization_settings, t2, fast=True)
    assert "pyflyte" not in sdk_task.container.args
