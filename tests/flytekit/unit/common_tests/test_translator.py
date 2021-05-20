import typing
from collections import OrderedDict

from flytekit import ContainerTask, Resources
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.base_task import kwtypes
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.launch_plan import LaunchPlan, ReferenceLaunchPlan
from flytekit.core.task import ReferenceTask, task
from flytekit.core.workflow import ReferenceWorkflow, workflow
from flytekit.models.core import identifier as identifier_models

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
    lp_model = get_serializable(OrderedDict(), serialization_settings, rlp)
    assert lp_model is None

    rt = ReferenceTask("media", "stg", "some.name", "cafe", inputs=kwtypes(in1=str), outputs=kwtypes())
    task_spec = get_serializable(OrderedDict(), serialization_settings, rt)
    assert task_spec is None

    rw = ReferenceWorkflow("media", "stg", "some.name", "cafe", inputs=kwtypes(in1=str), outputs=kwtypes())
    wf_spec = get_serializable(OrderedDict(), serialization_settings, rw)
    assert wf_spec is None


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

    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf, False)
    assert len(wf_spec.template.interface.inputs) == 2
    assert len(wf_spec.template.interface.outputs) == 2
    assert len(wf_spec.template.nodes) == 2
    assert wf_spec.template.id.resource_type == identifier_models.ResourceType.WORKFLOW

    # Gets cached the first time around so it's not actually fast.
    task_spec = get_serializable(OrderedDict(), serialization_settings, t1, True)
    assert "pyflyte-execute" in task_spec.template.container.args

    lp = LaunchPlan.create(
        "testlp",
        my_wf,
    )
    lp_model = get_serializable(OrderedDict(), serialization_settings, lp)
    assert lp_model.id.name == "testlp"


def test_fast():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1, True)
    assert "pyflyte-fast-execute" in task_spec.template.container.args


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
        requests=Resources(mem="400Mi", cpu="1"),
    )

    task_spec = get_serializable(OrderedDict(), serialization_settings, t2, fast=True)
    assert "pyflyte" not in task_spec.template.container.args
