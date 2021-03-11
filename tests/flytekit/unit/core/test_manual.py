from collections import OrderedDict

from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.node_creation import create_node
from flytekit.core.task import task
from flytekit.core.workflow import workflow, Workflow


def test_fdsafdsfds():
    @task
    def t1(a: str) -> str:
        return a + " world"

    @workflow
    def my_wf(a: str) -> str:
        t1_node = create_node(t1, a=a)
        return t1_node.o0

    r = my_wf(a="hello")
    assert r == "hello world"

    serialization_settings = context_manager.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    sdk_wf = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert len(sdk_wf.nodes) == 1
    assert len(sdk_wf.outputs) == 1

    @task
    def t2():
        ...

    @task
    def t3():
        ...

    @workflow
    def empty_wf():
        t2_node = create_node(t2)
        t3_node = create_node(t3)
        t3_node.runs_before(t2_node)

    # Test that VoidPromises can handle runs_before
    empty_wf()


    wb = WorkflowBuilder.new().add_task(t1)
    wb = wb.add_task(t2)
    wb = wb.add_input("in1", int)
    wb = wb.add_input("in2", float)
    wb = wb.add_task(t3, t3a="in1", t3b="in2")
    wb = wb.