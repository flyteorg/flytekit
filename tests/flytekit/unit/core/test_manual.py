import typing
from collections import OrderedDict

from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.task import task
from flytekit.core.workflow import WorkflowTwo


def test_wf2():
    @task
    def t1(a: str) -> str:
        return a + " world"

    @task
    def t2():
        print("side effect")

    wb = WorkflowTwo(name="my.workflow")
    wb.add_workflow_input("in1", str)
    node = wb.add_entity(t1, a=wb.inputs["in1"])
    wb.add_entity(t2)
    wb.add_workflow_output("from_n0t1", str, node.outputs["o0"])

    assert wb(in1="hello") == "hello world"

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    srz_wf = get_serializable(OrderedDict(), serialization_settings, wb)
    print(srz_wf)


def test_wf2_list_bound():
    @task
    def t1(a: typing.List[int]) -> int:
        return sum(a)

    wb = WorkflowTwo(name="my.workflow.a")
    wb.add_workflow_input("in1", int)
    wb.add_workflow_input("in2", int)
    node = wb.add_entity(t1, a=[wb.inputs["in1"], wb.inputs["in2"]])
    wb.add_workflow_output("from_n0t1", int, node.outputs["o0"])

    assert wb(in1=3, in2=4) == 7


def test_wf2_map_bound():
    @task
    def t1(a: typing.Dict[str, typing.List[int]]) -> typing.Dict[str, int]:
        return {k: sum(v) for k, v in a.items()}

    wb = WorkflowTwo(name="my.workflow.a")
    wb.add_workflow_input("in1", int)
    wb.add_workflow_input("in2", int)
    wb.add_workflow_input("in3", int)
    node = wb.add_entity(t1, a={"a": [wb.inputs["in1"], wb.inputs["in2"]], "b": [wb.inputs["in3"], wb.inputs["in3"]]})
    wb.add_workflow_output("from_n0t1", typing.Dict[str, int], node.outputs["o0"])

    assert wb(in1=3, in2=4, in3=5) == {"a": 7, "b": 10}


def test_wf2_with_list_io():
    @task
    def t1(a: int) -> typing.List[int]:
        return [1, a, 3]

    @task
    def t2(a: typing.List[int]) -> int:
        return sum(a)

    wb = WorkflowTwo(name="my.workflow.a")
    t1_node = wb.add_entity(t1, a=2)
    t2_node = wb.add_entity(t2, a=t1_node.outputs["o0"])
    wb.add_workflow_output("from_n0t2", int, t2_node.outputs["o0"])

    assert wb() == 6


# Get patch to patch
