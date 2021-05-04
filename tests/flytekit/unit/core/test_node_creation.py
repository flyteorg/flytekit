import typing
from collections import OrderedDict

import pytest

from flytekit.common.exceptions.user import FlyteAssertion
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.node_creation import create_node
from flytekit.core.task import task
from flytekit.core.workflow import workflow


def test_normal_task():
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
        t3_node_duplicate = create_node(t3)
        t3_node.runs_before(t2_node)
        t2_node.runs_before(t3_node_duplicate)

    # Test that VoidPromises can handle runs_before
    empty_wf()

    @workflow
    def empty_wf2():
        t2_node = create_node(t2)
        t3_node = create_node(t3)
        t3_node_duplicate = create_node(t3)
        t3_node >> t2_node
        t2_node >> t3_node_duplicate

    serialization_settings = context_manager.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    sdk_wf = get_serializable(OrderedDict(), serialization_settings, empty_wf)
    assert sdk_wf.nodes[0].upstream_node_ids[0] == "test-node-creation-t3"
    assert sdk_wf.nodes[0].id == "test-node-creation-t2"
    assert sdk_wf.nodes[1].id == "test-node-creation-t3"
    assert sdk_wf.nodes[2].upstream_node_ids[0] == "test-node-creation-t2"
    assert sdk_wf.nodes[2].id == "test-node-creation-t3-n1"

    sdk_wf = get_serializable(OrderedDict(), serialization_settings, empty_wf2)
    assert sdk_wf.nodes[0].upstream_node_ids[0] == "test-node-creation-t3"
    assert sdk_wf.nodes[0].id == "test-node-creation-t2"
    assert sdk_wf.nodes[1].id == "test-node-creation-t3"
    assert sdk_wf.nodes[2].upstream_node_ids[0] == "test-node-creation-t2"
    assert sdk_wf.nodes[2].id == "test-node-creation-t3-n1"

    with pytest.raises(FlyteAssertion):

        @workflow
        def empty_wf2():
            create_node(t2, "foo")


def test_more_normal_task():
    nt = typing.NamedTuple("OneOutput", t1_str_output=str)

    @task
    def t1(a: int) -> nt:
        # This one returns a regular tuple
        return (f"{a + 2}",)

    @task
    def t1_nt(a: int) -> nt:
        # This one returns an instance of the named tuple.
        return nt(f"{a + 2}")

    @task
    def t2(a: typing.List[str]) -> str:
        return " ".join(a)

    @workflow
    def my_wf(a: int, b: str) -> (str, str):
        t1_node = create_node(t1, a=a).with_overrides(aliases={"t1_str_output": "foo"})
        t1_nt_node = create_node(t1_nt, a=a)
        t2_node = create_node(t2, a=[t1_node.t1_str_output, t1_nt_node.t1_str_output, b])
        return t1_node.t1_str_output, t2_node.o0

    x = my_wf(a=5, b="hello")
    assert x == ("7", "7 7 hello")


def test_reserved_keyword():
    nt = typing.NamedTuple("OneOutput", outputs=str)

    @task
    def t1(a: int) -> nt:
        # This one returns a regular tuple
        return (f"{a + 2}",)

    # Test that you can't name an output "outputs"
    with pytest.raises(FlyteAssertion):

        @workflow
        def my_wf(a: int) -> str:
            t1_node = create_node(t1, a=a)
            return t1_node.outputs
