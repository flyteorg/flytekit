import typing
from collections import OrderedDict

import pytest

from flytekit import LaunchPlan, current_context, task, workflow
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.array_node import array_node
from flytekit.models.core import identifier as identifier_models
from flytekit.tools.translator import get_serializable


@pytest.fixture
def serialization_settings():
    default_img = Image(name="default", fqn="test", tag="tag")
    return SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )


@task
def multiply(val: int, val1: int) -> int:
    return val * val1


@workflow
def parent_wf(a: int, b: int) -> int:
    return multiply(val=a, val1=b)


lp = LaunchPlan.get_default_launch_plan(current_context(), parent_wf)


@workflow
def grandparent_wf() -> list[int]:
    return array_node(lp, concurrency=10, min_success_ratio=0.9)(a=[1, 3, 5], b=[2, 4, 6])


def test_lp_serialization(serialization_settings):

    wf_spec = get_serializable(OrderedDict(), serialization_settings, grandparent_wf)
    assert len(wf_spec.template.nodes) == 1
    assert wf_spec.template.nodes[0].array_node is not None
    assert wf_spec.template.nodes[0].array_node.node is not None
    assert wf_spec.template.nodes[0].array_node.node.workflow_node is not None
    assert (
        wf_spec.template.nodes[0].array_node.node.workflow_node.launchplan_ref.resource_type
        == identifier_models.ResourceType.LAUNCH_PLAN
    )
    assert wf_spec.template.nodes[0].array_node.node.workflow_node.launchplan_ref.name == "tests.flytekit.unit.core.test_array_node.parent_wf"
    assert wf_spec.template.nodes[0].array_node._min_success_ratio == 0.9
    assert wf_spec.template.nodes[0].array_node._parallelism == 10


def test_local_exec_lp():

    a = grandparent_wf()
    assert a == [2, 12, 30]


def test_local_exec_lp_min_successes():
    @workflow
    def grandparent_wf_1() -> list[int]:
        return array_node(lp, concurrency=10, min_successes=2)(a=[1, 3, 5], b=[2, 4, 6])

    a = grandparent_wf_1()
    assert a == [2, 12, 30]


@pytest.mark.parametrize(
    "min_success_ratio, should_raise_error",
    [
        (None, True),
        (1, True),
        (0.75, False),
        (0.5, False),
    ],
)
def test_local_exec_lp_min_success_ratio(min_success_ratio, should_raise_error):
    @task
    def ex_task(val: int) -> int:
        if val == 1:
            raise Exception("Test")
        return val

    @workflow
    def ex_wf(val: int) -> int:
        return ex_task(val=val)

    ex_lp = LaunchPlan.get_default_launch_plan(current_context(), ex_wf)

    @workflow
    def grandparent_ex_wf() -> list[typing.Optional[int]]:
        return array_node(ex_lp, min_success_ratio=min_success_ratio)(val=[1, 2, 3, 4])

    if should_raise_error:
        with pytest.raises(Exception):
            grandparent_ex_wf()
    else:
        a = grandparent_ex_wf()
        assert grandparent_ex_wf() == [None, 2, 3, 4]
