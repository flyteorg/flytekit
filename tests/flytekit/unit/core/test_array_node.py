import typing
from collections import OrderedDict

import pytest

from flytekit import LaunchPlan, current_context, task, workflow
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.array_node import array_node
from flytekit.core.array_node_map_task import map_task
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
def grandparent_wf() -> typing.List[int]:
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


@pytest.mark.parametrize(
    "min_successes, min_success_ratio, should_raise_error",
    [
        (None, None, True),
        (None, 1, True),
        (None, 0.75, False),
        (None, 0.5, False),
        (1, None, False),
        (3, None, False),
        (4, None, True),
        # Test min_successes takes precedence over min_success_ratio
        (1, 1.0, False),
        (4, 0.1, True),
    ],
)
def test_local_exec_lp_min_successes(min_successes, min_success_ratio, should_raise_error):
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
    def grandparent_ex_wf() -> typing.List[typing.Optional[int]]:
        return array_node(ex_lp, min_successes=min_successes, min_success_ratio=min_success_ratio)(val=[1, 2, 3, 4])

    if should_raise_error:
        with pytest.raises(Exception):
            grandparent_ex_wf()
    else:
        assert grandparent_ex_wf() == [None, 2, 3, 4]


def test_map_task_wrapper():
    mapped_task = map_task(multiply)(val=[1, 3, 5], val1=[2, 4, 6])
    assert mapped_task == [2, 12, 30]

    mapped_lp = map_task(lp)(a=[1, 3, 5], b=[2, 4, 6])
    assert mapped_lp == [2, 12, 30]
