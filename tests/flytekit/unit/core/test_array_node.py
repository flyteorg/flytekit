from collections import OrderedDict

import pytest

from flytekit import LaunchPlan, current_context, task, workflow
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.array_node import mapped_entity
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


def test_lp_serialization(serialization_settings):

    @task
    def multiply(val: int, val1: int) -> int:
        return val * val1

    @workflow
    def parent_wf(a: int, b: int) -> int:
        return multiply(val=a, val1=b)

    lp = LaunchPlan.get_default_launch_plan(current_context(), parent_wf)

    @workflow
    def grandparent_wf():
        return mapped_entity(lp, concurrency=10, min_success_ratio=0.9)(a=[1, 3, 5], b=[2, 4, 6])

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
