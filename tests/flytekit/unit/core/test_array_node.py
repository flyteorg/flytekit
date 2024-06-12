from collections import OrderedDict

import pytest

from flytekit import LaunchPlan, current_context, task, workflow
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.array_node import map_task
from flytekit.models.admin.workflow import WorkflowSpec
from flytekit.models.task import TaskSpec
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


def test_task_serialization(serialization_settings):
    @task(retries=2)
    def t1(a: int) -> int:
        return a + 1

    @task
    def t2(a: int) -> int:
        return a + 1

    # array_node = ArrayNode(t1)
    array_node = map_task(t1, a=[1, 2, 3])
    node_spec = get_serializable(OrderedDict(), serialization_settings, array_node)
    assert node_spec.node is not None
    assert isinstance(node_spec.node, TaskSpec)
    assert node_spec.node.template.metadata.retries.retries == 2


def test_lp_serialization(serialization_settings):
    @task
    def square(val: int) -> int:
        return val * val

    @workflow
    def parent_wf(a: int) -> int:
        return square(val=a)

    lp = LaunchPlan.get_default_launch_plan(current_context(), parent_wf)

    array_node = map_task(lp, a=[1, 2, 3])
    node_spec = get_serializable(OrderedDict(), serialization_settings, array_node)
    assert node_spec.node is not None
    assert isinstance(node_spec.node, WorkflowSpec)
