import typing
from collections import OrderedDict

import pytest

from flytekit import LaunchPlan, task, workflow
from flytekit.core.context_manager import FlyteContextManager
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.array_node import array_node
from flytekit.core.array_node_map_task import map_task
from flytekit.models.core import identifier as identifier_models
from flytekit.remote import FlyteLaunchPlan
from flytekit.remote.interface import TypedInterface
from flytekit.tools.translator import gather_dependent_entities, get_serializable


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
def multiply(val: int, val1: typing.Union[int, str], val2: int) -> int:
    if type(val1) is str:
        return val * val2
    return val * int(val1) * val2


@workflow
def parent_wf(a: int, b: typing.Union[int, str], c: int = 2) -> int:
    return multiply(val=a, val1=b, val2=c)


ctx = FlyteContextManager.current_context()
lp = LaunchPlan.get_default_launch_plan(ctx, parent_wf)


def get_grandparent_wf(serialization_settings):
    @workflow
    def grandparent_wf() -> typing.List[int]:
        return array_node(lp, concurrency=10, min_success_ratio=0.9)(a=[1, 3, 5], b=["two", 4, "six"], c=[7, 8, 9])

    return grandparent_wf


def get_grandparent_wf_with_overrides(serialization_settings):
    @workflow
    def grandparent_wf_with_overrides() -> typing.List[int]:
        return array_node(
            lp, concurrency=10, min_success_ratio=0.9
        )(a=[1, 3, 5], b=["two", 4, "six"], c=[7, 8, 9]).with_overrides(cache=True, cache_version="1.0")

    return grandparent_wf_with_overrides


def get_grandparent_remote_wf(serialization_settings):
    serialized = OrderedDict()
    lp_model = get_serializable(serialized, serialization_settings, lp)

    task_templates, wf_specs, lp_specs = gather_dependent_entities(serialized)
    for wf_id, spec in wf_specs.items():
        break

    remote_lp = FlyteLaunchPlan.promote_from_model(lp_model.id, lp_model.spec)
    # To pretend that we've fetched this launch plan from Admin, also fill in the Flyte interface, which isn't
    # part of the IDL object but is something FlyteRemote does
    remote_lp._interface = TypedInterface.promote_from_model(spec.template.interface)

    @workflow
    def grandparent_remote_wf() -> typing.List[int]:
        return array_node(
            remote_lp, concurrency=10, min_success_ratio=0.9
        )(a=[1, 3, 5], b=["two", 4, "six"], c=[7, 8, 9])

    return grandparent_remote_wf


@pytest.mark.parametrize(
    ("target", "overrides_metadata"),
    [
        (get_grandparent_wf, False),
        (get_grandparent_remote_wf, False),
        (get_grandparent_wf_with_overrides, True),
    ],
)
def test_lp_serialization(target, overrides_metadata, serialization_settings):
    wf_spec = get_serializable(OrderedDict(), serialization_settings, target(serialization_settings))
    assert len(wf_spec.template.nodes) == 1

    parent_node = wf_spec.template.nodes[0]
    assert parent_node.inputs[0].var == "a"
    assert len(parent_node.inputs[0].binding.collection.bindings) == 3
    for binding in parent_node.inputs[0].binding.collection.bindings:
        assert binding.scalar.primitive.integer is not None
    assert parent_node.inputs[1].var == "b"
    for binding in parent_node.inputs[1].binding.collection.bindings:
        assert (binding.scalar.union is not None or
                binding.scalar.primitive.integer is not None or
                binding.scalar.primitive.string_value is not None)
    assert len(parent_node.inputs[1].binding.collection.bindings) == 3
    assert parent_node.inputs[2].var == "c"
    assert len(parent_node.inputs[2].binding.collection.bindings) == 3
    for binding in parent_node.inputs[2].binding.collection.bindings:
        assert binding.scalar.primitive.integer is not None

    serialized_array_node = parent_node.array_node
    assert (
            serialized_array_node.node.workflow_node.launchplan_ref.resource_type
            == identifier_models.ResourceType.LAUNCH_PLAN
    )
    assert (
            serialized_array_node.node.workflow_node.launchplan_ref.name
            == "tests.flytekit.unit.core.test_array_node.parent_wf"
    )
    assert serialized_array_node._min_success_ratio == 0.9
    assert serialized_array_node._parallelism == 10

    subnode = serialized_array_node.node
    assert subnode.inputs == parent_node.inputs

    if overrides_metadata:
        assert parent_node.metadata.cacheable
        assert parent_node.metadata.cache_version == "1.0"
        assert subnode.metadata.cacheable
        assert subnode.metadata.cache_version == "1.0"
    else:
        assert not parent_node.metadata.cacheable
        assert not parent_node.metadata.cache_version
        assert not subnode.metadata.cacheable
        assert not subnode.metadata.cache_version


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

    ctx = FlyteContextManager.current_context()
    ex_lp = LaunchPlan.get_default_launch_plan(ctx, ex_wf)

    @workflow
    def grandparent_ex_wf() -> typing.List[typing.Optional[int]]:
        return array_node(ex_lp, min_successes=min_successes, min_success_ratio=min_success_ratio)(val=[1, 2, 3, 4])

    if should_raise_error:
        with pytest.raises(Exception):
            grandparent_ex_wf()
    else:
        assert grandparent_ex_wf() == [None, 2, 3, 4]


def test_map_task_wrapper():
    mapped_task = map_task(multiply)(val=[1, 3, 5], val1=[2, 4, 6], val2=[7, 8, 9])
    assert mapped_task == [14, 96, 270]

    mapped_lp = map_task(lp)(a=[1, 3, 5], b=[2, 4, 6], c=[7, 8, 9])
    assert mapped_lp == [14, 96, 270]
