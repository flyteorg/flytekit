import typing
from collections import OrderedDict

import pytest

from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.reference_entity import ReferenceSpec
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.remote import FlyteLaunchPlan, FlyteTask
from flytekit.remote.interface import TypedInterface
from flytekit.tools.translator import gather_dependent_entities, get_serializable

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = context_manager.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


@task
def t1(a: int) -> int:
    return a + 2


@task
def t2(a: int, b: str) -> str:
    return b + str(a)


@workflow
def sub_wf(a: int, b: str) -> (int, str):
    x = t1(a=a)
    d = t2(a=x, b=b)
    return x, d


serialized = OrderedDict()
t1_spec = get_serializable(serialized, serialization_settings, t1)
ft = FlyteTask.promote_from_model(t1_spec.template)


def test_fetched_task():
    @workflow
    def wf(a: int) -> int:
        return ft(a=a)

    # Should not work unless mocked out.
    with pytest.raises(Exception, match="cannot be run locally"):
        wf(a=3)

    # Should have one reference entity
    serialized = OrderedDict()
    get_serializable(serialized, serialization_settings, wf)
    vals = [v for v in serialized.values()]
    refs = [f for f in filter(lambda x: isinstance(x, ReferenceSpec), vals)]
    assert len(refs) == 1


def test_calling_lp():
    sub_wf_lp = LaunchPlan.get_or_create(sub_wf)
    serialized = OrderedDict()
    lp_model = get_serializable(serialized, serialization_settings, sub_wf_lp)
    task_templates, wf_specs, lp_specs = gather_dependent_entities(serialized)
    for wf_id, spec in wf_specs.items():
        break

    remote_lp = FlyteLaunchPlan.promote_from_model(lp_model.id, lp_model.spec)
    # To pretend that we've fetched this launch plan from Admin, also fill in the Flyte interface, which isn't
    # part of the IDL object but is something FlyteRemote does
    remote_lp._interface = TypedInterface.promote_from_model(spec.template.interface)
    serialized = OrderedDict()

    @workflow
    def wf2(a: int) -> typing.Tuple[int, str]:
        return remote_lp(a=a, b="hello")

    wf_spec = get_serializable(serialized, serialization_settings, wf2)
    print(wf_spec.template.nodes[0].workflow_node.launchplan_ref)
    assert wf_spec.template.nodes[0].workflow_node.launchplan_ref == lp_model.id
