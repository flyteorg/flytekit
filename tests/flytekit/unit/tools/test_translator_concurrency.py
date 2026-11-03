from collections import OrderedDict
from flytekit.core import launch_plan
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.models.concurrency import ConcurrencyPolicy, ConcurrencyLimitBehavior
from flytekit.configuration import Image, ImageConfig
import flytekit.configuration
from flytekit.tools.translator import get_serializable


default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


def test_translator_with_concurrency_policy():
    @task
    def t1(a: int) -> int:
        return a + 2

    @workflow
    def my_wf(a: int) -> int:
        return t1(a=a)

    # Create a launch plan with concurrency policy
    concurrency_policy = ConcurrencyPolicy(max_concurrency=5, behavior=ConcurrencyLimitBehavior.SKIP)
    lp = launch_plan.LaunchPlan.get_or_create(
        workflow=my_wf,
        name="translator_concurrency_test",
        default_inputs={"a": 10},
        concurrency=concurrency_policy
    )

    entity_mapping = OrderedDict()
    serialized_lp = get_serializable(entity_mapping, serialization_settings, lp)

    assert serialized_lp.spec.concurrency_policy is not None
    assert serialized_lp.spec.concurrency_policy.max_concurrency == 5
    assert serialized_lp.spec.concurrency_policy.behavior == ConcurrencyLimitBehavior.SKIP

    # Create a launch plan without concurrency policy
    lp_no_concurrency = launch_plan.LaunchPlan.get_or_create(
        workflow=my_wf,
        name="translator_no_concurrency_test",
        default_inputs={"a": 10}
    )
    entity_mapping_2 = OrderedDict()
    serialized_lp_2 = get_serializable(entity_mapping_2, serialization_settings, lp_no_concurrency)

    # Verify concurrency policy is not set
    assert serialized_lp_2.spec.concurrency_policy is None
