import pytest

from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.task import task
from flytekit.core.workflow import workflow

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = context_manager.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


def test_wf_resolving():
    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        @task
        def t1(a: int) -> (int, str):
            return a + 2, "world"

        @task
        def t2(a: str, b: str) -> str:
            return b + a

        x, y = t1(a=a)
        d = t2(a=y, b=b)
        return x, d

    x = my_wf(a=3, b="hello")
    assert x == (5, "helloworld")

    # Because the workflow is nested inside a test, calling location will fail as it tries to find the LHS that the
    # workflow was assigned to
    with pytest.raises(Exception):
        _ = my_wf.location

    # Pretend my_wf was not actually nested, but somehow assigned to example_var_name at the module layer
    my_wf._instantiated_in = "example.module"
    my_wf._lhs = "example_var_name"

    workflows_tasks = my_wf.get_all_tasks()
    assert len(workflows_tasks) == 2  # Two tasks were declared inside

    # The tasks should get the location the workflow was assigned to as the resolver.
    # The args are the index.
    srz_t0 = get_serializable(serialization_settings, workflows_tasks[0])
    assert srz_t0.container.args[-5:] == [
        "--resolver",
        "example.module.example_var_name",
        "--resolver-args",
        "--",
        "0",
    ]

    srz_t1 = get_serializable(serialization_settings, workflows_tasks[1])
    assert srz_t1.container.args[-5:] == [
        "--resolver",
        "example.module.example_var_name",
        "--resolver-args",
        "--",
        "1",
    ]
