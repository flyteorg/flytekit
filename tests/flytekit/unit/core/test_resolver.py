from collections import OrderedDict

import pytest

from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.python_auto_container import TaskResolverMixin
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
    srz_t0 = get_serializable(OrderedDict(), serialization_settings, workflows_tasks[0])
    assert srz_t0.container.args[-4:] == [
        "--resolver",
        "example.module.example_var_name",
        "--",
        "0",
    ]

    srz_t1 = get_serializable(OrderedDict(), serialization_settings, workflows_tasks[1])
    assert srz_t1.container.args[-4:] == [
        "--resolver",
        "example.module.example_var_name",
        "--",
        "1",
    ]


def test_class_resolver():
    c = ClassStorageTaskResolver()
    assert c.name() != ""

    with pytest.raises(RuntimeError):
        c.load_task([])

    @task
    def t1(a: str, b: str) -> str:
        return b + a

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    c.add(t2)
    assert c.loader_args(None, t2) == ["0"]

    with pytest.raises(Exception):
        c.loader_args(t1)


def test_mixin():
    """
    This test is only to make codecov happy. Actual logic is already tested above.
    """
    x = TaskResolverMixin()
    x.location
    x.name()
    x.loader_args(None, None)
    x.get_all_tasks()
    x.load_task([])
