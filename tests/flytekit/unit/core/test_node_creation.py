import datetime
import typing
from collections import OrderedDict

import pytest

import flytekit.configuration
from flytekit import Resources, map_task
from flytekit.configuration import Image, ImageConfig
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.node_creation import create_node
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.exceptions.user import FlyteAssertion
from flytekit.models import literals as _literal_models
from flytekit.models.task import Resources as _resources_models
from flytekit.tools.translator import get_serializable


def test_normal_task():
    @task
    def t1(a: str) -> str:
        return a + " world"

    @dynamic
    def my_subwf(a: int) -> typing.List[str]:
        s = []
        for i in range(a):
            s.append(t1(a=str(i)))
        return s

    @workflow
    def my_wf(a: str) -> (str, typing.List[str]):
        t1_node = create_node(t1, a=a)
        dyn_node = create_node(my_subwf, a=3)
        return t1_node.o0, dyn_node.o0

    r, x = my_wf(a="hello")
    assert r == "hello world"
    assert x == ["0 world", "1 world", "2 world"]

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert len(wf_spec.template.nodes) == 2
    assert len(wf_spec.template.outputs) == 2

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
        t3_node.runs_before(t2_node)

    # Test that VoidPromises can handle runs_before
    empty_wf()

    @workflow
    def empty_wf2():
        t2_node = create_node(t2)
        t3_node = create_node(t3)
        t3_node >> t2_node

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, empty_wf)
    assert wf_spec.template.nodes[0].upstream_node_ids[0] == "n1"
    assert wf_spec.template.nodes[0].id == "n0"

    wf_spec = get_serializable(OrderedDict(), serialization_settings, empty_wf2)
    assert wf_spec.template.nodes[0].upstream_node_ids[0] == "n1"
    assert wf_spec.template.nodes[0].id == "n0"

    with pytest.raises(FlyteAssertion):

        @workflow
        def empty_wf2():
            create_node(t2, "foo")


def test_more_normal_task():
    nt = typing.NamedTuple("OneOutput", t1_str_output=str)

    @task
    def t1(a: int) -> nt:
        # This one returns a regular tuple
        return nt(
            f"{a + 2}",
        )

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
        return nt(
            f"{a + 2}",
        )

    # Test that you can't name an output "outputs"
    with pytest.raises(FlyteAssertion):

        @workflow
        def my_wf(a: int) -> str:
            t1_node = create_node(t1, a=a)
            return t1_node.outputs


def test_runs_before():
    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @task()
    def sleep_task(a: int) -> str:
        a = a + 2
        return "world-" + str(a)

    @dynamic
    def my_subwf(a: int) -> (typing.List[str], int):
        s = []
        for i in range(a):
            s.append(sleep_task(a=i))
        return s, 5

    @workflow
    def my_wf(a: int, b: str) -> (str, typing.List[str], int):
        subwf_node = create_node(my_subwf, a=a)
        t2_node = create_node(t2, a=b, b=b)
        subwf_node.runs_before(t2_node)
        subwf_node >> t2_node
        return t2_node.o0, subwf_node.o0, subwf_node.o1

    my_wf(a=5, b="hello")


def test_resource_request_override():
    @task
    def t1(a: str) -> str:
        return f"*~*~*~{a}*~*~*~"

    @workflow
    def my_wf(a: typing.List[str]) -> typing.List[str]:
        mappy = map_task(t1)
        map_node = mappy(a=a).with_overrides(requests=Resources(cpu="1", mem="100", ephemeral_storage="500Mi"))
        return map_node

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert len(wf_spec.template.nodes) == 1
    assert wf_spec.template.nodes[0].task_node.overrides is not None
    assert wf_spec.template.nodes[0].task_node.overrides.resources.requests == [
        _resources_models.ResourceEntry(_resources_models.ResourceName.CPU, "1"),
        _resources_models.ResourceEntry(_resources_models.ResourceName.MEMORY, "100"),
        _resources_models.ResourceEntry(_resources_models.ResourceName.EPHEMERAL_STORAGE, "500Mi"),
    ]
    assert wf_spec.template.nodes[0].task_node.overrides.resources.limits == []


def test_resource_limits_override():
    @task
    def t1(a: str) -> str:
        return f"*~*~*~{a}*~*~*~"

    @workflow
    def my_wf(a: typing.List[str]) -> typing.List[str]:
        mappy = map_task(t1)
        map_node = mappy(a=a).with_overrides(limits=Resources(cpu="2", mem="200", ephemeral_storage="1Gi"))
        return map_node

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert len(wf_spec.template.nodes) == 1
    assert wf_spec.template.nodes[0].task_node.overrides.resources.requests == []
    assert wf_spec.template.nodes[0].task_node.overrides.resources.limits == [
        _resources_models.ResourceEntry(_resources_models.ResourceName.CPU, "2"),
        _resources_models.ResourceEntry(_resources_models.ResourceName.MEMORY, "200"),
        _resources_models.ResourceEntry(_resources_models.ResourceName.EPHEMERAL_STORAGE, "1Gi"),
    ]


def test_resources_override():
    @task
    def t1(a: str) -> str:
        return f"*~*~*~{a}*~*~*~"

    @workflow
    def my_wf(a: typing.List[str]) -> typing.List[str]:
        mappy = map_task(t1)
        map_node = mappy(a=a).with_overrides(
            requests=Resources(cpu="1", mem="100", ephemeral_storage="500Mi"),
            limits=Resources(cpu="2", mem="200", ephemeral_storage="1Gi"),
        )
        return map_node

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert len(wf_spec.template.nodes) == 1
    assert wf_spec.template.nodes[0].task_node.overrides is not None
    assert wf_spec.template.nodes[0].task_node.overrides.resources.requests == [
        _resources_models.ResourceEntry(_resources_models.ResourceName.CPU, "1"),
        _resources_models.ResourceEntry(_resources_models.ResourceName.MEMORY, "100"),
        _resources_models.ResourceEntry(_resources_models.ResourceName.EPHEMERAL_STORAGE, "500Mi"),
    ]

    assert wf_spec.template.nodes[0].task_node.overrides.resources.limits == [
        _resources_models.ResourceEntry(_resources_models.ResourceName.CPU, "2"),
        _resources_models.ResourceEntry(_resources_models.ResourceName.MEMORY, "200"),
        _resources_models.ResourceEntry(_resources_models.ResourceName.EPHEMERAL_STORAGE, "1Gi"),
    ]


@pytest.mark.parametrize(
    "timeout,expected",
    [(None, datetime.timedelta()), (10, datetime.timedelta(seconds=10))],
)
def test_timeout_override(timeout, expected):
    @task
    def t1(a: str) -> str:
        return f"*~*~*~{a}*~*~*~"

    @workflow
    def my_wf(a: str) -> str:
        return t1(a=a).with_overrides(timeout=timeout)

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert len(wf_spec.template.nodes) == 1
    assert wf_spec.template.nodes[0].metadata.timeout == expected


def test_timeout_override_invalid_value():
    @task
    def t1(a: str) -> str:
        return f"*~*~*~{a}*~*~*~"

    with pytest.raises(ValueError, match="datetime.timedelta or int seconds"):

        @workflow
        def my_wf(a: str) -> str:
            return t1(a=a).with_overrides(timeout="foo")


@pytest.mark.parametrize(
    "retries,expected", [(None, _literal_models.RetryStrategy(0)), (3, _literal_models.RetryStrategy(3))]
)
def test_retries_override(retries, expected):
    @task
    def t1(a: str) -> str:
        return f"*~*~*~{a}*~*~*~"

    @workflow
    def my_wf(a: str) -> str:
        return t1(a=a).with_overrides(retries=retries)

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert len(wf_spec.template.nodes) == 1
    assert wf_spec.template.nodes[0].metadata.retries == expected


@pytest.mark.parametrize("interruptible", [None, True, False])
def test_interruptible_override(interruptible):
    @task
    def t1(a: str) -> str:
        return f"*~*~*~{a}*~*~*~"

    @workflow
    def my_wf(a: str) -> str:
        return t1(a=a).with_overrides(interruptible=interruptible)

    serialization_settings = flytekit.configuration.SerializationSettings(
        project="test_proj",
        domain="test_domain",
        version="abc",
        image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
        env={},
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert len(wf_spec.template.nodes) == 1
    assert wf_spec.template.nodes[0].metadata.interruptible == interruptible
