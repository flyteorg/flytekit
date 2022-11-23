import typing

import pytest

import flytekit.configuration
from flytekit import dynamic
from flytekit.configuration import FastSerializationSettings, Image, ImageConfig
from flytekit.core import context_manager
from flytekit.core.context_manager import ExecutionState
from flytekit.core.node_creation import create_node
from flytekit.core.resources import Resources
from flytekit.core.task import task
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import workflow
from flytekit.models.literals import LiteralMap

settings = flytekit.configuration.SerializationSettings(
    project="test_proj",
    domain="test_domain",
    version="abc",
    image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
    env={},
    fast_serialization_settings=FastSerializationSettings(
        enabled=True,
        destination_dir="/User/flyte/workflows",
        distribution_location="s3://my-s3-bucket/fast/123",
    ),
)


def test_wf1_with_fast_dynamic():
    @task
    def t1(a: int) -> str:
        a = a + 2
        return "fast-" + str(a)

    @dynamic
    def my_subwf(a: int) -> typing.List[str]:
        s = []
        for i in range(a):
            s.append(t1(a=i))
        return s

    @workflow
    def my_wf(a: int) -> typing.List[str]:
        v = my_subwf(a=a)
        return v

    with context_manager.FlyteContextManager.with_context(
        context_manager.FlyteContextManager.current_context().with_serialization_settings(settings)
    ) as ctx:
        with context_manager.FlyteContextManager.with_context(
            ctx.with_execution_state(
                ctx.execution_state.with_params(
                    mode=ExecutionState.Mode.TASK_EXECUTION,
                )
            )
        ) as ctx:
            input_literal_map = TypeEngine.dict_to_literal_map(ctx, {"a": 5})
            dynamic_job_spec = my_subwf.dispatch_execute(ctx, input_literal_map)
            assert len(dynamic_job_spec._nodes) == 5
            assert len(dynamic_job_spec.tasks) == 1
            args = " ".join(dynamic_job_spec.tasks[0].container.args)
            assert args.startswith(
                "pyflyte-fast-execute --additional-distribution s3://my-s3-bucket/fast/123 "
                "--dest-dir /User/flyte/workflows"
            )

    assert context_manager.FlyteContextManager.size() == 1


def test_dynamic_local():
    @task
    def t1(a: int) -> str:
        a = a + 2
        return "fast-" + str(a)

    @dynamic
    def ranged_int_to_str(a: int) -> typing.List[str]:
        s = []
        for i in range(a):
            s.append(t1(a=i))
        return s

    res = ranged_int_to_str(a=5)
    assert res == ["fast-2", "fast-3", "fast-4", "fast-5", "fast-6"]


def test_nested_dynamic_local():
    @task
    def t1(a: int) -> str:
        a = a + 2
        return "fast-" + str(a)

    @dynamic
    def ranged_int_to_str(a: int) -> typing.List[str]:
        s = []
        for i in range(a):
            s.append(t1(a=i))
        return s

    @dynamic
    def add_and_range(a: int, b: int) -> typing.List[str]:
        x = a + b
        return ranged_int_to_str(a=x)

    res = add_and_range(a=2, b=3)
    assert res == ["fast-2", "fast-3", "fast-4", "fast-5", "fast-6"]

    @workflow
    def wf(a: int, b: int) -> typing.List[str]:
        return add_and_range(a=a, b=b)

    res = wf(a=2, b=3)
    assert res == ["fast-2", "fast-3", "fast-4", "fast-5", "fast-6"]


def test_dynamic_local_use():
    @task
    def t1(a: int) -> str:
        a = a + 2
        return "fast-" + str(a)

    @dynamic
    def use_result(a: int) -> int:
        x = t1(a=a)
        if len(x) > 6:
            return 5
        else:
            return 0

    with pytest.raises(TypeError):
        use_result(a=6)


def test_create_node_dynamic_local():
    @task
    def task1(s: str) -> str:
        return s

    @task
    def task2(s: str) -> str:
        return s

    @dynamic
    def dynamic_wf() -> str:
        node_1 = create_node(task1, s="hello")
        node_2 = create_node(task2, s="world")
        node_1 >> node_2

        return node_1.o0

    @workflow
    def wf() -> str:
        return dynamic_wf()

    assert wf() == "hello"


def test_dynamic_local_rshift():
    @task
    def task1(s: str) -> str:
        return s

    @task
    def task2(s: str) -> str:
        return s

    @dynamic
    def dynamic_wf() -> str:
        to1 = task1(s="hello").with_overrides(requests=Resources(cpu="3", mem="5Gi"))
        to2 = task2(s="world")
        to1 >> to2  # noqa

        return to1

    @workflow
    def wf() -> str:
        return dynamic_wf()

    assert wf() == "hello"

    with context_manager.FlyteContextManager.with_context(
        context_manager.FlyteContextManager.current_context().with_serialization_settings(settings)
    ) as ctx:
        with context_manager.FlyteContextManager.with_context(
            ctx.with_execution_state(
                ctx.execution_state.with_params(
                    mode=ExecutionState.Mode.TASK_EXECUTION,
                )
            )
        ) as ctx:
            dynamic_job_spec = dynamic_wf.dispatch_execute(ctx, LiteralMap(literals={}))
            assert dynamic_job_spec.nodes[1].upstream_node_ids == ["dn0"]
            assert dynamic_job_spec.nodes[0].task_node.overrides.resources.requests[0].value == "3"
            assert dynamic_job_spec.nodes[0].task_node.overrides.resources.requests[1].value == "5Gi"


def test_dynamic_return_dict():
    @dynamic
    def t1(v: str) -> typing.Dict[str, str]:
        return {"a": v}

    @dynamic
    def t2(v: str) -> typing.Dict[str, typing.Dict[str, str]]:
        return {"a": {"b": v}}

    @dynamic
    def t3(v: str) -> (str, typing.Dict[str, typing.Dict[str, str]]):
        return v, {"a": {"b": v}}

    @workflow
    def wf():
        t1(v="a")
        t2(v="b")
        t3(v="c")

    wf()


def test_nested_dynamic_locals():
    @task
    def t1(a: int) -> str:
        a = a + 2
        return "fast-" + str(a)

    @task
    def t2(b: str) -> str:
        return f"In t2 string is {b}"

    @task
    def t3(b: str) -> str:
        return f"In t3 string is {b}"

    @workflow()
    def normalwf(a: int) -> str:
        x = t1(a=a)
        return x

    @dynamic
    def dt(ss: str) -> typing.List[str]:
        if ss == "hello":
            bb = t2(b=ss)
            bbb = t3(b=bb)
        else:
            bb = t2(b=ss + "hi again")
            bbb = "static"
        return [bb, bbb]

    @workflow
    def wf(wf_in: str) -> typing.List[str]:
        x = dt(ss=wf_in)
        return x

    res = wf(wf_in="hello")
    assert res == ["In t2 string is hello", "In t3 string is In t2 string is hello"]

    res = dt(ss="hello")
    assert res == ["In t2 string is hello", "In t3 string is In t2 string is hello"]
