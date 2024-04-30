import functools
import typing
from collections import OrderedDict
from typing import List

import pytest

from flytekit import map_task, task, workflow
from flytekit.configuration import FastSerializationSettings, Image, ImageConfig, SerializationSettings
from flytekit.core.array_node_map_task import ArrayNodeMapTask, ArrayNodeMapTaskResolver
from flytekit.core.task import TaskMetadata
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


def test_map(serialization_settings):
    @task
    def say_hello(name: str) -> str:
        return f"hello {name}!"

    @workflow
    def wf() -> List[str]:
        return map_task(say_hello)(name=["abc", "def"])

    res = wf()
    assert res is not None


def test_execution(serialization_settings):
    @task
    def say_hello(name: str) -> str:
        return f"hello {name}!"

    @task
    def create_input_list() -> List[str]:
        return ["earth", "mars"]

    @workflow
    def wf() -> List[str]:
        xs = map_task(say_hello)(name=create_input_list())
        return map_task(say_hello)(name=xs)

    assert wf() == ["hello hello earth!!", "hello hello mars!!"]


def test_serialization(serialization_settings):
    @task
    def t1(a: int) -> int:
        return a + 1

    arraynode_maptask = map_task(t1, metadata=TaskMetadata(retries=2))
    task_spec = get_serializable(OrderedDict(), serialization_settings, arraynode_maptask)

    assert task_spec.template.metadata.retries.retries == 2
    assert task_spec.template.custom["minSuccessRatio"] == 1.0
    assert task_spec.template.type == "python-task"
    assert task_spec.template.task_type_version == 1
    assert task_spec.template.container.args == [
        "pyflyte-map-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--checkpoint-path",
        "{{.checkpointOutputPrefix}}",
        "--prev-checkpoint",
        "{{.prevCheckpointPrefix}}",
        "--resolver",
        "flytekit.core.array_node_map_task.ArrayNodeMapTaskResolver",
        "--",
        "vars",
        "",
        "resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "task-module",
        "tests.flytekit.unit.core.test_array_node_map_task",
        "task-name",
        "t1",
    ]


def test_fast_serialization(serialization_settings):
    serialization_settings.fast_serialization_settings = FastSerializationSettings(enabled=True)

    @task
    def t1(a: int) -> int:
        return a + 1

    arraynode_maptask = map_task(t1, metadata=TaskMetadata(retries=2))
    task_spec = get_serializable(OrderedDict(), serialization_settings, arraynode_maptask)

    assert task_spec.template.container.args == [
        "pyflyte-fast-execute",
        "--additional-distribution",
        "{{ .remote_package_path }}",
        "--dest-dir",
        "{{ .dest_dir }}",
        "--",
        "pyflyte-map-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--checkpoint-path",
        "{{.checkpointOutputPrefix}}",
        "--prev-checkpoint",
        "{{.prevCheckpointPrefix}}",
        "--resolver",
        "flytekit.core.array_node_map_task.ArrayNodeMapTaskResolver",
        "--",
        "vars",
        "",
        "resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "task-module",
        "tests.flytekit.unit.core.test_array_node_map_task",
        "task-name",
        "t1",
    ]


@pytest.mark.parametrize(
    "kwargs1, kwargs2, same",
    [
        ({}, {}, True),
        ({}, {"concurrency": 2}, False),
        ({}, {"min_successes": 3}, False),
        ({}, {"min_success_ratio": 0.2}, False),
        ({}, {"concurrency": 10, "min_successes": 999, "min_success_ratio": 0.2}, False),
        ({"concurrency": 1}, {"concurrency": 2}, False),
        ({"concurrency": 42}, {"concurrency": 42}, True),
        ({"min_successes": 1}, {"min_successes": 2}, False),
        ({"min_successes": 42}, {"min_successes": 42}, True),
        ({"min_success_ratio": 0.1}, {"min_success_ratio": 0.2}, False),
        ({"min_success_ratio": 0.42}, {"min_success_ratio": 0.42}, True),
        ({"min_success_ratio": 0.42}, {"min_success_ratio": 0.42}, True),
        (
            {
                "concurrency": 1,
                "min_successes": 2,
                "min_success_ratio": 0.42,
            },
            {
                "concurrency": 1,
                "min_successes": 2,
                "min_success_ratio": 0.99,
            },
            False,
        ),
    ],
)
def test_metadata_in_task_name(kwargs1, kwargs2, same):
    @task
    def say_hello(name: str) -> str:
        return f"hello {name}!"

    t1 = map_task(say_hello, **kwargs1)
    t2 = map_task(say_hello, **kwargs2)

    assert (t1.name == t2.name) is same


def test_inputs_outputs_length():
    @task
    def many_inputs(a: int, b: str, c: float) -> str:
        return f"{a} - {b} - {c}"

    m = map_task(many_inputs)
    assert m.python_interface.inputs == {"a": List[int], "b": List[str], "c": List[float]}
    assert (
        m.name
        == "tests.flytekit.unit.core.test_array_node_map_task.map_many_inputs_6b3bd0353da5de6e84d7982921ead2b3-arraynode"
    )
    r_m = ArrayNodeMapTask(many_inputs)
    assert str(r_m.python_interface) == str(m.python_interface)

    p1 = functools.partial(many_inputs, c=1.0)
    m = map_task(p1)
    assert m.python_interface.inputs == {"a": List[int], "b": List[str], "c": float}
    assert (
        m.name
        == "tests.flytekit.unit.core.test_array_node_map_task.map_many_inputs_7df6892fe8ce5343c76197a0b6127e80-arraynode"
    )
    r_m = ArrayNodeMapTask(many_inputs, bound_inputs=set("c"))
    assert str(r_m.python_interface) == str(m.python_interface)

    p2 = functools.partial(p1, b="hello")
    m = map_task(p2)
    assert m.python_interface.inputs == {"a": List[int], "b": str, "c": float}
    assert (
        m.name
        == "tests.flytekit.unit.core.test_array_node_map_task.map_many_inputs_80fd21f14571026755b99d6b1c045089-arraynode"
    )
    r_m = ArrayNodeMapTask(many_inputs, bound_inputs={"c", "b"})
    assert str(r_m.python_interface) == str(m.python_interface)

    p3 = functools.partial(p2, a=1)
    m = map_task(p3)
    assert m.python_interface.inputs == {"a": int, "b": str, "c": float}
    assert (
        m.name
        == "tests.flytekit.unit.core.test_array_node_map_task.map_many_inputs_5d2500dc176052a030efda3b8c283f96-arraynode"
    )
    r_m = ArrayNodeMapTask(many_inputs, bound_inputs={"a", "c", "b"})
    assert str(r_m.python_interface) == str(m.python_interface)

    with pytest.raises(TypeError):
        m(a=[1, 2, 3])

    @task
    def many_outputs(a: int) -> (int, str):
        return a, f"{a}"

    with pytest.raises(ValueError):
        _ = map_task(many_outputs)


def test_parameter_order():
    @task()
    def task1(a: int, b: float, c: str) -> str:
        return f"{a} - {b} - {c}"

    @task()
    def task2(b: float, c: str, a: int) -> str:
        return f"{a} - {b} - {c}"

    @task()
    def task3(c: str, a: int, b: float) -> str:
        return f"{a} - {b} - {c}"

    param_a = [1, 2, 3]
    param_b = [0.1, 0.2, 0.3]
    param_c = "c"

    m1 = map_task(functools.partial(task1, c=param_c))(a=param_a, b=param_b)
    m2 = map_task(functools.partial(task2, c=param_c))(a=param_a, b=param_b)
    m3 = map_task(functools.partial(task3, c=param_c))(a=param_a, b=param_b)

    assert m1 == m2 == m3 == ["1 - 0.1 - c", "2 - 0.2 - c", "3 - 0.3 - c"]


def test_bounded_inputs_vars_order(serialization_settings):
    @task()
    def task1(a: int, b: float, c: str) -> str:
        return f"{a} - {b} - {c}"

    mt = map_task(functools.partial(task1, c=1.0, b="hello", a=1))
    mtr = ArrayNodeMapTaskResolver()
    args = mtr.loader_args(serialization_settings, mt)

    assert args[1] == "a,b,c"


@pytest.mark.parametrize(
    "min_success_ratio, should_raise_error",
    [
        (None, True),
        (1, True),
        (0.75, False),
        (0.5, False),
    ],
)
def test_raw_execute_with_min_success_ratio(min_success_ratio, should_raise_error):
    @task
    def some_task1(inputs: int) -> int:
        if inputs == 2:
            raise ValueError("Unexpected inputs: 2")
        return inputs

    @workflow
    def my_wf1() -> typing.List[typing.Optional[int]]:
        return map_task(some_task1, min_success_ratio=min_success_ratio)(inputs=[1, 2, 3, 4])

    if should_raise_error:
        with pytest.raises(ValueError):
            my_wf1()
    else:
        assert my_wf1() == [1, None, 3, 4]


def test_map_task_override(serialization_settings):
    @task
    def my_mappable_task(a: int) -> typing.Optional[str]:
        return str(a)

    @workflow
    def wf(x: typing.List[int]):
        map_task(my_mappable_task)(a=x).with_overrides(container_image="random:image")

    assert wf.nodes[0]._container_image == "random:image"


def test_serialization_metadata(serialization_settings):
    @task(interruptible=True)
    def t1(a: int) -> int:
        return a + 1

    arraynode_maptask = map_task(t1, metadata=TaskMetadata(retries=2))
    # since we manually override task metadata, the underlying task metadata will not be copied.
    assert not arraynode_maptask.metadata.interruptible

    @workflow
    def wf(x: typing.List[int]):
        return arraynode_maptask(a=x)

    od = OrderedDict()
    wf_spec = get_serializable(od, serialization_settings, wf)

    assert not arraynode_maptask.construct_node_metadata().interruptible
    assert not wf_spec.template.nodes[0].metadata.interruptible


def test_serialization_metadata2(serialization_settings):
    @task
    def t1(a: int) -> int:
        return a + 1

    arraynode_maptask = map_task(t1, metadata=TaskMetadata(retries=2, interruptible=True))
    assert arraynode_maptask.metadata.interruptible

    @workflow
    def wf(x: typing.List[int]):
        return arraynode_maptask(a=x)

    od = OrderedDict()
    wf_spec = get_serializable(od, serialization_settings, wf)

    assert arraynode_maptask.construct_node_metadata().interruptible
    assert wf_spec.template.nodes[0].metadata.interruptible
    task_spec = od[arraynode_maptask]
    assert task_spec.template.metadata.retries.retries == 2
    assert task_spec.template.metadata.interruptible
