import functools
from datetime import timedelta
import os
import tempfile
import typing
from collections import OrderedDict
from typing import List

import pytest
from flyteidl.core import workflow_pb2 as _core_workflow

from flytekit import dynamic, map, task, workflow, eager, PythonFunctionTask
from flytekit.configuration import FastSerializationSettings, Image, ImageConfig, SerializationSettings
from flytekit.core import context_manager
from flytekit.core.array_node_map_task import ArrayNodeMapTask, ArrayNodeMapTaskResolver
from flytekit.core.task import TaskMetadata
from flytekit.core.type_engine import TypeEngine
from flytekit.extras.accelerators import GPUAccelerator
from flytekit.models import types
from flytekit.models.literals import (
    BindingData,
    Literal,
    LiteralMap,
    LiteralOffloadedMetadata,
    Scalar,
    Primitive,
)
from flytekit.tools.translator import get_serializable
from flytekit.types.directory import FlyteDirectory


class PythonFunctionTaskExtension(PythonFunctionTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


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


@pytest.fixture
def interactive_serialization_settings():
    default_img = Image(name="default", fqn="test", tag="tag")
    return SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        interactive_mode_enabled=True,
    )


def test_map(serialization_settings):
    @task
    def say_hello(name: str) -> str:
        return f"hello {name}!"

    @workflow
    def wf() -> List[str]:
        return map(say_hello)(name=["abc", "def"])

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
        xs = map(say_hello)(name=create_input_list())
        return map(say_hello)(name=xs)

    assert wf() == ["hello hello earth!!", "hello hello mars!!"]


def test_remote_execution(serialization_settings):
    @task
    def say_hello(name: str) -> str:
        return f"hello {name}!"

    ctx = context_manager.FlyteContextManager.current_context()
    with context_manager.FlyteContextManager.with_context(
            ctx.with_execution_state(
                ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
            )
    ) as ctx:
        t = map(say_hello)
        lm = TypeEngine.dict_to_literal_map(ctx, {"name": ["earth", "mars"]}, type_hints={"name": typing.List[str]})
        res = t.dispatch_execute(ctx, lm)
        assert len(res.literals) == 1
        assert res.literals["o0"].scalar.primitive.string_value == "hello earth!"


def test_map_task_with_pickle():
    @task
    def say_hello(name: typing.Any) -> str:
        return f"hello {name}!"

    map(say_hello)(name=["abc", "def"])


def test_serialization(serialization_settings):
    @task
    def t1(a: int) -> int:
        return a + 1

    arraynode_maptask = map(t1, metadata=TaskMetadata(retries=2))
    task_spec = get_serializable(OrderedDict(), serialization_settings, arraynode_maptask)

    assert task_spec.template.metadata.retries.retries == 2
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

    arraynode_maptask = map(t1, metadata=TaskMetadata(retries=2))
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

    t1 = map(say_hello, **kwargs1)
    t2 = map(say_hello, **kwargs2)

    assert (t1.name == t2.name) is same


def test_inputs_outputs_length():
    @task
    def many_inputs(a: int, b: str, c: float) -> str:
        return f"{a} - {b} - {c}"

    m = map(many_inputs)
    assert m.python_interface.inputs == {"a": List[int], "b": List[str], "c": List[float]}
    assert (
        m.name
        == "tests.flytekit.unit.core.test_array_node_map_task.map_many_inputs_6b3bd0353da5de6e84d7982921ead2b3-arraynode"
    )
    r_m = ArrayNodeMapTask(many_inputs)
    assert str(r_m.python_interface) == str(m.python_interface)

    p1 = functools.partial(many_inputs, c=1.0)
    m = map(p1)
    assert m.python_interface.inputs == {"a": List[int], "b": List[str], "c": float}
    assert (
        m.name
        == "tests.flytekit.unit.core.test_array_node_map_task.map_many_inputs_7df6892fe8ce5343c76197a0b6127e80-arraynode"
    )
    r_m = ArrayNodeMapTask(many_inputs, bound_inputs=set("c"))
    assert str(r_m.python_interface) == str(m.python_interface)

    p2 = functools.partial(p1, b="hello")
    m = map(p2)
    assert m.python_interface.inputs == {"a": List[int], "b": str, "c": float}
    assert (
        m.name
        == "tests.flytekit.unit.core.test_array_node_map_task.map_many_inputs_80fd21f14571026755b99d6b1c045089-arraynode"
    )
    r_m = ArrayNodeMapTask(many_inputs, bound_inputs={"c", "b"})
    assert str(r_m.python_interface) == str(m.python_interface)

    p3 = functools.partial(p2, a=1)
    m = map(p3)
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
        _ = map(many_outputs)


def test_partials_local_execute():
    @task()
    def task1(a: int, b: float, c: str) -> str:
        return f"{a} - {b} - {c}"

    @task()
    def task2(b: float, c: str, a: int) -> str:
        return f"{a} - {b} - {c}"

    @task()
    def task3(c: str, a: int, b: float) -> str:
        return f"{a} - {b} - {c}"

    @task()
    def task4(c: list[str], a: list[int], b: list[float]) -> list[str]:
        return [f"{a[i]} - {b[i]} - {c[i]}" for i in range(len(a))]

    param_a = [1, 2, 3]
    param_b = [0.1, 0.2, 0.3]
    fixed_param_c = "c"

    m1 = map(functools.partial(task1, c=param_c))(a=param_a, b=param_b)
    m2 = map(functools.partial(task2, c=param_c))(a=param_a, b=param_b)
    m3 = map(functools.partial(task3, c=param_c))(a=param_a, b=param_b)


    m4 = ArrayNodeMapTask(task1, bound_inputs_values={"c": fixed_param_c})(a=param_a, b=param_b)
    m5 = ArrayNodeMapTask(task2, bound_inputs_values={"c": fixed_param_c})(a=param_a, b=param_b)
    m6 = ArrayNodeMapTask(task3, bound_inputs_values={"c": fixed_param_c})(a=param_a, b=param_b)

    assert m1 == m2 == m3 == m4 == m5 == m6 == ["1 - 0.1 - c", "2 - 0.2 - c", "3 - 0.3 - c"]

    list_param_a = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    list_param_b = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]]
    fixed_list_param_c = ["c", "d", "e"]

    m7 = map_task(functools.partial(task4, c=fixed_list_param_c))(a=list_param_a, b=list_param_b)
    m8 = ArrayNodeMapTask(task4, bound_inputs_values={"c": fixed_list_param_c})(a=list_param_a, b=list_param_b)

    assert m7 == m8 == [
        ['1 - 0.1 - c', '2 - 0.2 - d', '3 - 0.3 - e'],
        ['4 - 0.4 - c', '5 - 0.5 - d', '6 - 0.6 - e'],
        ['7 - 0.7 - c', '8 - 0.8 - d', '9 - 0.9 - e']
    ]

    with pytest.raises(ValueError):
        map_task(functools.partial(task1, c=fixed_list_param_c))(a=param_a, b=param_b)


def test_bounded_inputs_vars_order(serialization_settings):
    @task()
    def task1(a: int, b: float, c: str) -> str:
        return f"{a} - {b} - {c}"

    mt = map(functools.partial(task1, c=1.0, b="hello", a=1))
    mtr = ArrayNodeMapTaskResolver()
    args = mtr.loader_args(serialization_settings, mt)

    assert args[1] == "a,b,c"


def test_bound_inputs_collision():
    @task()
    def task1(a: int, b: float, c: str) -> str:
        return f"{a} - {b} - {c}"

    param_a = [1, 2, 3]
    param_b = [0.1, 0.2, 0.3]
    param_c = "c"
    param_d = "d"

    partial_task = functools.partial(task1, c=param_c)
    m1 = ArrayNodeMapTask(partial_task, bound_inputs_values={"c": param_d})(a=param_a, b=param_b)

    assert m1 == ["1 - 0.1 - d", "2 - 0.2 - d", "3 - 0.3 - d"]

    with pytest.raises(ValueError, match="bound_inputs and bound_inputs_values should have the same keys if both set"):
        ArrayNodeMapTask(task1, bound_inputs_values={"c": param_c}, bound_inputs={"b"})(a=param_a, b=param_b)

    # no error raised
    ArrayNodeMapTask(task1, bound_inputs_values={"c": param_c}, bound_inputs={"c"})(a=param_a, b=param_b)


@task()
def task_1(a: int, b: int, c: str) -> str:
    return f"{a} - {b} - {c}"


@task()
def task_2() -> int:
    return 2


def get_wf_bound_input(serialization_settings):
    @workflow()
    def wf1() -> List[str]:
        return ArrayNodeMapTask(task_1, bound_inputs_values={"a": 1})(b=[1, 2, 3], c=["a", "b", "c"])

    return wf1


def get_wf_partials(serialization_settings):
    @workflow()
    def wf2() -> List[str]:
        return ArrayNodeMapTask(functools.partial(task_1, a=1))(b=[1, 2, 3], c=["a", "b", "c"])

    return wf2


def get_wf_bound_input_upstream(serialization_settings):

    @workflow()
    def wf3() -> List[str]:
        a = task_2()
        return ArrayNodeMapTask(task_1, bound_inputs_values={"a": a})(b=[1, 2, 3], c=["a", "b", "c"])

    return wf3


def get_wf_partials_upstream(serialization_settings):

    @workflow()
    def wf4() -> List[str]:
        a = task_2()
        return ArrayNodeMapTask(functools.partial(task_1, a=a))(b=[1, 2, 3], c=["a", "b", "c"])

    return wf4


def get_wf_bound_input_partials_collision(serialization_settings):

    @workflow()
    def wf5() -> List[str]:
        return ArrayNodeMapTask(functools.partial(task_1, a=1), bound_inputs_values={"a": 2})(b=[1, 2, 3], c=["a", "b", "c"])

    return wf5


def get_wf_bound_input_overrides(serialization_settings):

    @workflow()
    def wf6() -> List[str]:
        return ArrayNodeMapTask(task_1, bound_inputs_values={"a": 1})(a=[1, 2, 3], b=[1, 2, 3], c=["a", "b", "c"])

    return wf6


def get_int_binding(value):
    return BindingData(scalar=Scalar(primitive=Primitive(integer=value)))


def get_str_binding(value):
    return BindingData(scalar=Scalar(primitive=Primitive(string_value=value)))


def promise_binding(node_id, var):
    return BindingData(promise=types.OutputReference(node_id=node_id, var=var))


B_BINDINGS_LIST = [get_int_binding(1), get_int_binding(2), get_int_binding(3)]
C_BINDINGS_LIST = [get_str_binding("a"), get_str_binding("b"), get_str_binding("c")]


@pytest.mark.parametrize(
    ("wf", "upstream_nodes", "expected_inputs"),
    [
        (get_wf_bound_input, {}, {"a": get_int_binding(1), "b": B_BINDINGS_LIST, "c": C_BINDINGS_LIST}),
        (get_wf_partials, {}, {"a": get_int_binding(1), "b": B_BINDINGS_LIST, "c": C_BINDINGS_LIST}),
        (get_wf_bound_input_upstream, {"n0"}, {"a": promise_binding("n0", "o0"), "b": B_BINDINGS_LIST, "c": C_BINDINGS_LIST}),
        (get_wf_partials_upstream, {"n0"}, {"a": promise_binding("n0", "o0"), "b": B_BINDINGS_LIST, "c": C_BINDINGS_LIST}),
        (get_wf_bound_input_partials_collision, {}, {"a": get_int_binding(2), "b": B_BINDINGS_LIST, "c": C_BINDINGS_LIST}),
        (get_wf_bound_input_overrides, {}, {"a": get_int_binding(1), "b": B_BINDINGS_LIST, "c": C_BINDINGS_LIST}),
    ]
)
def test_bound_inputs_serialization(wf, upstream_nodes, expected_inputs, serialization_settings):
    wf_spec = get_serializable(OrderedDict(), serialization_settings, wf(serialization_settings))
    assert len(wf_spec.template.nodes) == len(upstream_nodes) + 1
    parent_node = wf_spec.template.nodes[len(upstream_nodes)]

    assert len(parent_node.inputs) == len(expected_inputs)
    inputs_map = {x.var: x for x in parent_node.inputs}

    for param, expected_input in expected_inputs.items():
        node_input = inputs_map[param]
        assert node_input
        if isinstance(expected_input, list):
            bindings = node_input.binding.collection.bindings
            assert len(bindings) == len(expected_inputs[param])
            for i, binding in enumerate(bindings):
                assert binding == expected_input[i]
        else:
            binding = node_input.binding
            assert binding == expected_input

    assert parent_node.array_node._bound_inputs == {"a"}
    assert set(parent_node.upstream_node_ids) == set(upstream_nodes)


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
        return map(some_task1, min_success_ratio=min_success_ratio)(inputs=[1, 2, 3, 4])

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
        map(my_mappable_task)(a=x).with_overrides(container_image="random:image")

    assert wf.nodes[0]._container_image == "random:image"


def test_serialization_metadata(serialization_settings):
    @task(interruptible=True)
    def t1(a: int) -> int:
        return a + 1

    arraynode_maptask = map(t1, metadata=TaskMetadata(retries=2))
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
    def t1(a: int) -> typing.Optional[int]:
        return a + 1

    arraynode_maptask = map(
        t1,
        min_success_ratio=0.9,
        concurrency=10,
        metadata=TaskMetadata(retries=2, interruptible=True, timeout=timedelta(seconds=10))
    )
    assert arraynode_maptask.metadata.interruptible

    @workflow
    def wf(x: typing.List[int]):
        return arraynode_maptask(a=x)

    full_state_array_node_map_task = map(PythonFunctionTaskExtension(task_config={}, task_function=t1))

    @workflow
    def wf1(x: typing.List[int]):
        return full_state_array_node_map_task(a=x)

    od = OrderedDict()
    wf_spec = get_serializable(od, serialization_settings, wf)

    array_node = wf_spec.template.nodes[0]
    assert array_node.metadata.timeout == timedelta()
    assert array_node.array_node._min_success_ratio == 0.9
    assert array_node.array_node._parallelism == 10
    assert not array_node.array_node._is_original_sub_node_interface
    assert array_node.array_node._execution_mode == _core_workflow.ArrayNode.MINIMAL_STATE
    task_spec = od[arraynode_maptask]
    assert task_spec.template.metadata.retries.retries == 2
    assert task_spec.template.metadata.interruptible
    assert task_spec.template.metadata.timeout == timedelta(seconds=10)

    wf1_spec = get_serializable(od, serialization_settings, wf1)
    array_node = wf1_spec.template.nodes[0]
    assert array_node.array_node._execution_mode == _core_workflow.ArrayNode.FULL_STATE


def test_serialization_extended_resources(serialization_settings):
    @task(
        accelerator=GPUAccelerator("test_gpu"),
    )
    def t1(a: int) -> int:
        return a + 1

    arraynode_maptask = map(t1)

    @workflow
    def wf(x: typing.List[int]):
        return arraynode_maptask(a=x)

    od = OrderedDict()
    get_serializable(od, serialization_settings, wf)
    task_spec = od[arraynode_maptask]

    assert task_spec.template.extended_resources.gpu_accelerator.device == "test_gpu"


def test_serialization_extended_resources_shared_memory(serialization_settings):
    @task(
        shared_memory="2Gi"
    )
    def t1(a: int) -> int:
        return a + 1

    arraynode_maptask = map(t1)

    @workflow
    def wf(x: typing.List[int]):
        return arraynode_maptask(a=x)

    od = OrderedDict()
    get_serializable(od, serialization_settings, wf)
    task_spec = od[arraynode_maptask]

    assert task_spec.template.extended_resources.shared_memory.size_limit == "2Gi"


def test_supported_node_type():
    @task
    def test_task():
        ...

    map(test_task)


def test_unsupported_node_types():
    @dynamic
    def test_dynamic():
        ...

    with pytest.raises(ValueError):
        map(test_dynamic)

    @eager
    def test_eager():
        ...

    with pytest.raises(ValueError):
        map(test_eager)

    @workflow
    def test_wf():
        ...

    with pytest.raises(ValueError):
        map(test_wf)


def test_mis_match():
    @task
    def generate_directory(word: str) -> FlyteDirectory:
        temp_dir1 = tempfile.TemporaryDirectory()
        with open(os.path.join(temp_dir1.name, "file.txt"), "w") as tmp:
            tmp.write(f"Hello world {word}!\n")
        return FlyteDirectory(path=temp_dir1.name)

    @task
    def consume_directories(dirs: List[FlyteDirectory]):
        for d in dirs:
            print(f"Directory: {d.path} {d._remote_source}")
            for path_info, other_info in d.crawl():
                print(path_info)

    mt = map(generate_directory, min_success_ratio=0.1)

    @workflow
    def wf():
        dirs = mt(word=["one", "two", "three"])
        consume_directories(dirs=dirs)

    with pytest.raises(AssertionError):
        wf.compile()


def test_load_offloaded_literal(tmp_path, monkeypatch):
    @task
    def say_hello(name: str) -> str:
        return f"hello {name}!"

    ctx = context_manager.FlyteContextManager.current_context()
    with context_manager.FlyteContextManager.with_context(
            ctx.with_execution_state(
                ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
            )
    ) as ctx:
        list_strs = ["a", "b", "c"]
        lt = TypeEngine.to_literal_type(typing.List[str])
        to_be_offloaded = TypeEngine.to_literal(ctx, list_strs, typing.List[str], lt)
        with open(f"{tmp_path}/literal.pb", "wb") as f:
            f.write(to_be_offloaded.to_flyte_idl().SerializeToString())

        literal = Literal(
            offloaded_metadata=LiteralOffloadedMetadata(
                uri=f"{tmp_path}/literal.pb",
                inferred_type=lt,
            ),
        )

        lm = LiteralMap({
            "name": literal
        })

        for index, map_input_str in enumerate(list_strs):
            monkeypatch.setenv("BATCH_JOB_ARRAY_INDEX_VAR_NAME", "name")
            monkeypatch.setenv("name", str(index))
            t = map(say_hello)
            res = t.dispatch_execute(ctx, lm)
            assert len(res.literals) == 1
            assert res.literals[f"o{0}"].scalar.primitive.string_value == f"hello {map_input_str}!"
            monkeypatch.undo()
