import functools
import typing
from collections import OrderedDict

import pytest

import flytekit.configuration
from flytekit import LaunchPlan, map_task
from flytekit.configuration import Image, ImageConfig
from flytekit.core.map_task import MapPythonTask, MapTaskResolver
from flytekit.core.task import TaskMetadata, task
from flytekit.core.workflow import workflow
from flytekit.tools.translator import get_serializable


@pytest.fixture
def serialization_settings():
    default_img = Image(name="default", fqn="test", tag="tag")
    return flytekit.configuration.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )


@task
def t1(a: int) -> str:
    b = a + 2
    return str(b)


@task(cache=True, cache_version="1")
def t2(a: int) -> str:
    b = a + 2
    return str(b)


@task(cache=True, cache_version="1")
def t3(a: int, b: str, c: float) -> str:
    pass


# This test is for documentation.
def test_map_docs():
    # test_map_task_start
    @task
    def my_mappable_task(a: int) -> str:
        return str(a)

    @workflow
    def my_wf(x: typing.List[int]) -> typing.List[str]:
        return map_task(my_mappable_task, metadata=TaskMetadata(retries=1), concurrency=10, min_success_ratio=0.75,)(
            a=x
        ).with_overrides(cpu="10M")

    # test_map_task_end

    res = my_wf(x=[1, 2, 3])
    assert res == ["1", "2", "3"]


def test_map_task_types():
    strs = map_task(t1, metadata=TaskMetadata(retries=1))(a=[5, 6])
    assert strs == ["7", "8"]

    with pytest.raises(TypeError):
        _ = map_task(t1, metadata=TaskMetadata(retries=1))(a=1)

    with pytest.raises(TypeError):
        _ = map_task(t1, metadata=TaskMetadata(retries=1))(a=["invalid", "args"])


def test_serialization(serialization_settings):
    maptask = map_task(t1, metadata=TaskMetadata(retries=1))
    task_spec = get_serializable(OrderedDict(), serialization_settings, maptask)

    # By default all map_task tasks will have their custom fields set.
    assert task_spec.template.custom["minSuccessRatio"] == 1.0
    assert task_spec.template.type == "container_array"
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
        "MapTaskResolver",
        "--",
        "vars",
        "",
        "resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "task-module",
        "tests.flytekit.unit.core.test_map_task",
        "task-name",
        "t1",
    ]


@pytest.mark.parametrize(
    "custom_fields_dict, expected_custom_fields",
    [
        ({}, {"minSuccessRatio": 1.0}),
        ({"concurrency": 99}, {"parallelism": "99", "minSuccessRatio": 1.0}),
        ({"min_success_ratio": 0.271828}, {"minSuccessRatio": 0.271828}),
        ({"concurrency": 42, "min_success_ratio": 0.31415}, {"parallelism": "42", "minSuccessRatio": 0.31415}),
    ],
)
def test_serialization_of_custom_fields(custom_fields_dict, expected_custom_fields, serialization_settings):
    maptask = map_task(t1, **custom_fields_dict)
    task_spec = get_serializable(OrderedDict(), serialization_settings, maptask)

    assert task_spec.template.custom == expected_custom_fields


def test_serialization_workflow_def(serialization_settings):
    @task
    def complex_task(a: int) -> str:
        b = a + 2
        return str(b)

    maptask = map_task(complex_task, metadata=TaskMetadata(retries=1))

    @workflow
    def w1(a: typing.List[int]) -> typing.List[str]:
        return maptask(a=a)

    @workflow
    def w2(a: typing.List[int]) -> typing.List[str]:
        return map_task(complex_task, metadata=TaskMetadata(retries=2))(a=a)

    serialized_control_plane_entities = OrderedDict()
    wf1_spec = get_serializable(serialized_control_plane_entities, serialization_settings, w1)
    assert wf1_spec.template is not None
    assert len(wf1_spec.template.nodes) == 1

    wf2_spec = get_serializable(serialized_control_plane_entities, serialization_settings, w2)
    assert wf2_spec.template is not None
    assert len(wf2_spec.template.nodes) == 1

    flyte_entities = list(serialized_control_plane_entities.keys())

    tasks_seen = []
    for entity in flyte_entities:
        if isinstance(entity, MapPythonTask) and "complex" in entity.name:
            tasks_seen.append(entity)

    assert len(tasks_seen) == 2
    print(tasks_seen[0])


def test_map_tasks_only():
    @workflow
    def wf1(a: int):
        print(f"{a}")

    with pytest.raises(ValueError):

        @workflow
        def wf2(a: typing.List[int]):
            return map_task(wf1)(a=a)

        wf2()

    lp = LaunchPlan.create("test", wf1)

    with pytest.raises(ValueError):

        @workflow
        def wf3(a: typing.List[int]):
            return map_task(lp)(a=a)

        wf3()


def test_inputs_outputs_length():
    @task
    def many_inputs(a: int, b: str, c: float) -> str:
        return f"{a} - {b} - {c}"

    m = map_task(many_inputs)
    assert m.python_interface.inputs == {"a": typing.List[int], "b": typing.List[str], "c": typing.List[float]}
    assert m.name == "tests.flytekit.unit.core.test_map_task.map_many_inputs_24c08b3a2f9c2e389ad9fc6a03482cf9"
    r_m = MapPythonTask(many_inputs)
    assert str(r_m.python_interface) == str(m.python_interface)

    p1 = functools.partial(many_inputs, c=1.0)
    m = map_task(p1)
    assert m.python_interface.inputs == {"a": typing.List[int], "b": typing.List[str], "c": float}
    assert m.name == "tests.flytekit.unit.core.test_map_task.map_many_inputs_697aa7389996041183cf6cfd102be4f7"
    r_m = MapPythonTask(many_inputs, bound_inputs=set("c"))
    assert str(r_m.python_interface) == str(m.python_interface)

    p2 = functools.partial(p1, b="hello")
    m = map_task(p2)
    assert m.python_interface.inputs == {"a": typing.List[int], "b": str, "c": float}
    assert m.name == "tests.flytekit.unit.core.test_map_task.map_many_inputs_cc18607da7494024a402a5fa4b3ea5c6"
    r_m = MapPythonTask(many_inputs, bound_inputs={"c", "b"})
    assert str(r_m.python_interface) == str(m.python_interface)

    p3 = functools.partial(p2, a=1)
    m = map_task(p3)
    assert m.python_interface.inputs == {"a": int, "b": str, "c": float}
    assert m.name == "tests.flytekit.unit.core.test_map_task.map_many_inputs_52fe80b04781ea77ef6f025f4b49abef"
    r_m = MapPythonTask(many_inputs, bound_inputs={"a", "c", "b"})
    assert str(r_m.python_interface) == str(m.python_interface)

    with pytest.raises(TypeError):
        m(a=[1, 2, 3])

    @task
    def many_outputs(a: int) -> (int, str):
        return a, f"{a}"

    with pytest.raises(ValueError):
        _ = map_task(many_outputs)


def test_map_task_metadata():
    map_meta = TaskMetadata(retries=1)
    mapped_1 = map_task(t2, metadata=map_meta)
    assert mapped_1.metadata is map_meta
    mapped_2 = map_task(t2)
    assert mapped_2.metadata is t2.metadata


def test_map_task_resolver(serialization_settings):
    list_outputs = {"o0": typing.List[str]}
    mt = map_task(t3)
    assert mt.python_interface.inputs == {"a": typing.List[int], "b": typing.List[str], "c": typing.List[float]}
    assert mt.python_interface.outputs == list_outputs
    mtr = MapTaskResolver()
    assert mtr.name() == "MapTaskResolver"
    args = mtr.loader_args(serialization_settings, mt)
    t = mtr.load_task(loader_args=args)
    assert t.python_interface.inputs == mt.python_interface.inputs
    assert t.python_interface.outputs == mt.python_interface.outputs

    mt = map_task(functools.partial(t3, b="hello", c=1.0))
    assert mt.python_interface.inputs == {"a": typing.List[int], "b": str, "c": float}
    assert mt.python_interface.outputs == list_outputs
    mtr = MapTaskResolver()
    args = mtr.loader_args(serialization_settings, mt)
    t = mtr.load_task(loader_args=args)
    assert t.python_interface.inputs == mt.python_interface.inputs
    assert t.python_interface.outputs == mt.python_interface.outputs

    mt = map_task(functools.partial(t3, b="hello"))
    assert mt.python_interface.inputs == {"a": typing.List[int], "b": str, "c": typing.List[float]}
    assert mt.python_interface.outputs == list_outputs
    mtr = MapTaskResolver()
    args = mtr.loader_args(serialization_settings, mt)
    t = mtr.load_task(loader_args=args)
    assert t.python_interface.inputs == mt.python_interface.inputs
    assert t.python_interface.outputs == mt.python_interface.outputs
