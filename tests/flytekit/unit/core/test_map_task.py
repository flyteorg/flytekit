import typing
from collections import OrderedDict

import pytest

from flytekit.configuration import FastSerializationSettings, Image, ImageConfig, SerializationSettings
from flytekit.core.context_manager import ExecutionState, FlyteContextManager
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.map_task import MapPythonTask, map_task
from flytekit.core.task import TaskMetadata, task
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import workflow
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


@task
def t1(a: int) -> str:
    b = a + 2
    return str(b)


@task(cache=True, cache_version="1")
def t2(a: int) -> str:
    b = a + 2
    return str(b)


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
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
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

    lp = LaunchPlan.create("test", wf1)

    with pytest.raises(ValueError):

        @workflow
        def wf3(a: typing.List[int]):
            return map_task(lp)(a=a)


def test_inputs_outputs_length():
    @task
    def many_inputs(a: int, b: str, c: float) -> str:
        return f"{a} - {b} - {c}"

    with pytest.raises(ValueError):
        _ = map_task(many_inputs)

    @task
    def many_outputs(a: int) -> (int, str):
        return a, f"{a}"

    with pytest.raises(ValueError):
        _ = map_task(many_inputs)


def test_map_task_metadata():
    map_meta = TaskMetadata(retries=1)
    mapped_1 = map_task(t2, metadata=map_meta)
    assert mapped_1.metadata is map_meta
    mapped_2 = map_task(t2)
    assert mapped_2.metadata is t2.metadata


def test_fdsi():
    @task
    def a_mappable_task(a: int) -> str:
        inc = a + 2
        stringified = str(inc)
        return stringified

    @task
    def coalesce(b: typing.List[str]) -> str:
        coalesced = "".join(b)
        return coalesced

    @dynamic
    def wrapper_task(a: typing.List[int]) -> str:
        mapped_out = map_task(a_mappable_task)(a=a)
        coalesced = coalesce(b=mapped_out)
        return coalesced

    @workflow
    def my_map_workflow(a: typing.List[int]) -> str:
        return wrapper_task(a=a)

    result = my_map_workflow(a=[1, 2, 3, 4, 5])
    print(f"{result}")


def test_fdsif():
    @task
    def a_mappable_task(a: int) -> str:
        inc = a + 2
        stringified = str(inc)
        return stringified

    @task
    def coalesce(b: typing.List[str]) -> str:
        coalesced = "".join(b)
        return coalesced

    @dynamic
    def wrapper_task(a: typing.List[int]) -> str:
        mapped_out = map_task(a_mappable_task)(a=a)
        coalesced = coalesce(b=mapped_out)
        return coalesced

    with FlyteContextManager.with_context(
        FlyteContextManager.current_context().with_serialization_settings(
            SerializationSettings(
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
        )
    ) as ctx:
        with FlyteContextManager.with_context(
            ctx.with_execution_state(
                ctx.execution_state.with_params(
                    mode=ExecutionState.Mode.TASK_EXECUTION,
                )
            )
        ) as ctx:
            input_literal_map = TypeEngine.dict_to_literal_map(
                ctx, {"a": [1, 2, 3, 4, 5]}, type_hints={"a": typing.List[int]}
            )
            dynamic_job_spec = wrapper_task.dispatch_execute(ctx, input_literal_map)
            print(dynamic_job_spec)
