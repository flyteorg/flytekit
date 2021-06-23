import typing
from collections import OrderedDict

import pytest

from flytekit import LaunchPlan, Resources, Secret, SecurityContext, map_task
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.map_task import MapPythonTask
from flytekit.core.task import TaskMetadata, task
from flytekit.core.workflow import workflow
from flytekit.models.task import Resources as ResourceModels


@task
def t1(a: int) -> str:
    b = a + 2
    return str(b)


def test_map_task_types():
    strs = map_task(t1, metadata=TaskMetadata(retries=1))(a=[5, 6])
    assert strs == ["7", "8"]

    with pytest.raises(TypeError):
        _ = map_task(t1, metadata=TaskMetadata(retries=1))(a=1)

    with pytest.raises(TypeError):
        _ = map_task(t1, metadata=TaskMetadata(retries=1))(a=["invalid", "args"])


def test_serialization():
    maptask = map_task(t1, metadata=TaskMetadata(retries=1))
    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    task_spec = get_serializable(OrderedDict(), serialization_settings, maptask)

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
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "test_map_task",
        "task-name",
        "t1",
    ]


def test_serialization_workflow_def():
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

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
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


def test_task_config():
    @task(
        requests=Resources(cpu="1", mem="100"),
        limits=Resources(cpu="5", mem="500"),
        container_image="docker.io/org/myimage:latest",
        environment={"A": "A", "B": "B", "C": "C"},
        secret_requests=[Secret("my_group", "my_key")],
    )
    def simple_task(a: int) -> str:
        return str(a)

    maptask = map_task(
        simple_task,
        requests=Resources(cpu="2", mem="200"),
        limits=Resources(cpu="4", mem="400"),
        container_image="docker.io/org/myimage:abc",
        metadata=TaskMetadata(retries=3),
        environment={"A": "1", "B": "2", "C": "3"},
        secret_requests=[Secret("my_group", "my_other_key")],
    )
    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    task_spec = get_serializable(OrderedDict(), serialization_settings, maptask)
    assert task_spec.template.container.resources.requests == [
        ResourceModels.ResourceEntry(ResourceModels.ResourceName.CPU, "2"),
        ResourceModels.ResourceEntry(ResourceModels.ResourceName.MEMORY, "200"),
    ]
    assert task_spec.template.container.resources.limits == [
        ResourceModels.ResourceEntry(ResourceModels.ResourceName.CPU, "4"),
        ResourceModels.ResourceEntry(ResourceModels.ResourceName.MEMORY, "400"),
    ]
    assert task_spec.template.container.image == "docker.io/org/myimage:abc"
    assert task_spec.template.metadata.retries.retries == 3
    assert task_spec.template.container.env == {"A": "1", "B": "2", "C": "3"}
    assert task_spec.template.security_context == SecurityContext(secrets=[Secret("my_group", "my_other_key")])
