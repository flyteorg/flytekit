from collections import OrderedDict
from typing import List

import pytest

from flytekit.core import context_manager
from flytekit import task, workflow
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.array_node_map_task import map_task as array_node_map_task
from flytekit.core.context_manager import FlyteEntities
from flytekit.core.map_task import map_task
from flytekit.tools.serialize_helpers import get_registrable_entities
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


def test_serialization(serialization_settings):
    @task
    def t1(a: int) -> int:
        return a + 1

    @workflow
    def wf(xs: List[int]) -> List[int]:
        ys = map_task(t1)(a=xs)
        return array_node_map_task(t1)(a=ys)

    wf_spec = get_serializable(OrderedDict(), serialization_settings, wf)

    assert wf_spec is not None


def test_map(serialization_settings):
    @task
    def say_hello(name: str) -> str:
        return f"hello {name}!"

    @workflow
    def wf() -> List[str]:
        return array_node_map_task(say_hello)(name=["abc", "def"])

    res = wf()
    entities = FlyteEntities.entities
    assert res is not None

def test_execution(serialization_settings):
    @task
    def say_hello(name: str) -> str:
        return f"hello {name}!"

    @task
    def create_input_list() -> List[str]:
        return ["earth", "mars"]

    @task
    def print_list(xs: List[str]):
        print(xs)

    # @workflow
    # def wf():
    #     say_hello(name="world")

    @workflow
    def wf(name: str) -> List[str]:
        xs = array_node_map_task(say_hello)(name=create_input_list())
        return map_task(say_hello)(name=xs)

    assert wf(name="world") == ["hello hello earth!!", "hello hello mars!!"]
    assert 1 == 1

    # By default all map_task tasks will have their custom fields set.
    # assert task_spec.template.custom["minSuccessRatio"] == 1.0
    # assert task_spec.template.type == "container_array"
    # assert task_spec.template.task_type_version == 1
    # assert task_spec.template.container.args == [
    #     "pyflyte-map-execute",
    #     "--inputs",
    #     "{{.input}}",
    #     "--output-prefix",
    #     "{{.outputPrefix}}",
    #     "--raw-output-data-prefix",
    #     "{{.rawOutputDataPrefix}}",
    #     "--checkpoint-path",
    #     "{{.checkpointOutputPrefix}}",
    #     "--prev-checkpoint",
    #     "{{.prevCheckpointPrefix}}",
    #     "--resolver",
    #     "MapTaskResolver",
    #     "--",
    #     "vars",
    #     "",
    #     "resolver",
    #     "flytekit.core.python_auto_container.default_task_resolver",
    #     "task-module",
    #     "tests.flytekit.unit.core.test_map_task",
    #     "task-name",
    #     "t1",
    # ]
    #

def test_get_registrable_entities():
    @task
    def say_hello(name: str) -> str:
        return f"hello {name}!"

    @workflow
    def wf(name: str):
        # map_task(say_hello)(name=["alice"])
        array_node_map_task(say_hello)(name=["bob"])

    ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(
        SerializationSettings(
            project="p",
            domain="d",
            version="v",
            image_config=ImageConfig(
                default_image=Image("def", "docker.io/def", "latest")
            ),
        )
    )
    entities = get_registrable_entities(ctx)
    assert entities
    assert len(entities) == 4
