import pytest

from collections import OrderedDict
from flytekit.image_spec.image_spec import ImageSpec
from flytekit.core.task import task, eager
import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core.python_function_task import EagerFailureHandlerTask
from flytekit.tools.translator import get_serializable

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)

eager_image = "ghcr.io/testorg/testimage:v12"


def test_failure():
    t = EagerFailureHandlerTask(name="tester", inputs={"a": int})

    spec = get_serializable(OrderedDict(), serialization_settings, t)
    print(spec)

    assert spec.template.container.args == ['pyflyte-execute', '--inputs', '{{.input}}', '--output-prefix', '{{.outputPrefix}}', '--raw-output-data-prefix', '{{.rawOutputDataPrefix}}', '--checkpoint-path', '{{.checkpointOutputPrefix}}', '--prev-checkpoint', '{{.prevCheckpointPrefix}}', '--resolver', 'flytekit.core.python_function_task.eager_failure_task_resolver', '--', 'eager', 'failure', 'handler']


def test_loading():
    from flytekit.tools.module_loader import load_object_from_module

    resolver = load_object_from_module("flytekit.core.python_function_task.eager_failure_task_resolver")
    print(resolver)
    t = resolver.load_task([])
    assert isinstance(t, EagerFailureHandlerTask)


@task
def add_one(x: int) -> int:
    return x + 1


@eager(container_image=eager_image)
async def simple_eager_workflow(x: int) -> int:
    # This is the normal way of calling tasks. Call normal tasks in an effectively async way by hanging and waiting for
    # the result.
    out = add_one(x=x)
    return out


def test_as_wf():
    imperative_wf = simple_eager_workflow.get_as_workflow()
    assert imperative_wf.failure_node.flyte_entity.container_image == simple_eager_workflow.container_image

    entities = OrderedDict()
    get_serializable(entities, serialization_settings, imperative_wf)

    for key, entity in entities.items():
        if isinstance(key, EagerFailureHandlerTask):
            assert entity.template.container.image == "ghcr.io/testorg/testimage:v12"
