from collections import OrderedDict

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
