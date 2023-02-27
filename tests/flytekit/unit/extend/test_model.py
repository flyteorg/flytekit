from datetime import timedelta

from flytekit.extend.backend.model import TaskCreateRequest
from flytekit.models import literals
from flytekit.models.core import identifier
from flytekit.models.interface import TypedInterface
from flytekit.models.literals import Literal, LiteralMap, Primitive, Scalar
from flytekit.models.task import Container, Resources, RuntimeMetadata, TaskMetadata, TaskTemplate


def test_create_request():
    inputs = LiteralMap({"foo": Literal(scalar=Scalar(primitive=Primitive(integer=2)))})
    resource = [Resources.ResourceEntry(Resources.ResourceName.CPU, "1")]
    resources = Resources(resource, resource)
    template = TaskTemplate(
        identifier.Identifier(identifier.ResourceType.TASK, "project", "domain", "name", "version"),
        "python",
        TaskMetadata(
            True,
            RuntimeMetadata(RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
            timedelta(days=1),
            literals.RetryStrategy(3),
            True,
            "0.1.1b0",
            "This is deprecated!",
            True,
            "A",
        ),
        TypedInterface(inputs={}, outputs={}),
        {"a": 1, "b": {"c": 2, "d": 3}},
        container=Container(
            "my_image",
            ["this", "is", "a", "cmd"],
            ["this", "is", "an", "arg"],
            resources,
            {},
            {},
        ),
    )
    req = TaskCreateRequest(inputs=inputs, template=template)
    assert req.inputs == inputs
    assert req.template == template
    assert req == TaskCreateRequest.from_flyte_idl(req.to_flyte_idl())
