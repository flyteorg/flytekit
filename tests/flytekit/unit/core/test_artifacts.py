import typing
from collections import OrderedDict

from flyteidl.artifact import artifacts_pb2
from typing_extensions import Annotated

import flytekit.configuration
from flytekit.configuration import Image, ImageConfig
from flytekit.core.artifact import Artifact
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.tools.translator import get_serializable
from flytekit.types.structured.structured_dataset import StructuredDataset

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


class CustomReturn(object):
    def __init__(self, data):
        self.data = data


def test_artifact_interface():
    task_artifact = Artifact(name="task_artifact", tags={"type": "task"})
    wf_artifact = Artifact(name="wf_artifact", aliases={"version": "my_v0.1.0"})

    @task
    def t1() -> Annotated[typing.Union[CustomReturn, Annotated[StructuredDataset, "avro"]], task_artifact]:
        return CustomReturn({"name": ["Tom", "Joseph"], "age": [20, 22]})

    @workflow
    def wf() -> Annotated[CustomReturn, wf_artifact]:
        u = t1()
        return u

    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, t1)
    assert spec.template.interface.outputs["o0"].artifact.artifact_id.artifact_key.name == "task_artifact"
    assert spec.template.interface.outputs["o0"].artifact.spec.tags == [artifacts_pb2.Tag(key="type", value="task")]

    spec = get_serializable(entities, serialization_settings, wf)
    print(spec.template.interface.outputs["o0"].artifact)
    assert spec.template.interface.outputs["o0"].artifact.artifact_id.artifact_key.name == "wf_artifact"
    assert spec.template.interface.outputs["o0"].artifact.spec.aliases == [
        artifacts_pb2.Alias(key="version", value="my_v0.1.0")
    ]


def test_artifact_as_promise():

    @task
    def t1() -> Annotated[typing.Union[CustomReturn, Annotated[StructuredDataset, "avro"]], task_artifact]:
        return CustomReturn({"name": ["Tom", "Joseph"], "age": [20, 22]})

    @workflow
    def wf(a: CustomReturn = Artifact.as_query()):
        u = t1()
        return u
