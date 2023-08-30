import typing
from collections import OrderedDict

import pytest
from flyteidl.artifact import artifacts_pb2
from flyteidl.core import identifier_pb2
from typing_extensions import Annotated

from flytekit.configuration import Config, Image, ImageConfig, SerializationSettings
from flytekit.core.artifact import Artifact
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.remote.remote import FlyteRemote
from flytekit.tools.translator import get_serializable
from flytekit.types.structured.structured_dataset import StructuredDataset

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


class CustomReturn(object):
    def __init__(self, data):
        self.data = data


def test_controlling_aliases_when_running():
    task_alias = Artifact(name="task_artifact", aliases=["latest"])
    wf_alias = Artifact(name="wf_artifact", aliases=["my_v0.1.0"])

    @task
    def t1() -> Annotated[typing.Union[CustomReturn, Annotated[StructuredDataset, "avro"]], task_alias]:
        return CustomReturn({"name": ["Tom", "Joseph"], "age": [20, 22]})

    @workflow
    def wf() -> Annotated[CustomReturn, wf_alias]:
        u = t1()
        return u

    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, t1)
    alias = spec.template.interface.outputs["o0"].aliases[0]
    assert alias.name == "task_artifact"
    assert alias.value == "latest"

    spec = get_serializable(entities, serialization_settings, wf)
    alias = spec.template.interface.outputs["o0"].aliases[0]
    assert alias.name == "wf_artifact"
    assert alias.value == "my_v0.1.0"


def test_artifact_as_promise_query():
    # when artifact is partially specified, can be used as a query input
    wf_artifact = Artifact(project="project1", domain="dev", name="wf_artifact", aliases=["my_v0.1.0"])

    @task
    def t1(a: CustomReturn) -> CustomReturn:
        print(a)
        return CustomReturn({"name": ["Tom", "Joseph"], "age": [20, 22]})

    @workflow
    def wf(a: CustomReturn = wf_artifact.as_query()):
        u = t1(a=a)
        return u

    ctx = FlyteContextManager.current_context()
    lp = LaunchPlan.get_default_launch_plan(ctx, wf)
    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, lp)
    assert spec.spec.default_inputs.parameters["a"].artifact_query
    assert spec.spec.default_inputs.parameters["a"].artifact_query.project == "project1"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.domain == "dev"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.alias.name == "wf_artifact"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.alias.value == "my_v0.1.0"


def test_artifact_as_promise():
    # when the full artifact is specified, the artifact should be bindable as a literal
    wf_artifact = Artifact(project="pro", domain="dom", suffix="key")

    @task
    def t1(a: CustomReturn) -> CustomReturn:
        print(a)
        return CustomReturn({"name": ["Tom", "Joseph"], "age": [20, 22]})

    @workflow
    def wf(a: CustomReturn = wf_artifact):
        u = t1(a=a)
        return u

    ctx = FlyteContextManager.current_context()
    lp = LaunchPlan.get_default_launch_plan(ctx, wf)
    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, lp)
    assert spec.spec.default_inputs.parameters["a"].default.artifact_id.artifact_key.project == "pro"
    assert spec.spec.default_inputs.parameters["a"].default.artifact_id.artifact_key.domain == "dom"
    assert spec.spec.default_inputs.parameters["a"].default.artifact_id.artifact_key.suffix == "key"


@pytest.mark.sandbox_test
def test_create_an_artifact33_locally():
    import grpc
    from flyteidl.artifact.artifacts_pb2_grpc import ArtifactRegistryStub

    local_artifact_channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = ArtifactRegistryStub(local_artifact_channel)
    ak = identifier_pb2.ArtifactKey(
        project="flytesnacks", domain="development", suffix="f3bea14ee52f8409eb5b/n0/0/o/o0"
    )
    req = artifacts_pb2.GetArtifactRequest(artifact_key=ak)
    x = stub.GetArtifact(req)
    print(x)


@pytest.mark.sandbox_test
def test_create_an_artifact_locally():
    df = pd.DataFrame({"Name": ["Mary", "Jane"], "Age": [22, 23]})
    # a = Artifact.initialize(python_val=df, python_type=pd.DataFrame, name="flyteorg.test.yt.test1",
    # aliases=["v0.1.0"])
    a = Artifact.initialize(python_val=42, python_type=int, name="flyteorg.test.yt.test1", aliases=["v0.1.6"])
    r = FlyteRemote(
        Config.auto(config_file="/Users/ytong/.flyte/local_admin.yaml"),
        default_project="flytesnacks",
        default_domain="development",
    )
    r.create_artifact(a)
    print(a)


@pytest.mark.sandbox_test
def test_pull_artifact_and_use_to_launch():
    """
    df_artifact = Artifact("flyte://a1")
    remote.execute(wf, inputs={"a": df_artifact})
    Artifact.i
    """
    r = FlyteRemote(
        Config.auto(config_file="/Users/ytong/.flyte/local_admin.yaml"),
        default_project="flytesnacks",
        default_domain="development",
    )
    wf = r.fetch_workflow(
        "flytesnacks", "development", "cookbook.core.flyte_basics.basic_workflow.my_wf", "KVBL7dDsBdtaqjUgZIJzdQ=="
    )

    # Fetch artifact and execute workflow with it
    a = r.get_artifact(uri="flyte://av0.1/flytesnacks/development/flyteorg.test.yt.test1:v0.1.6")
    print(a)
    r.execute(wf, inputs={"a": a})

    # Just specify the artifact components
    a = Artifact(project="flytesnacks", domain="development", suffix="7438595e5c0e63613dc8df41dac5ee40")


@pytest.mark.sandbox_test
def test_artifact_query():
    str_artifact = Artifact(name="flyteorg.test.yt.teststr", aliases=["latest"])

    @task
    def base_t1() -> Annotated[str, str_artifact]:
        return "hello world"

    @workflow
    def base_wf():
        base_t1()

    @task
    def printer(a: str):
        print(f"Task 2: {a}")

    @workflow
    def user_wf(a: str = str_artifact.as_query()):
        printer(a=a)


def test_jfdjdsk():
    r = FlyteRemote(
        Config.auto(config_file="/Users/ytong/.flyte/local_admin.yaml"),
        default_project="flytesnacks",
        default_domain="development",
    )
    a = r.get_artifact(uri="flyte://av0.1/flytesnacks/development/a5zk94pb6lgg5v7l7zw8/n0/0/o:o0")
    print(a)

    wf = r.fetch_workflow(
        "flytesnacks", "development", "artifact_examples.consume_a_dataframe", "DZsIW4WlZPqKwJyRQ24SGw=="
    )
    r.execute(wf, inputs={"df": a})
