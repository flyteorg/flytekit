import pytest
from flyteidl.artifact import artifacts_pb2
from flyteidl.core import identifier_pb2
from typing_extensions import Annotated

from flytekit.configuration import Config, Image, ImageConfig, SerializationSettings
from flytekit.core.artifact import Artifact
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.remote.remote import FlyteRemote

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


# These test are not updated yet


@pytest.mark.sandbox_test
def test_create_an_artifact33_locally():
    import grpc
    from flyteidl.artifact.artifacts_pb2_grpc import ArtifactRegistryStub

    local_artifact_channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = ArtifactRegistryStub(local_artifact_channel)
    ak = identifier_pb2.ArtifactKey(project="flytesnacks", domain="development", name="f3bea14ee52f8409eb5b/n0/0/o/o0")
    ai = identifier_pb2.ArtifactID(artifact_key=ak)
    req = artifacts_pb2.GetArtifactRequest(query=identifier_pb2.ArtifactQuery(artifact_id=ai))
    x = stub.GetArtifact(req)
    print(x)


@pytest.mark.sandbox_test
def test_create_an_artifact_locally():
    import pandas as pd
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


@pytest.mark.sandbox_test
def test_get_and_run():
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
