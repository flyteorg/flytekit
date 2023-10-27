import datetime
import typing
from collections import OrderedDict

import pandas as pd
import pytest
from flyteidl.artifact import artifacts_pb2
from flyteidl.core import identifier_pb2
from typing_extensions import Annotated

from flytekit.configuration import Config, Image, ImageConfig, SerializationSettings
from flytekit.core.artifact import Artifact, Inputs
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


def test_basic_option_a_rev():
    a1_t_ab = Artifact(name="my_data", partition_keys=["a", "b"], time_partitioned=True)

    @task
    def t1(
        b_value: str, dt: datetime.datetime
    ) -> Annotated[pd.DataFrame, a1_t_ab.bind_time_partition(Inputs.dt)(b=Inputs.b_value, a="manual")]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return df

    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert len(t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 3
    p = t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value
    assert p["ds"].HasField("input_binding")
    assert p["ds"].input_binding.var == "dt"
    assert p["b"].HasField("input_binding")
    assert p["b"].input_binding.var == "b_value"
    assert p["a"].HasField("static_value")
    assert p["a"].static_value == "manual"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.version == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.project == ""


def test_basic_option_a():
    a1_t_ab = Artifact(name="my_data", partition_keys=["a", "b"], time_partitioned=True)

    @task
    def t1(
        b_value: str, dt: datetime.datetime
    ) -> Annotated[pd.DataFrame, a1_t_ab(b=Inputs.b_value, a="manual").bind_time_partition(Inputs.dt)]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return df

    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert len(t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 3
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.version == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.project == ""

    a2_ab = Artifact(name="my_data2", partition_keys=["a", "b"])

    with pytest.raises(ValueError):

        @task
        def t2(b_value: str) -> Annotated[pd.DataFrame, a2_ab(a=Inputs.b_value)]:
            ...

    @task
    def t2(b_value: str) -> Annotated[pd.DataFrame, a2_ab(a=Inputs.b_value, b="manualval")]:
        ...

    entities = OrderedDict()
    t2_s = get_serializable(entities, serialization_settings, t2)
    assert len(t2_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 2
    assert t2_s.template.interface.outputs["o0"].artifact_partial_id.version == ""
    assert t2_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data2"
    assert t2_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.project == ""

    a3 = Artifact(name="my_data3")

    @task
    def t3(b_value: str) -> Annotated[pd.DataFrame, a3]:
        ...

    entities = OrderedDict()
    t3_s = get_serializable(entities, serialization_settings, t3)
    assert len(t3_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 0
    assert t3_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data3"


def test_controlling_aliases_when_running():
    task_alias = Artifact(name="task_artifact", tags=["latest"])
    wf_alias = Artifact(name="wf_artifact", tags=["my_v0.1.0"])

    @task
    def t1() -> Annotated[typing.Union[CustomReturn, Annotated[StructuredDataset, "avro"]], task_alias]:
        return CustomReturn({"name": ["Tom", "Joseph"], "age": [20, 22]})

    @workflow
    def wf() -> Annotated[CustomReturn, wf_alias]:
        u = t1()
        return u

    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, t1)
    tag = spec.template.interface.outputs["o0"].artifact_tag
    assert tag.value.static_value == "latest"

    spec = get_serializable(entities, serialization_settings, wf)
    tag = spec.template.interface.outputs["o0"].artifact_tag
    assert tag.value.static_value == "my_v0.1.0"


def test_artifact_as_promise_query():
    # when artifact is partially specified, can be used as a query input
    wf_artifact = Artifact(project="project1", domain="dev", name="wf_artifact", tags=["my_v0.1.0"])

    @task
    def t1(a: CustomReturn) -> CustomReturn:
        print(a)
        return CustomReturn({"name": ["Tom", "Joseph"], "age": [20, 22]})

    @workflow
    def wf(a: CustomReturn = wf_artifact.query()):
        u = t1(a=a)
        return u

    ctx = FlyteContextManager.current_context()
    lp = LaunchPlan.get_default_launch_plan(ctx, wf)
    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, lp)
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_tag.artifact_key.project == "project1"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_tag.artifact_key.domain == "dev"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_tag.artifact_key.name == "wf_artifact"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_tag.value.static_value == "my_v0.1.0"


def test_query_basic():
    aa = Artifact(
        name="ride_count_data",
        time_partitioned=True,
        partition_keys=["region"],
    )
    data_query = aa.query(time_partition=Inputs.dt, region=Inputs.blah)
    assert data_query.bindings == []
    assert data_query.artifact is aa
    dq_idl = data_query.to_flyte_idl()
    assert dq_idl.HasField("artifact_id")
    assert dq_idl.artifact_id.artifact_key.name == "ride_count_data"
    assert len(dq_idl.artifact_id.partitions.value) == 2
    assert dq_idl.artifact_id.partitions.value["ds"].HasField("input_binding")
    assert dq_idl.artifact_id.partitions.value["ds"].input_binding.var == "dt"
    assert dq_idl.artifact_id.partitions.value["region"].HasField("input_binding")
    assert dq_idl.artifact_id.partitions.value["region"].input_binding.var == "blah"


def test_not_specified_behavior():
    wf_artifact_no_tag = Artifact(project="project1", domain="dev", name="wf_artifact", version="1", partitions=None)
    aq = wf_artifact_no_tag.query("pr", "dom").to_flyte_idl()
    assert aq.artifact_id.HasField("partitions") is False
    assert aq.artifact_id.artifact_key.project == "pr"
    assert aq.artifact_id.artifact_key.domain == "dom"

    assert wf_artifact_no_tag.as_artifact_id.HasField("partitions") is False

    wf_artifact_no_tag = Artifact(project="project1", domain="dev", name="wf_artifact", partitions={})
    assert wf_artifact_no_tag.partitions is None
    aq = wf_artifact_no_tag.query().to_flyte_idl()
    assert aq.artifact_id.HasField("partitions") is False


def test_artifact_as_promise():
    # when the full artifact is specified, the artifact should be bindable as a literal
    wf_artifact = Artifact(project="pro", domain="dom", name="key", version="v0.1.0", partitions={"region": "LAX"})

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
    assert spec.spec.default_inputs.parameters["a"].artifact_id.artifact_key.project == "pro"
    assert spec.spec.default_inputs.parameters["a"].artifact_id.artifact_key.domain == "dom"
    assert spec.spec.default_inputs.parameters["a"].artifact_id.artifact_key.name == "key"

    aq = wf_artifact.query().to_flyte_idl()
    assert aq.artifact_id.HasField("partitions") is True
    assert aq.artifact_id.partitions.value["region"].static_value == "LAX"


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
