import datetime
import sys
import typing
from base64 import b64decode
from collections import OrderedDict
from dataclasses import dataclass

import pytest
from flyteidl.core import artifact_id_pb2 as art_id
from flyteidl.core.artifact_id_pb2 import Granularity
from google.protobuf.timestamp_pb2 import Timestamp
from typing_extensions import Annotated, get_args

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.array_node_map_task import map_task
from flytekit.core.artifact import Artifact, Inputs, TimePartition
from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteContextManager, OutputMetadataTracker
from flytekit.core.interface import detect_artifact
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.task import task
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import workflow
from flytekit.exceptions.user import FlyteValidationException
from flytekit.tools.translator import get_serializable

if "pandas" not in sys.modules:
    pytest.skip(reason="Requires pandas", allow_module_level=True)

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


@dataclass
class TestObject(object):
    text: str
    prefix: str

    def serialize_to_string(self, ctx: FlyteContext, variable_name: str) -> typing.Tuple[str, str]:
        return f"{self.prefix}_meta{variable_name}", self.text


class CustomReturn(object):
    def __init__(self, data):
        self.data = data


def test_basic_option_a_rev():
    import pandas as pd

    a1_t_ab = Artifact(name="my_data", partition_keys=["a", "b"], time_partitioned=True)

    @task
    def t1(
        b_value: str, dt: datetime.datetime
    ) -> Annotated[pd.DataFrame, a1_t_ab(time_partition=Inputs.dt, b=Inputs.b_value, a="manual")]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return df

    assert a1_t_ab.time_partition.granularity == Granularity.DAY
    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert len(t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 2
    p = t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition is not None
    assert (
        t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition.granularity == art_id.Granularity.DAY
    )
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition.value.input_binding.var == "dt"
    assert p["b"].HasField("input_binding")
    assert p["b"].input_binding.var == "b_value"
    assert p["a"].HasField("static_value")
    assert p["a"].static_value == "manual"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.version == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.project == ""


def test_basic_multiple_call():
    import pandas as pd

    a1_t_ab = Artifact(name="my_data", partition_keys=["a", "b"], time_partitioned=True)

    @task
    def t1(
        b_value: str, dt: datetime.datetime
    ) -> Annotated[pd.DataFrame, a1_t_ab(b=Inputs.b_value)(time_partition=Inputs.dt)(a="manual")]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return df

    assert a1_t_ab.time_partition.granularity == Granularity.DAY
    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert len(t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 2
    p = t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition is not None
    assert (
        t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition.granularity == art_id.Granularity.DAY
    )
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition.value.input_binding.var == "dt"
    assert p["b"].HasField("input_binding")
    assert p["b"].input_binding.var == "b_value"
    assert p["a"].HasField("static_value")
    assert p["a"].static_value == "manual"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.version == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.project == ""


def test_args_getting():
    a1 = Artifact(name="argstst")
    a1_called = a1()
    x = Annotated[int, a1_called]
    gotten = get_args(x)
    assert len(gotten) == 2
    assert gotten[1] is a1_called
    detected = detect_artifact(get_args(int))
    assert detected is None
    detected = detect_artifact(get_args(x))
    assert detected == a1_called.to_partial_artifact_id()


def test_basic_option_no_tp():
    import pandas as pd

    a1_t_ab = Artifact(name="my_data", partition_keys=["a", "b"])
    assert not a1_t_ab.time_partitioned

    # trying to bind to a time partition when not so raises an error.
    with pytest.raises(ValueError):

        @task
        def t1x(
            b_value: str, dt: datetime.datetime
        ) -> Annotated[pd.DataFrame, a1_t_ab(time_partition=Inputs.dt, b=Inputs.b_value, a="manual")]:
            df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
            return df

    @task
    def t1(b_value: str, dt: datetime.datetime) -> Annotated[pd.DataFrame, a1_t_ab(b=Inputs.b_value, a="manual")]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return df

    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert len(t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 2
    p = t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.HasField("time_partition") is False
    assert p["b"].HasField("input_binding")


def test_basic_option_hardcoded_tp():
    a1_t_ab = Artifact(name="my_data", time_partitioned=True)

    dt = datetime.datetime.strptime("04/05/2063", "%m/%d/%Y")

    id_spec = a1_t_ab(time_partition=dt)
    assert id_spec.partitions is None
    assert id_spec.time_partition.value.HasField("time_value")


def test_bound_ness():
    a1_a = Artifact(name="my_data", partition_keys=["a"])
    q = a1_a.query()
    assert not q.bound

    q = a1_a.query(a="hi")
    assert q.bound


def test_bound_ness_time():
    a1_t = Artifact(name="my_data", time_partitioned=True)
    q = a1_t.query()
    assert not q.bound

    q = a1_t.query(time_partition=Inputs.dt)
    assert q.bound


def test_basic_option_a():
    import pandas as pd

    a1_t_ab = Artifact(name="my_data", partition_keys=["a", "b"], time_partitioned=True)

    @task
    def t1(
        b_value: str, dt: datetime.datetime
    ) -> Annotated[pd.DataFrame, a1_t_ab(b=Inputs.b_value, a="const", time_partition=Inputs.dt)]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return df

    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert len(t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 2
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.version == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.project == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition is not None


def test_basic_dynamic():
    import pandas as pd

    ctx = FlyteContextManager.current_context()
    # without this omt, the part that keeps track of dynamic partitions doesn't kick in.
    omt = OutputMetadataTracker()
    ctx = ctx.with_output_metadata_tracker(omt).build()

    a1_t_ab = Artifact(
        name="my_data", partition_keys=["a", "b"], time_partitioned=True, time_partition_granularity=Granularity.MONTH
    )

    @task
    def t1(b_value: str, dt: datetime.datetime) -> Annotated[pd.DataFrame, a1_t_ab(b=Inputs.b_value)]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return a1_t_ab.create_from(df, a="dynamic!", time_partition=dt)

    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert len(t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 2
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value["a"].HasField("runtime_binding")
    assert (
        not t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value["b"].HasField("runtime_binding")
    )
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.version == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.project == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition is not None

    d = datetime.datetime(2021, 1, 1, 0, 0)
    lm = TypeEngine.dict_to_literal_map(ctx, {"b_value": "my b value", "dt": d})
    lm_outputs = t1.dispatch_execute(ctx, lm)
    dyn_partition_encoded = lm_outputs.literals["o0"].metadata["_uap"]
    artifact_id = art_id.ArtifactID()
    artifact_id.ParseFromString(b64decode(dyn_partition_encoded.encode("utf-8")))
    assert artifact_id.partitions.value["a"].static_value == "dynamic!"

    proto_timestamp = Timestamp()
    proto_timestamp.FromDatetime(d)
    assert artifact_id.time_partition.value.time_value == proto_timestamp
    assert artifact_id.time_partition.granularity == Granularity.MONTH


def test_basic_dynamic_only_time():
    # This test is to ensure the metadata tracking component works if the user only binds a time at run time.
    import pandas as pd

    ctx = FlyteContextManager.current_context()
    # without this omt, the part that keeps track of dynamic partitions doesn't kick in.
    omt = OutputMetadataTracker()
    ctx = ctx.with_output_metadata_tracker(omt).build()

    a1_t = Artifact(name="my_data", time_partitioned=True)

    @task
    def t1(b_value: str, dt: datetime.datetime) -> Annotated[pd.DataFrame, a1_t]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return a1_t.create_from(df, time_partition=dt)

    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert not t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition is not None

    d = datetime.datetime(2021, 1, 1, 0, 0)
    lm = TypeEngine.dict_to_literal_map(ctx, {"b_value": "my b value", "dt": d})
    lm_outputs = t1.dispatch_execute(ctx, lm)
    dyn_partition_encoded = lm_outputs.literals["o0"].metadata["_uap"]
    artifact_id = art_id.ArtifactID()
    artifact_id.ParseFromString(b64decode(dyn_partition_encoded.encode("utf-8")))
    assert not artifact_id.partitions.value

    proto_timestamp = Timestamp()
    proto_timestamp.FromDatetime(d)
    assert artifact_id.time_partition.value.time_value == proto_timestamp


def test_dynamic_with_extras():
    import pandas as pd

    ctx = FlyteContextManager.current_context()
    # without this omt, the part that keeps track of dynamic partitions doesn't kick in.
    omt = OutputMetadataTracker()
    ctx = ctx.with_output_metadata_tracker(omt).build()

    a1_t_ab = Artifact(name="my_data", partition_keys=["a", "b"], time_partitioned=True)

    @task
    def t1(b_value: str, dt: datetime.datetime) -> Annotated[pd.DataFrame, a1_t_ab(b=Inputs.b_value)]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        to1 = TestObject("this is extra information", "p1")
        to2 = TestObject("this is more extra information", "p2")
        return a1_t_ab.create_from(df, to1, to2, a="dynamic!", time_partition=dt)

    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert len(t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 2
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value["a"].HasField("runtime_binding")
    assert (
        not t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value["b"].HasField("runtime_binding")
    )
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.version == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.project == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.time_partition is not None

    d = datetime.datetime(2021, 1, 1, 0, 0)
    lm = TypeEngine.dict_to_literal_map(ctx, {"b_value": "my b value", "dt": d})
    lm_outputs = t1.dispatch_execute(ctx, lm)
    o0 = lm_outputs.literals["o0"]
    dyn_partition_encoded = o0.metadata["_uap"]
    artifact_id = art_id.ArtifactID()
    artifact_id.ParseFromString(b64decode(dyn_partition_encoded.encode("utf-8")))
    assert artifact_id.partitions.value["a"].static_value == "dynamic!"

    proto_timestamp = Timestamp()
    proto_timestamp.FromDatetime(d)
    assert artifact_id.time_partition.value.time_value == proto_timestamp
    assert o0.metadata["p1_metao0"] == "this is extra information"
    assert o0.metadata["p2_metao0"] == "this is more extra information"


def test_basic_option_a3():
    import pandas as pd

    a3 = Artifact(name="my_data3")

    @task
    def t3(b_value: str) -> Annotated[pd.DataFrame, a3]:
        return pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})

    entities = OrderedDict()
    t3_s = get_serializable(entities, serialization_settings, t3)
    assert len(t3_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 0
    assert t3_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data3"


def test_query_basic():
    aa = Artifact(
        name="ride_count_data",
        time_partitioned=True,
        partition_keys=["region"],
    )
    data_query = aa.query(time_partition=Inputs.dt, region=Inputs.blah)
    assert data_query.binding is None
    assert data_query.artifact is aa
    dq_idl = data_query.to_flyte_idl()
    assert dq_idl.HasField("artifact_id")
    assert dq_idl.artifact_id.artifact_key.name == "ride_count_data"
    assert len(dq_idl.artifact_id.partitions.value) == 1
    assert dq_idl.artifact_id.partitions.value["region"].HasField("input_binding")
    assert dq_idl.artifact_id.partitions.value["region"].input_binding.var == "blah"
    assert dq_idl.artifact_id.time_partition.value.input_binding.var == "dt"


def test_not_specified_behavior():
    wf_artifact_no_tag = Artifact(project="project1", domain="dev", name="wf_artifact", version="1", partitions=None)
    aq = wf_artifact_no_tag.query("pr", "dom").to_flyte_idl()
    assert aq.artifact_id.HasField("partitions") is False
    assert aq.artifact_id.artifact_key.project == "pr"
    assert aq.artifact_id.artifact_key.domain == "dom"

    assert wf_artifact_no_tag.concrete_artifact_id.HasField("partitions") is False

    wf_artifact_no_tag = Artifact(project="project1", domain="dev", name="wf_artifact", partitions={})
    assert wf_artifact_no_tag.partitions is None
    aq = wf_artifact_no_tag.query().to_flyte_idl()
    assert aq.artifact_id.HasField("partitions") is False
    assert aq.artifact_id.HasField("time_partition") is False


def test_artifact_as_promise_query():
    # when artifact is partially specified, can be used as a query input
    wf_artifact = Artifact(project="project1", domain="dev", name="wf_artifact")

    @task
    def t1(a: CustomReturn) -> CustomReturn:
        return CustomReturn({"name": ["Tom", "Joseph"], "age": [20, 22]})

    @workflow
    def wf(a: CustomReturn = wf_artifact.query()):
        u = t1(a=a)
        return u

    ctx = FlyteContextManager.current_context()
    lp = LaunchPlan.get_default_launch_plan(ctx, wf)
    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, lp)
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_id.artifact_key.project == "project1"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_id.artifact_key.domain == "dev"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_id.artifact_key.name == "wf_artifact"

    # Test non-specified query for unpartitioned artifacts
    @workflow
    def wf2(a: CustomReturn = wf_artifact):
        u = t1(a=a)
        return u

    lp2 = LaunchPlan.get_default_launch_plan(ctx, wf2)
    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, lp2)
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_id.artifact_key.project == "project1"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_id.artifact_key.domain == "dev"
    assert spec.spec.default_inputs.parameters["a"].artifact_query.artifact_id.artifact_key.name == "wf_artifact"


def test_artifact_as_promise():
    # when the full artifact is specified, the artifact should be bindable as a literal
    wf_artifact = Artifact(project="pro", domain="dom", name="key", version="v0.1.0", partitions={"region": "LAX"})

    @task
    def t1(a: CustomReturn) -> CustomReturn:
        print(a)
        return CustomReturn({"name": ["Tom", "Joseph"], "age": [20, 22]})

    @workflow
    def wf3(a: CustomReturn = wf_artifact):
        u = t1(a=a)
        return u

    ctx = FlyteContextManager.current_context()
    lp = LaunchPlan.get_default_launch_plan(ctx, wf3)
    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, lp)
    assert spec.spec.default_inputs.parameters["a"].artifact_id.artifact_key.project == "pro"
    assert spec.spec.default_inputs.parameters["a"].artifact_id.artifact_key.domain == "dom"
    assert spec.spec.default_inputs.parameters["a"].artifact_id.artifact_key.name == "key"

    aq = wf_artifact.query().to_flyte_idl()
    assert aq.artifact_id.HasField("partitions") is True
    assert aq.artifact_id.partitions.value["region"].static_value == "LAX"


def test_query_basic_query_bindings():
    # Note these bindings don't really work yet.
    aa = Artifact(
        name="ride_count_data",
        time_partitioned=True,
        partition_keys=["region"],
    )
    bb = Artifact(
        name="driver_data",
        time_partitioned=True,
        partition_keys=["region"],
    )
    cc = Artifact(
        name="passenger_data",
        time_partitioned=True,
        partition_keys=["region"],
    )
    aa.query(time_partition=Inputs.dt, region=bb.partitions.region)
    with pytest.raises(ValueError):
        aa.query(time_partition=cc.time_partition, region=bb.partitions.region)


def test_partition_none():
    # confirm that we can distinguish between partitions being set to empty, and not being set
    # though this is not currently used.
    ak = art_id.ArtifactKey(project="p", domain="d", name="name")
    no_partition = art_id.ArtifactID(artifact_key=ak, version="without_p")
    assert not no_partition.HasField("partitions")

    p = art_id.Partitions()
    with_partition = art_id.ArtifactID(artifact_key=ak, version="without_p", partitions=p)
    assert with_partition.HasField("partitions")


def test_as_artf_no_partitions():
    int_artf = Artifact(name="important_int")

    @task
    def greet(day_of_week: str, number: int, am: bool) -> str:
        greeting = "Have a great " + day_of_week + " "
        greeting += "morning" if am else "evening"
        return greeting + "!" * number

    @workflow
    def go_greet(day_of_week: str, number: int = int_artf.query(), am: bool = False) -> str:
        return greet(day_of_week=day_of_week, number=number, am=am)

    tst_lp = LaunchPlan.create(
        "morning_lp",
        go_greet,
        fixed_inputs={"am": True},
        default_inputs={"day_of_week": "monday"},
    )

    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, tst_lp)
    aq = spec.spec.default_inputs.parameters["number"].artifact_query
    assert aq.artifact_id.artifact_key.name == "important_int"
    assert not aq.artifact_id.HasField("partitions")
    assert not aq.artifact_id.HasField("time_partition")


def test_check_input_binding():
    import pandas as pd

    a1_t_ab = Artifact(name="my_data", partition_keys=["a", "b"], time_partitioned=True)

    with pytest.raises(FlyteValidationException):

        @task
        def t1(
            b_value: str, dt: datetime.datetime
        ) -> Annotated[pd.DataFrame, a1_t_ab(time_partition=Inputs.dt, b=Inputs.xyz, a="manual")]:
            df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
            return df

    with pytest.raises(FlyteValidationException):

        @task
        def t2(
            b_value: str, dt: datetime.datetime
        ) -> Annotated[pd.DataFrame, a1_t_ab(time_partition=Inputs.dtt, b=Inputs.b_value, a="manual")]:
            df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
            return df

    with pytest.raises(ValueError):
        Artifact(partition_keys=["a", "b"], time_partitioned=True)


def test_dynamic_input_binding():
    a1_t_ab = Artifact(name="my_data", partition_keys=["a", "b"], time_partitioned=True)

    @task
    def t1(b_value: str, dt: datetime.datetime) -> Annotated[int, a1_t_ab(time_partition=Inputs.dt, a="manual")]:
        i = 3
        return a1_t_ab.create_from(i, b="dynamic string")

    # dynamic bindings for partition values in workflows is not allowed.
    with pytest.raises(FlyteValidationException):

        @workflow
        def wf1(b_value: str, dt: datetime.datetime) -> Annotated[int, a1_t_ab(time_partition=Inputs.dt, a="manual")]:
            return 3


def test_tp_granularity():
    a1_t_b = Artifact(
        name="my_data", partition_keys=["b"], time_partition_granularity=Granularity.MONTH, time_partitioned=True
    )
    assert a1_t_b.time_partition.granularity == Granularity.MONTH

    @task
    def t1(b_value: str, dt: datetime.datetime) -> Annotated[int, a1_t_b(b=Inputs.b_value)(time_partition=Inputs.dt)]:
        return 5

    entities = OrderedDict()
    spec = get_serializable(entities, serialization_settings, t1)
    assert (
        spec.template.interface.outputs["o0"].artifact_partial_id.time_partition.granularity == art_id.Granularity.MONTH
    )


def test_map_doesnt_add_any_metadata():
    # The base task only looks for items in the metadata tracker at the top level. This test is here to maintain
    # that state for now, though we may want to revisit this.
    import pandas as pd

    ctx = FlyteContextManager.current_context()
    # without this omt, the part that keeps track of dynamic partitions doesn't kick in.
    omt = OutputMetadataTracker()
    ctx = (
        ctx.with_output_metadata_tracker(omt)
        .with_execution_state(ctx.execution_state.with_params(mode=ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION))
        .build()
    )

    a1_b = Artifact(name="my_data", partition_keys=["b"])

    @task
    def t1(b_value: str) -> Annotated[pd.DataFrame, a1_b]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return a1_b.create_from(df, b="dynamic!")

    mt1 = map_task(t1)
    entities = OrderedDict()
    mt1_s = get_serializable(entities, serialization_settings, mt1)
    o0 = mt1_s.template.interface.outputs["o0"]
    assert not o0.artifact_partial_id
    lm = TypeEngine.dict_to_literal_map(
        ctx, {"b_value": ["my b value 1", "my b value 2"]}, type_hints={"b_value": typing.List[str]}
    )
    lm_outputs = mt1.dispatch_execute(ctx, lm)
    coll = lm_outputs.literals["o0"].collection.literals
    assert not coll[0].metadata
    assert not coll[1].metadata


def test_tp_math():
    a = Artifact(name="test artifact", time_partitioned=True)
    d = datetime.datetime(2063, 4, 5, 0, 0)
    pt = Timestamp()
    pt.FromDatetime(d)
    tp = TimePartition(value=art_id.LabelValue(time_value=pt), granularity=Granularity.HOUR)
    tp.reference_artifact = a
    tp2 = tp + datetime.timedelta(days=1)
    assert tp2.op == art_id.Operator.PLUS
    assert tp2.other == datetime.timedelta(days=1)
    assert tp2.granularity == Granularity.HOUR
    assert tp2 is not tp

    tp = TimePartition(value=art_id.LabelValue(time_value=pt), granularity=Granularity.HOUR)
    tp.reference_artifact = a
    tp2 = tp - datetime.timedelta(days=1)
    assert tp2.op == art_id.Operator.MINUS
    assert tp2.other == datetime.timedelta(days=1)
    assert tp2.granularity == Granularity.HOUR
    assert tp2 is not tp


def test_lims():
    # test an artifact with 11 partition keys
    with pytest.raises(ValueError):
        Artifact(name="test artifact", time_partitioned=True, partition_keys=[f"key_{i}" for i in range(11)])
