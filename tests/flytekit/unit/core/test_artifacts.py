import datetime
import typing
from collections import OrderedDict

import pandas as pd
import pytest
from typing_extensions import Annotated

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.artifact import Artifact, Inputs
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.task import task
from flytekit.core.workflow import workflow
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
    ) -> Annotated[pd.DataFrame, a1_t_ab(time_partition=Inputs.dt, b=Inputs.b_value, a="manual")]:
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
    ) -> Annotated[pd.DataFrame, a1_t_ab(b=Inputs.b_value, a="manual", time_partition=Inputs.dt)]:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [b_value, b_value, b_value]})
        return df

    entities = OrderedDict()
    t1_s = get_serializable(entities, serialization_settings, t1)
    assert len(t1_s.template.interface.outputs["o0"].artifact_partial_id.partitions.value) == 3
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.version == ""
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.name == "my_data"
    assert t1_s.template.interface.outputs["o0"].artifact_partial_id.artifact_key.project == ""


def test_basic_option_a2():
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


def test_basic_option_a3():
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
