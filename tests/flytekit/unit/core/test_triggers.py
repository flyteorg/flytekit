from datetime import datetime, timedelta

from flyteidl.core import artifact_id_pb2 as art_id
from flyteidl.core import literals_pb2
from typing_extensions import Annotated

from flytekit.core.artifact import Artifact, Inputs
from flytekit.core.workflow import workflow
from flytekit.trigger import Trigger


def test_basic_11():
    # This test translates to
    # Trigger(trigger_on=[hourlyArtifact],
    #   inputs={"x": hourlyArtifact})
    hourlyArtifact = Artifact(
        name="hourly_artifact",
        time_partitioned=True,
        partition_keys=["region"],
    )
    aq_idl = hourlyArtifact.embed_as_query([hourlyArtifact])
    assert aq_idl.HasField("binding")
    assert aq_idl.binding.index == 0
    assert aq_idl.binding.partition_key == ""
    assert aq_idl.binding.bind_to_time_partition is False


def test_basic_1():
    # This test would translate to
    # Trigger(trigger_on=[hourlyArtifact],
    #   inputs={"x": hourlyArtifact.query(region="LAX")})
    # note since hourlyArtifact is time partitioned, and it has one other partition key called some_dim,
    # these should be bound to the trigger, and region should be a static value.
    hourlyArtifact = Artifact(
        name="hourly_artifact",
        time_partitioned=True,
        partition_keys=["region", "some_dim"],
    )

    aq = hourlyArtifact.query(partitions={"region": "LAX"})
    aq_idl = aq.to_flyte_idl([hourlyArtifact])
    assert aq_idl.artifact_id.time_partition.value.HasField("triggered_binding")
    assert aq_idl.artifact_id.time_partition.value.triggered_binding.index == 0
    assert aq_idl.artifact_id.time_partition.value.triggered_binding.bind_to_time_partition is True
    assert aq_idl.artifact_id.partitions.value["some_dim"].HasField("triggered_binding")
    assert aq_idl.artifact_id.partitions.value["some_dim"].triggered_binding.index == 0
    assert aq_idl.artifact_id.partitions.value["some_dim"].triggered_binding.bind_to_time_partition is False
    assert aq_idl.artifact_id.partitions.value["region"].static_value == "LAX"


def test_basic_2():
    dailyArtifact = Artifact(name="daily_artifact", time_partitioned=True)

    aq = dailyArtifact.query(time_partition=dailyArtifact.time_partition - timedelta(days=1))
    aq_idl = aq.to_flyte_idl([dailyArtifact])
    x = aq_idl.artifact_id.partitions.value
    assert len(x) == 0
    assert aq_idl.artifact_id.time_partition.value.triggered_binding.index == 0
    assert aq_idl.artifact_id.time_partition.value.triggered_binding.HasField("partition_key") is False
    assert aq_idl.artifact_id.time_partition.value.triggered_binding.bind_to_time_partition is True
    assert aq_idl.artifact_id.time_partition.value.triggered_binding.transform is not None


def test_big_trigger():
    dailyArtifact = Artifact(name="daily_artifact", time_partitioned=True)
    hourlyArtifact = Artifact(
        name="hourly_artifact",
        time_partitioned=True,
        partition_keys=["region"],
    )
    UnrelatedArtifact = Artifact(name="unrelated_artifact", time_partitioned=True)
    UnrelatedTwo = Artifact(name="unrelated_two", partition_keys=["region"])

    t = Trigger(
        # store these locally.
        trigger_on=[dailyArtifact, hourlyArtifact],
        inputs={
            # this needs to be serialized into a query.
            "today_upstream": dailyArtifact,  # this means: use the matched artifact
            "yesterday_upstream": dailyArtifact.query(time_partition=dailyArtifact.time_partition - timedelta(days=1)),
            # use the matched hourly artifact's time partition, but query on region = LAX
            # This is tricky because it's partially bound.
            "other_daily_upstream": hourlyArtifact.query(partitions={"region": "LAX"}),
            "region": "SEA",  # static value that will be passed as input
            "other_artifact": UnrelatedArtifact.query(time_partition=dailyArtifact.time_partition),
            "other_artifact_2": UnrelatedArtifact.query(time_partition=hourlyArtifact.time_partition),
            "other_artifact_3": UnrelatedTwo.query(partitions={"rgg": hourlyArtifact.partitions.region}),
        },
    )

    @workflow
    def my_workflow(
        today_upstream: str,
        yesterday_upstream: str,
        other_daily_upstream: str,
        region: str,
        other_artifact: str,
        other_artifact_2: str,
        other_artifact_3: str,
        dt: datetime,
    ) -> Annotated[str, dailyArtifact(time_partition=Inputs.dt)]:
        ...

    pm = t.get_parameter_map(my_workflow.python_interface.inputs, my_workflow.interface.inputs)

    assert pm.parameters["today_upstream"].artifact_query == art_id.ArtifactQuery(
        binding=art_id.ArtifactBindingData(
            index=0,
        ),
    )
    assert not pm.parameters["today_upstream"].artifact_query.binding.partition_key
    assert not pm.parameters["today_upstream"].artifact_query.binding.bind_to_time_partition
    assert not pm.parameters["today_upstream"].artifact_query.binding.transform

    assert pm.parameters["yesterday_upstream"].artifact_query == art_id.ArtifactQuery(
        artifact_id=art_id.ArtifactID(
            artifact_key=art_id.ArtifactKey(project=None, domain=None, name="daily_artifact"),
            time_partition=art_id.TimePartition(
                value=art_id.LabelValue(
                    triggered_binding=art_id.ArtifactBindingData(index=0, bind_to_time_partition=True, transform="-P1D")
                ),
            ),
        ),
    )

    assert pm.parameters["other_daily_upstream"].artifact_query == art_id.ArtifactQuery(
        artifact_id=art_id.ArtifactID(
            artifact_key=art_id.ArtifactKey(project=None, domain=None, name="hourly_artifact"),
            time_partition=art_id.TimePartition(
                value=art_id.LabelValue(
                    triggered_binding=art_id.ArtifactBindingData(index=1, bind_to_time_partition=True)
                )
            ),
            partitions=art_id.Partitions(
                value={
                    "region": art_id.LabelValue(static_value="LAX"),
                }
            ),
        ),
    )

    assert pm.parameters["region"].default == literals_pb2.Literal(
        scalar=literals_pb2.Scalar(primitive=literals_pb2.Primitive(string_value="SEA"))
    )

    assert pm.parameters["other_artifact"].artifact_query == art_id.ArtifactQuery(
        artifact_id=art_id.ArtifactID(
            artifact_key=art_id.ArtifactKey(project=None, domain=None, name="unrelated_artifact"),
            time_partition=art_id.TimePartition(
                value=art_id.LabelValue(
                    triggered_binding=art_id.ArtifactBindingData(index=0, bind_to_time_partition=True)
                )
            ),
        )
    )

    assert pm.parameters["other_artifact_2"].artifact_query == art_id.ArtifactQuery(
        artifact_id=art_id.ArtifactID(
            artifact_key=art_id.ArtifactKey(project=None, domain=None, name="unrelated_artifact"),
            time_partition=art_id.TimePartition(
                value=art_id.LabelValue(
                    triggered_binding=art_id.ArtifactBindingData(index=1, bind_to_time_partition=True)
                )
            ),
        )
    )

    assert pm.parameters["other_artifact_3"].artifact_query == art_id.ArtifactQuery(
        artifact_id=art_id.ArtifactID(
            artifact_key=art_id.ArtifactKey(project=None, domain=None, name="unrelated_two"),
            partitions=art_id.Partitions(
                value={
                    "rgg": art_id.LabelValue(
                        triggered_binding=art_id.ArtifactBindingData(index=1, partition_key="region")
                    ),
                }
            ),
        )
    )

    idl_t = t.to_flyte_idl()
    assert idl_t.triggers[0].HasField("time_partition")
    assert idl_t.triggers[1].HasField("time_partition")

    # Test calling it to create the LaunchPlan object which adds to the global context
    @t
    @workflow
    def tst_wf(
        today_upstream: str,
        yesterday_upstream: str,
        other_daily_upstream: str,
        region: str,
        other_artifact: str,
        other_artifact_2: str,
        other_artifact_3: str,
    ) -> str:
        ...


def test_partition_only():
    dailyArtifact = Artifact(name="daily_artifact", time_partitioned=True)

    t = Trigger(
        # store these locally.
        trigger_on=[dailyArtifact],
        inputs={
            "today_upstream": dailyArtifact.time_partition - timedelta(days=1),
        },
    )

    @workflow
    def tst_wf(
        today_upstream: str,
    ):
        ...

    pm = t.get_parameter_map(tst_wf.python_interface.inputs, tst_wf.interface.inputs)
    assert pm.parameters["today_upstream"].artifact_query == art_id.ArtifactQuery(
        binding=art_id.ArtifactBindingData(
            index=0,
            bind_to_time_partition=True,
            transform="-P1D",
        )
    )
