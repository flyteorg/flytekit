from __future__ import annotations

import datetime
import random
import typing
from datetime import timedelta
from typing import Optional, Union
from uuid import UUID

import isodate
from flyteidl.artifact import artifacts_pb2
from flyteidl.core import artifact_id_pb2 as art_id
from flyteidl.core.identifier_pb2 import TaskExecutionIdentifier, WorkflowExecutionIdentifier
from flyteidl.core.literals_pb2 import Literal
from flyteidl.core.types_pb2 import LiteralType

from flytekit.models.literals import Literal
from flytekit.models.types import LiteralType

if typing.TYPE_CHECKING:
    from flytekit.remote.remote import FlyteRemote

# Consider separated out into an independent field in the IDL before GA
#   probably worthwhile to add a format field to the date as well
#   but separating may be hard as it'll need a new element in the URI mapping.
TIME_PARTITION = "ds"


class InputsBase(object):
    """
    A class to provide better partition semantics
    Used for invoking an Artifact to bind partition keys to input values.
    If there's a good reason to use a metaclass in the future we can, but a simple instance suffices for now
    """

    def __getattr__(self, name: str) -> art_id.InputBindingData:
        return art_id.InputBindingData(var=name)


Inputs = InputsBase()


class ArtifactIDSpecification(object):
    """
    This is a special object that helps specify how Artifacts are to be created. See the comment in the
    call function of the main Artifact class. Also see the handling code in transform_variable_map for more
    information. There's a limited set of information that we ultimately need in a TypedInterface, so it
    doesn't make sense to carry the full Artifact object around. This object should be sufficient, despite
    having a pointer to the main artifact.
    """

    def __init__(
        self, a: Artifact, partitions: Optional[Partitions] = None, time_partition: Optional[TimePartition] = None
    ):
        self.artifact = a
        self.partitions = partitions
        self.time_partition = time_partition

    def __call__(self, *args, **kwargs):
        return self.bind_partitions(*args, **kwargs)

    def bind_partitions(self, *args, **kwargs) -> ArtifactIDSpecification:
        # See the parallel function in the main Artifact class for more information.
        if len(args) > 0:
            raise ValueError("Cannot set partition values by position")

        if len(kwargs) > 0:
            p = Partitions(None)
            # k is the partition key, v should be static, or an input to the task or workflow
            for k, v in kwargs.items():
                if k not in self.artifact.partition_keys:
                    raise ValueError(f"Partition key {k} not found in {self.artifact.partition_keys}")
                if isinstance(v, art_id.InputBindingData):
                    p.partitions[k] = Partition(art_id.LabelValue(input_binding=v), name=k)
                elif isinstance(v, str):
                    p.partitions[k] = Partition(art_id.LabelValue(static_value=v), name=k)
                else:
                    raise ValueError(f"Partition key {k} needs to be input binding data or static string, not {v}")
            # Given the context, shouldn't need to set further reference_artifacts.
            self.partitions = p

        return self

    def bind_time_partition(self, p: Union[datetime.datetime, art_id.InputBindingData]) -> ArtifactIDSpecification:
        # See the parallel function in the main Artifact class for more information.
        if not self.artifact.time_partitioned:
            raise ValueError("Cannot bind time partition to non-time partitioned artifact")
        if isinstance(p, datetime.datetime):
            self.time_partition = TimePartition(value=art_id.LabelValue(static_value=f"{p}"))
        else:
            self.time_partition = TimePartition(value=art_id.LabelValue(input_binding=p))
        # Given the context, shouldn't need to set further reference_artifacts.

        return self

    def to_partial_artifact_id(self) -> art_id.ArtifactID:
        # This function should only be called by transform_variable_map
        artifact_id = self.artifact.to_flyte_idl().artifact_id
        # Use the partitions from this object, but replacement is not allowed by protobuf, so generate new object
        if self.partitions:
            p = self.partitions.to_flyte_idl(self.time_partition)
        elif self.time_partition:
            # Use an empty partitions object to avoid re-implementing to_flyte_idl
            p = Partitions(None).to_flyte_idl(self.time_partition)
        else:
            p = None
        if self.artifact.partition_keys:
            required = len(self.artifact.partition_keys)
            required += 1 if self.artifact.time_partitioned else 0
            fulfilled = len(p.value) if p else 0
            if required != fulfilled:
                raise ValueError(
                    f"Artifact {artifact_id.artifact_key} requires {required} partitions, but only {fulfilled} are bound."
                )
        artifact_id = art_id.ArtifactID(
            artifact_key=artifact_id.artifact_key,
            partitions=p,
            version=artifact_id.version,
        )
        return artifact_id


class ArtifactBindingData(object):
    """
    We need some way of linking the triggered artifacts, with the parameter map. This object represents that link.

    These are used in two places in triggers. If the input to a trigger's target launchplan is the whole artifact,
    then this binding should just have the index in the list.
    If the input is an ArtifactQuery, then the query can reference one of these objects to dereference information
    to be used in the search.
    """

    def __init__(self, triggered_artifact_id: int, partition_key: str, transform: Optional[str] = None):
        self.triggered_artifact_id = triggered_artifact_id
        self.partition_key = partition_key
        self.transform = transform

    def to_flyte_idl(self) -> art_id.ArtifactBindingData:
        return art_id.ArtifactBindingData(
            index=self.triggered_artifact_id,
            partition_key=self.partition_key,
            transform=self.transform,
        )


class ArtifactQuery(object):
    def __init__(
        self,
        artifact: Artifact,
        name: str,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        time_partition: Optional[TimePartition] = None,
        partitions: Optional[Partitions] = None,
        tag: Optional[str] = None,
    ):
        if not name:
            raise ValueError(f"Cannot create query without name")
        if partitions and partitions.partitions and TIME_PARTITION in partitions.partitions:
            raise ValueError(f"Cannot use 'ds' as a partition name, just use time partition")

        # So normally, if you just do MyData.query(partitions={"region": "{{ inputs.region }}"}), it will just
        # use the input value to fill in the partition. But if you do
        #   MyData.query(partitions={"region": OtherArtifact.partitions.region })
        # then you now have a dependency on the other artifact. This list keeps track of all the other Artifacts you've
        # referenced.
        # Note that this is only used for Triggers.
        self.artifact = artifact
        bindings: typing.List[Artifact] = []
        if time_partition:
            if time_partition.reference_artifact and time_partition.reference_artifact is not artifact:
                bindings.append(time_partition.reference_artifact)
        if partitions and partitions.partitions:
            for k, v in partitions.partitions.items():
                if v.reference_artifact and v.reference_artifact is not artifact:
                    bindings.append(v.reference_artifact)

        self.name = name
        self.project = project
        self.domain = domain
        self.time_partition = time_partition
        self.partitions = partitions
        self.tag = tag
        self.bindings = bindings

    def to_flyte_idl(
        self,
        bindings: Optional[typing.List[Artifact]] = None,
    ) -> art_id.ArtifactQuery:
        """
        Think input keys can be removed
        """

        ak = art_id.ArtifactKey(
            name=self.name,
            project=self.project,
            domain=self.domain,
        )
        # If there's a tag, it takes precedence over other current options
        if self.tag:
            aq = art_id.ArtifactQuery(
                artifact_tag=art_id.ArtifactTag(artifact_key=ak, value=art_id.LabelValue(static_value=self.tag)),
            )
            return aq

        temp_partitions = self.partitions or Partitions(None)
        partition_idl = temp_partitions.to_flyte_idl(self.time_partition, bindings)

        i = art_id.ArtifactID(
            artifact_key=ak,
            partitions=partition_idl,
        )

        aq = art_id.ArtifactQuery(
            artifact_id=i,
        )

        return aq

    @staticmethod
    def from_uri(uri: str) -> ArtifactQuery:
        ...

    def as_uri(self) -> str:
        ...


class TimePartition(object):
    def __init__(
        self,
        value: Union[art_id.LabelValue, art_id.InputBindingData, str, datetime.datetime],
        op: Optional[str] = None,
        other: Optional[timedelta] = None,
    ):
        if isinstance(value, str):
            value = art_id.LabelValue(static_value=value)
        elif isinstance(value, datetime.datetime):
            value = art_id.LabelValue(static_value=f"{value}")
        elif isinstance(value, art_id.InputBindingData):
            value = art_id.LabelValue(input_binding=value)
        # else should already be a LabelValue
        self.value = value
        self.op = op
        self.other = other
        self.reference_artifact = None

    def __add__(self, other: timedelta) -> TimePartition:
        tp = TimePartition(self.value, op="+", other=other)
        tp.reference_artifact = self.reference_artifact
        return tp

    def __sub__(self, other: timedelta) -> TimePartition:
        tp = TimePartition(self.value, op="-", other=other)
        tp.reference_artifact = self.reference_artifact
        return tp

    def truncate_to_day(self):
        # raise NotImplementedError("Not implemented yet")
        return self


class Partition(object):
    def __init__(self, value: art_id.LabelValue, name: str = None):
        self.name = name
        self.value = value
        self.reference_artifact: Optional[Artifact] = None


class Partitions(object):
    def __init__(self, partitions: Optional[typing.Dict[str, Union[str, art_id.InputBindingData, Partition]]]):
        self._partitions = {}
        if partitions:
            for k, v in partitions.items():
                if isinstance(v, Partition):
                    self._partitions[k] = v
                elif isinstance(v, art_id.InputBindingData):
                    self._partitions[k] = Partition(art_id.LabelValue(input_binding=v), name=k)
                else:
                    self._partitions[k] = Partition(art_id.LabelValue(static_value=v), name=k)
        self.reference_artifact = None

    @property
    def partitions(self) -> Optional[typing.Dict[str, Partition]]:
        return self._partitions

    def set_reference_artifact(self, artifact: Artifact):
        self.reference_artifact = artifact
        if self.partitions:
            for p in self.partitions.values():
                p.reference_artifact = artifact

    def __getattr__(self, item):
        if self.partitions and item in self.partitions:
            return self.partitions[item]
        raise AttributeError(f"Partition {item} not found in {self}")

    def get_idl_partitions_for_trigger(
        self,
        time_partition: Optional[TimePartition],
        bindings: Optional[typing.List[Artifact]] = None,
    ) -> Optional[art_id.Partitions]:
        p = {}
        # First create partition requirements for all the partitions
        # if self.reference_artifact and self.reference_artifact in bindings:
        #     idx = bindings.index(self.reference_artifact)
        #     triggering_artifact_id = bindings[idx]
        #     if triggering_artifact_id.artifact_id.partitions and triggering_artifact_id.artifact_id.partitions.value:
        #         for k, v in triggering_artifact_id.artifact_id.partitions.value.items():
        #             p[k] = art_id.LabelValue(
        #                 triggered_binding=art_id.ArtifactBindingData(
        #                     index=idx,
        #                     partition_key=k,
        #                 )
        #             )

        if self.partitions:
            for k, v in self.partitions.items():
                if not v.reference_artifact or (
                    v.reference_artifact
                    and v.reference_artifact is self.reference_artifact
                    and not v.reference_artifact in bindings
                ):
                    # consider changing condition to just check for static value
                    p[k] = art_id.LabelValue(static_value=v.value.static_value)
                elif v.reference_artifact in bindings:
                    # This line here is why the PartitionValue object has a name field.
                    # We might bind to a partition key that's a different name than the k here.
                    p[k] = art_id.LabelValue(
                        triggered_binding=art_id.ArtifactBindingData(
                            index=bindings.index(v.reference_artifact),
                            partition_key=v.name,
                        )
                    )
                else:
                    raise ValueError(f"Partition has unhandled reference artifact {v.reference_artifact}")

        if time_partition:
            if not time_partition.reference_artifact or (
                time_partition.reference_artifact is self.reference_artifact
                and not time_partition.reference_artifact in bindings
            ):
                p[TIME_PARTITION] = art_id.LabelValue(static_value=time_partition.expr)
            elif time_partition.reference_artifact in bindings:
                transform = None
                if time_partition.op and time_partition.other and isinstance(time_partition.other, timedelta):
                    transform = str(time_partition.op) + isodate.duration_isoformat(time_partition.other)
                p[TIME_PARTITION] = art_id.LabelValue(
                    triggered_binding=art_id.ArtifactBindingData(
                        index=bindings.index(time_partition.reference_artifact),
                        partition_key=TIME_PARTITION,
                        transform=transform,
                    )
                )
            else:
                raise ValueError(f"Partition values has unhandled reference artifact {time_partition}")
        return art_id.Partitions(value=p)


    def to_flyte_idl(
        self,
        time_partition: Optional[TimePartition],
        bindings: Optional[typing.List[Artifact]] = None,
    ) -> Optional[art_id.Partitions]:
        # This is basically a flag, which indicates that we are serializing this object within the context of a Trigger
        # If we are not, then we are just serializing normally
        if bindings and len(bindings) > 0:
            return self.get_idl_partitions_for_trigger(time_partition, bindings)

        if not self.partitions and not time_partition:
            return None

        pp = {}
        if self.partitions:
            for k, v in self.partitions.items():
                pp[k] = v.value
        pp.update({TIME_PARTITION: time_partition.value} if time_partition else {})
        return art_id.Partitions(value=pp)


class Artifact(object):
    """
    An Artifact is effectively just a metadata layer on top of data that exists in Flyte. Most data of interest
    will be the output of tasks and workflows. The other class is user uploads.

    This Python class has two purposes - as a Python representation of a materialized Artifact,
    and as a way for users to specify that tasks/workflows create Artifacts and the manner
    in which they are created.

    Python fields will be missing when retrieved from the service.

    Use one as input to workflow (only workflow for now)
    df_artifact = Artifact.get("flyte://a1")
    remote.execute(wf, inputs={"a": df_artifact})

    Control creation parameters at task/workflow execution time ::

        @task
        def t1() -> Annotated[nn.Module, Artifact(name="my.artifact.name",
                              tags=["latest", "1.0.0"])]:
            ...
    """

    def __init__(
        self,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        time_partitioned: bool = False,
        time_partition: Optional[TimePartition] = None,
        partition_keys: Optional[typing.List[str]] = None,
        partitions: Optional[Union[Partitions, typing.Dict[str, str]]] = None,
        tags: Optional[typing.List[str]] = None,
        python_val: Optional[typing.Any] = None,
        python_type: Optional[typing.Type] = None,
        literal: Optional[Literal] = None,
        literal_type: Optional[LiteralType] = None,
        short_description: Optional[str] = None,
        source: Optional[typing.Union[WorkflowExecutionIdentifier, TaskExecutionIdentifier, str]] = None,
    ):
        """

        :param project: Should not be directly user provided, the project/domain will come from the project/domain of
           the execution that produced the output. These values will be filled in automatically when retrieving however.
        :param domain: See above.
        :param name: The name of the Artifact. This should be user provided.
        :param version: Version of the Artifact, typically the execution ID, plus some additional entropy.
           Not user provided.
        :param time_partitioned: Whether or not this Artifact will have a time partition.
        :param partition_keys: This is a list of keys that will be used to partition the Artifact. These are not the
           values. Values are set via a () on the artifact and will end up in the partition_values field.
        :param partitions: This is a dictionary of partition keys to values.
        :param tags: A list of tags that can be used as shortcuts to this Artifact. A tag targets one particular
           partition if your Artifact is partitioned.
        :param python_val: The Python value.
        :param python_type: The Python type.
        :param literal: The Literal value from the output.
        :param literal_type: The LiteralType as taken from the task/workflow that produced the Artifact
        :param short_description: A description of the Artifact.
        TODO: Additional fields to come: figure out sources, and also add metadata (cards).
        """
        if partition_keys and TIME_PARTITION in partition_keys:
            # See note on TIME_PARTITION above. If we can figure out a nice uri syntax for time partition then remove
            raise ValueError("cannot use 'ds' as a partition name, just use time partitions")

        self.project = project
        self.domain = domain
        self.name = name
        self.version = version
        self.time_partitioned = time_partitioned
        self._time_partition = None
        if time_partition:
            self._time_partition = time_partition
            self._time_partition.reference_artifact = self
        self.partition_keys = partition_keys
        self._partitions = None
        if partitions:
            if isinstance(partitions, dict):
                self._partitions = Partitions(partitions)
                self.partition_keys = list(partitions.keys())
            else:
                self._partitions = partitions
            self._partitions.set_reference_artifact(self)
        self.python_val = python_val
        self.python_type = python_type
        self.literal = literal
        self.literal_type = literal_type
        self.tags = tags
        self.short_description = short_description
        self.source = source

    def __call__(self, *args, **kwargs) -> ArtifactIDSpecification:
        """
        This __call__ should only ever happen in the context of a task or workflow's output, to be
        used in an Annotated[] call. The other styles will go through different call functions.
        """
        # Can't guarantee the order in which time/non-time partitions are bound so create the helper
        # object and invoke the function there.
        partial_id = ArtifactIDSpecification(self)
        return partial_id.bind_partitions(*args, **kwargs)

    def bind_time_partition(self, p: Union[datetime.datetime, art_id.InputBindingData]) -> ArtifactIDSpecification:
        """
        This function should only ever be called in the context of a task or workflow's output, to be
        used in an Annotated[] call. The other styles will go through different call functions.
        """
        # Can't guarantee the order in which time/non-time partitions are bound so create the helper
        # object and invoke the function there.
        partial_id = ArtifactIDSpecification(self)
        return partial_id.bind_time_partition(p)

    @property
    def partitions(self) -> Optional[Partitions]:
        return self._partitions

    @property
    def time_partition(self) -> Optional[TimePartition]:
        if not self._time_partition and self.time_partitioned:
            self._time_partition = TimePartition()
        return self._time_partition

    def __str__(self):
        return (
            f"Artifact: project={self.project}, domain={self.domain}, name={self.name}, version={self.version}\n"
            f"  name={self.name}\n"
            f"  partitions={self.partitions}\n"
            f"  tags={self.tags}\n"
            f"  literal_type={self.literal_type}, literal={self.literal})"
        )

    def __repr__(self):
        return self.__str__()

    @classmethod
    def get(
        cls,
        uri: Optional[str],
        artifact_id: Optional[idl.ArtifactID],
        remote: FlyteRemote,
        get_details: bool = False,
    ) -> Optional[Artifact]:
        """
        Use one locally. This retrieves the Literal.
        a = remote.get("flyte://blah")
        a = Artifact.get("flyte://blah", remote, tag="latest")
        u = union.get("union://blah")
        """
        return remote.get_artifact(uri=uri, artifact_id=artifact_id, get_details=get_details)

    def query(
        self,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        time_partition: Optional[Union[datetime.datetime, TimePartition, art_id.InputBindingData]] = None,
        partitions: Optional[Union[typing.Dict[str, str], Partitions]] = None,
        tag: Optional[str] = None,
        **kwargs,
    ) -> ArtifactQuery:
        if self.partition_keys:
            fn_args = {"project", "domain", "time_partition", "partitions", "tag"}
            k = set(self.partition_keys)
            if len(fn_args & k) > 0:
                raise ValueError(
                    f"There are conflicting partition key names {fn_args ^ k}, please rename"
                    f" use a partitions object"
                )
        if partitions and kwargs:
            raise ValueError("Please either specify kwargs or a partitions object not both")

        if kwargs:
            partitions = Partitions(kwargs)
            partitions.reference_artifact = self  # only set top level

        if partitions and isinstance(partitions, dict):
            partitions = Partitions(partitions)
            partitions.reference_artifact = self  # only set top level

        time_partition = (
            TimePartition(time_partition) if time_partition and not isinstance(time_partition, TimePartition) else None
        )

        aq = ArtifactQuery(
            artifact=self,
            name=self.name,
            project=project or self.project or None,
            domain=domain or self.domain or None,
            time_partition=time_partition or self.time_partition,
            partitions=partitions or self.partitions,
            tag=tag or self.tags[0] if self.tags else None,
        )
        return aq

    @classmethod
    def initialize(
        cls,
        python_val: typing.Any,
        python_type: typing.Type,
        name: Optional[str] = None,
        literal_type: Optional[LiteralType] = None,
        tags: Optional[typing.List[str]] = None,
    ) -> Artifact:
        """
        Use this for when you have a Python value you want to get an Artifact object out of.

        This function readies an Artifact for creation, it doesn't actually create it just yet since this is a
        network-less call. You will need to persist it with a FlyteRemote instance:
            remote.create_artifact(Artifact.initialize(...))

        Artifact.initialize("/path/to/file", tags={"tag1": "val1"})
        Artifact.initialize("/path/to/parquet", type=pd.DataFrame, tags=["0.1.0"])

        What's set here is everything that isn't set by the server. What is set by the server?
        - name, version, if not set by user.
        - uri
        Set by remote
        - project, domain
        """
        # Create the artifact object
        return Artifact(
            python_val=python_val,
            python_type=python_type,
            literal_type=literal_type,
            tags=tags,
            name=name,
        )

    # todo: merge this later with the as_artifact_id property
    @property
    def artifact_id(self) -> Optional[art_id.ArtifactID]:
        if not self.project or not self.domain or not self.name or not self.version:
            return None

        return self.to_flyte_idl().artifact_id

    @property
    def as_artifact_id(self) -> art_id.ArtifactID:
        if self.name is None or self.project is None or self.domain is None or self.version is None:
            raise ValueError("Cannot create artifact id without name, project, domain, version")
        return self.to_flyte_idl().artifact_id

    def embed_as_query(
        self, bindings: typing.List[Artifact]) -> art_id.ArtifactQuery:
        """
        This should only be called in the context of a Trigger
        :param bindings: The list of artifacts in trigger_on
        :param partition: Can embed a time partition
        :param expr: Only valid if there's a time partition.
        """
        # Find self in the list, raises ValueError if not there.
        idx = bindings.index(self)
        aq = art_id.ArtifactQuery(
            binding=art_id.ArtifactBindingData(
                index=idx,
            )
        )
        return aq

    def to_flyte_idl(self) -> artifacts_pb2.Artifact:
        """
        Converts this object to the IDL representation.
        This is here instead of translator because it's in the interface, a relatively simple proto object
        that's exposed to the user.
        """
        if self.partitions:
            p = self.partitions.to_flyte_idl(self.time_partition)
        elif self.time_partition:
            p = Partitions(None).to_flyte_idl(self.time_partition)
        else:
            p = None
        return artifacts_pb2.Artifact(
            artifact_id=art_id.ArtifactID(
                artifact_key=art_id.ArtifactKey(
                    project=self.project,
                    domain=self.domain,
                    name=self.name,
                ),
                version=self.version,
                partitions=p,
            ),
            spec=artifacts_pb2.ArtifactSpec(),
            tags=self.tags,
        )

    def as_create_request(self) -> artifacts_pb2.CreateArtifactRequest:
        if not self.project or not self.domain:
            raise ValueError("Project and domain are required to create an artifact")
        name = self.name or UUID(int=random.getrandbits(128)).hex
        ak = art_id.ArtifactKey(project=self.project, domain=self.domain, name=name)

        spec = artifacts_pb2.ArtifactSpec(
            value=self.literal,
            type=self.literal_type,
        )
        partitions = self.partitions.to_flyte_idl(self.time_partition)
        tag = self.tags[0] if self.tags else None
        return artifacts_pb2.CreateArtifactRequest(artifact_key=ak, spec=spec, partitions=partitions, tag=tag)

    @classmethod
    def from_flyte_idl(cls, pb2: artifacts_pb2.Artifact) -> Artifact:
        """
        Converts the IDL representation to this object.
        """
        tags = [t for t in pb2.tags] if pb2.tags else None
        a = Artifact(
            project=pb2.artifact_id.artifact_key.project,
            domain=pb2.artifact_id.artifact_key.domain,
            name=pb2.artifact_id.artifact_key.name,
            version=pb2.artifact_id.version,
            tags=tags,
            literal_type=LiteralType.from_flyte_idl(pb2.spec.type),
            literal=Literal.from_flyte_idl(pb2.spec.value),
            # source=pb2.spec.source,  # todo: source isn't installed in artifact service yet
        )
        if pb2.artifact_id.HasField("partitions"):
            if len(pb2.artifact_id.partitions.value) > 0:
                # static values should be the only ones set since currently we don't from_flyte_idl
                # anything that's not a materialized artifact.
                if TIME_PARTITION in pb2.artifact_id.partitions.value:
                    a._time_partition = TimePartition(pb2.artifact_id.partitions.value[TIME_PARTITION].static_value)
                    a._time_partition.reference_artifact = a

                a._partitions = Partitions(
                    partitions={
                        k: Partition(value=v, name=k)
                        for k, v in pb2.artifact_id.partitions.value.items()
                        if k != TIME_PARTITION
                    }
                )
                a.partitions.reference_artifact = a

        return a
