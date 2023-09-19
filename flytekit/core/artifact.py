from __future__ import annotations

import random
import re
import typing
from datetime import timedelta
from typing import Optional, Union
from uuid import UUID

import isodate
from flyteidl.artifact import artifacts_pb2
from flyteidl.core import identifier_pb2 as idl
from flyteidl.core.identifier_pb2 import TaskExecutionIdentifier, WorkflowExecutionIdentifier
from flyteidl.core.literals_pb2 import Literal
from flyteidl.core.types_pb2 import LiteralType

from flytekit.loggers import logger
from flytekit.models.literals import Literal
from flytekit.models.types import LiteralType

if typing.TYPE_CHECKING:
    from flytekit.remote.remote import FlyteRemote

# TODO: This needs to be separated out into an independent field in the IDL before GA
#   probably worthwhile to add a format field to the date as well
TIME_PARTITION = "ds"
input_pattern = r"{{\s*inputs\.([\w_]+)\s*}}"


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

    def to_flyte_idl(self) -> idl.ArtifactBindingData:
        return idl.ArtifactBindingData(
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
        self, bindings: Optional[typing.List[Artifact]] = None, input_keys: Optional[typing.List[str]] = None
    ) -> idl.ArtifactQuery:

        ak = idl.ArtifactKey(
            name=self.name,
            project=self.project,
            domain=self.domain,
        )

        temp_partitions = self.partitions or Partitions(None)
        # temp_partitions.idl(query_bindings=self.bindings, fulfilled=bindings, input_keys=input_keys)
        # there's the list of what this query needs, what it has to fulfill it, and other inputs that can be used.
        # logic is.
        #  - if you need something that isn't bound, then fail.
        #  -
        partition_idl = temp_partitions.to_flyte_idl(self.time_partition, bindings, input_keys)

        i = idl.ArtifactID(
            artifact_key=ak,
            partitions=partition_idl,
        )

        aq = idl.ArtifactQuery(
            artifact_id=i,
            artifact_tag=idl.ArtifactTag(artifact_key=ak, value=self.tag) if self.tag else None,
        )

        return aq


class TimePartition(object):
    def __init__(self, expr: str, op: Optional[str] = None, other: Optional[timedelta] = None):
        self.expr = expr
        matches = re.match(input_pattern, expr)
        self.op = op
        self.other = other
        if matches:
            self.input_arg = matches.groups()[0]
            logger.debug(f"time partition matched input {self.input_arg}")
        self.reference_artifact = None

    def __add__(self, other: timedelta) -> TimePartition:
        tp = TimePartition(self.expr, op="+", other=other)
        tp.reference_artifact = self.reference_artifact
        return tp

    def __sub__(self, other: timedelta) -> TimePartition:
        tp = TimePartition(self.expr, op="-", other=other)
        tp.reference_artifact = self.reference_artifact
        return tp

    def truncate_to_day(self):
        # raise NotImplementedError("Not implemented yet")
        return self


class Partition(object):
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value
        self.reference_artifact: Optional[Artifact] = None


class Partitions(object):
    def __init__(self, partitions: Optional[typing.Dict[str, Union[str, Partition]]]):
        self._partitions = {}
        if partitions:
            for k, v in partitions.items():
                if isinstance(v, Partition):
                    self._partitions[k] = v
                else:
                    self._partitions[k] = Partition(k, v)
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

    def to_flyte_idl(
        self,
        time_partition: Optional[TimePartition],
        bindings: Optional[typing.List[Artifact]] = None,
        input_keys: Optional[typing.List[str]] = None,
    ) -> Optional[idl.Partitions]:
        if not self.partitions and not time_partition:
            return None
        # This is basically a flag, which indicates that we are serializing this object within the context of a Trigger
        if bindings and len(bindings) > 0:
            p = {}
            if self.partitions:
                for k, v in self.partitions.items():
                    if not v.reference_artifact or (
                        v.reference_artifact
                        and v.reference_artifact is self.reference_artifact
                        and not v.reference_artifact in bindings
                    ):
                        p[k] = idl.PartitionValue(static_value=v.value)
                    elif v.reference_artifact in bindings:
                        p[k] = idl.PartitionValue(
                            binding=idl.ArtifactBindingData(
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
                    p[TIME_PARTITION] = idl.PartitionValue(static_value=time_partition.expr)
                elif time_partition.reference_artifact in bindings:
                    transform = None
                    if time_partition.op and time_partition.other and isinstance(time_partition.other, timedelta):
                        transform = str(time_partition.op) + isodate.duration_isoformat(time_partition.other)
                    p[TIME_PARTITION] = idl.PartitionValue(
                        binding=idl.ArtifactBindingData(
                            index=bindings.index(time_partition.reference_artifact),
                            partition_key=TIME_PARTITION,
                            transform=transform,
                        )
                    )
                else:
                    raise ValueError(f"Partition values has unhandled reference artifact {time_partition}")
            return idl.Partitions(value=p)

        # If we are not, then we are just serializing normally
        pp = {}
        if self.partitions:
            for k, v in self.partitions.items():
                pp[k] = idl.PartitionValue(static_value=v.value)
        pp.update({TIME_PARTITION: idl.PartitionValue(static_value=time_partition.expr)} if time_partition else {})
        return idl.Partitions(value=pp)


class Artifact(object):
    """
    Artifact depends on three things, literal type, source, format

    Use one as input to workflow (only workflow for now)
    df_artifact = Artifact("flyte://a1")
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
        time_partition: Optional[str] = None,
        with_time_partition: Optional[bool] = None,
        partitions: Optional[typing.Dict[str, str]] = None,
        tags: Optional[typing.List[str]] = None,
        python_val: Optional[typing.Any] = None,
        python_type: Optional[typing.Type] = None,
        literal: Optional[Literal] = None,
        literal_type: Optional[LiteralType] = None,
        short_description: Optional[str] = None,
        long_description: Optional[str] = None,
        source: Optional[typing.Union[WorkflowExecutionIdentifier, TaskExecutionIdentifier, str]] = None,
    ):
        """
        Constructor used when instantiating something from the Artifact service.
        Can convert to dataclass in the future.
        Python fields will be missing when retrieved from the service.

        :param project:
        :param domain:
        :param name: The name of the Artifact.
        :param version: Version of the Artifact, typically the execution ID.
        """
        if partitions and TIME_PARTITION in partitions:
            raise ValueError("cannot use 'ds' as a partition name, just use time partitions")

        self.project = project
        self.domain = domain
        self.name = name
        self.version = version
        self.with_time_partition = with_time_partition
        self._time_partition = None
        if time_partition:
            self._time_partition = TimePartition(time_partition)
            self._time_partition.reference_artifact = self
        self._partitions = Partitions(partitions or None)
        self._partitions.set_reference_artifact(self)
        self.python_val = python_val
        self.python_type = python_type
        self.literal = literal
        self.literal_type = literal_type
        self.tags = tags
        self.short_description = short_description
        self.long_description = long_description
        self.source = source

    @property
    def time_partition(self) -> Optional[TimePartition]:
        return self._time_partition

    @property
    def partitions(self) -> Partitions:
        return self._partitions

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
        time_partition: Optional[Union[str, TimePartition]] = None,
        partitions: Optional[Union[typing.Dict[str, str], Partitions]] = None,
        tag: Optional[str] = None,
    ) -> ArtifactQuery:
        if isinstance(partitions, dict):
            partitions = Partitions(partitions)
            partitions.reference_artifact = self  # only set top level

        if isinstance(time_partition, str):
            time_partition = TimePartition(time_partition)

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

    @property
    def artifact_id(self) -> Optional[idl.ArtifactID]:
        # todo: merge this later with the as_artifact_id property
        if not self.project or not self.domain or not self.name or not self.version:
            return None

        return self.to_flyte_idl().artifact_id

    @property
    def as_artifact_id(self) -> idl.ArtifactID:
        if self.name is None or self.project is None or self.domain is None or self.version is None:
            raise ValueError("Cannot create artifact id without name, project, domain, version")
        return self.to_flyte_idl().artifact_id

    def embed_as_query(
        self, bindings: typing.List[Artifact], partition: Optional[str], expr: Optional[str]
    ) -> idl.ArtifactQuery:
        """
        This should only be called in the context of a Trigger
        :param bindings: The list of artifacts in trigger_on
        :param partition: Can embed a time partition
        :param expr: Only valid if there's a time partition.
        """
        # Find self in the list, raises ValueError if not there.
        idx = bindings.index(self)
        aq = idl.ArtifactQuery(
            binding=idl.ArtifactBindingData(
                index=idx,
                partition_key=partition,
                transform=str(expr) if expr and partition else None,
            )
        )
        return aq

    def to_flyte_idl(self) -> artifacts_pb2.Artifact:
        """
        Converts this object to the IDL representation.
        This is here instead of translator because it's in the interface, a relatively simple proto object
        that's exposed to the user.
        """
        return artifacts_pb2.Artifact(
            artifact_id=idl.ArtifactID(
                artifact_key=idl.ArtifactKey(
                    project=self.project,
                    domain=self.domain,
                    name=self.name,
                ),
                version=self.version,
                partitions=self.partitions.to_flyte_idl(self.time_partition),
            ),
            spec=artifacts_pb2.ArtifactSpec(),
            tags=self.tags,
        )

    def as_create_request(self) -> artifacts_pb2.CreateArtifactRequest:
        if not self.project or not self.domain:
            raise ValueError("Project and domain are required to create an artifact")
        name = self.name or UUID(int=random.getrandbits(128)).hex
        ak = idl.ArtifactKey(project=self.project, domain=self.domain, name=name)

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
                if TIME_PARTITION in pb2.artifact_id.partitions.value:
                    a._time_partition = TimePartition(pb2.artifact_id.partitions.value[TIME_PARTITION].static_value)
                    a._time_partition.reference_artifact = a

                a._partitions = Partitions(
                    partitions={
                        k: Partition(name=k, value=v.static_value)
                        for k, v in pb2.artifact_id.partitions.value.items()
                        if k != TIME_PARTITION
                    }
                )
                a.partitions.reference_artifact = a

        return a
