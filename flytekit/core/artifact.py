from __future__ import annotations

import random
import typing
from typing import Optional
from uuid import UUID

from flyteidl.artifact import artifacts_pb2
from flyteidl.core.identifier_pb2 import (
    ArtifactID,
    ArtifactKey,
    ArtifactQuery,
    ArtifactTag,
    Partitions,
    TaskExecutionIdentifier,
    WorkflowExecutionIdentifier,
)
from flyteidl.core.literals_pb2 import Literal
from flyteidl.core.types_pb2 import LiteralType

from flytekit.core.context_manager import FlyteContextManager
from flytekit.loggers import logger
from flytekit.models.literals import Literal
from flytekit.models.types import LiteralType

if typing.TYPE_CHECKING:
    from flytekit.remote.remote import FlyteRemote


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
        self.project = project
        self.domain = domain
        self.name = name
        self.version = version
        self.partitions = partitions or None  # Don't let users set empty partitions
        self.python_val = python_val
        self.python_type = python_type
        self.literal = literal
        self.literal_type = literal_type
        self.tags = tags
        self.short_description = short_description
        self.long_description = long_description
        self.source = source

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

    @property
    def artifact_id(self) -> Optional[ArtifactID]:
        if not self.project or not self.domain or not self.name or not self.version:
            return None

        return ArtifactID(
            artifact_key=ArtifactKey(
                project=self.project,
                domain=self.domain,
                name=self.name,
            ),
            version=self.version,
            partitions=self.partitions,
        )

    @classmethod
    def get(
        cls,
        uri: Optional[str],
        artifact_id: Optional[ArtifactID],
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

    def as_query(self, project: Optional[str] = None, domain: Optional[str] = None) -> ArtifactQuery:
        """
        model_artifact = Artifact(name="models.nn.lidar", tags=["latest"])
        @task
        def t1() -> Annotated[nn.Module, model_artifact]: ...

        @workflow
        def wf(model: nn.Module = model_artifact.as_query()): ...
        """
        # if (not self.project and not project) or (not self.domain and not domain):
        #     raise ValueError(f"Cannot bind artifact {self} as query, project or domain are missing")
        if not self.name:
            raise ValueError(f"Cannot bind artifact {self} as query, name is missing")
        ak = ArtifactKey(
            project=project or self.project or None,
            domain=domain or self.domain or None,
            name=self.name,
        )
        if self.tags:
            # If tags are present, assume it's a query by tag
            if len(self.tags) > 1:
                logger.warning(f"Multiple tags specified: {self.tags}, only using the first one")
            return ArtifactQuery(
                artifact_tag=ArtifactTag(
                    artifact_key=ak,
                    value=self.tags[0],
                )
            )
        else:
            # Otherwise assume it's a query by ArtifactID - keep in mind not all fields are specified, if it is, it's
            # just a fully specified get request, not a search of any kind.
            return ArtifactQuery(
                artifact_id=ArtifactID(
                    artifact_key=ak,
                    version=self.version,
                    partitions=Partitions(value=self.partitions) if self.partitions else None,
                )
            )

    def download(self):
        """
        errors if it's not an offloaded type
        """
        if not self.literal or not self.literal_type:
            raise ValueError("Literal value is missing")

        # todo: handle lists/maps
        if not self.literal.scalar.HasField("structured_dataset") and not self.literal.scalar.HasField("blob"):
            raise ValueError("Literal value is not offloaded")

        ctx = FlyteContextManager.current_context()
        lpath = ctx.file_access.get_random_local_path()
        # todo: somehow make this work for folders
        ctx.file_access.get_filesystem_for_path(self.uri).get(self.uri, lpath, recursive=False)

    def upload(self, remote: FlyteRemote) -> Artifact:
        return remote.create_artifact(self)

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
    def as_artifact_id(self) -> ArtifactID:
        if self.name is None or self.project is None or self.domain is None or self.version is None:
            raise ValueError("Cannot create artifact id without name, project, domain, version")
        return self.to_flyte_idl().artifact_id

    def to_flyte_idl(self) -> artifacts_pb2.Artifact:
        """
        Converts this object to the IDL representation.
        This is here instead of translator because it's in the interface, a relatively simple proto object
        that's exposed to the user.
        todo: where is this called besides in as_artifact_id?
        """
        return artifacts_pb2.Artifact(
            artifact_id=ArtifactID(
                artifact_key=ArtifactKey(
                    project=self.project,
                    domain=self.domain,
                    name=self.name,
                ),
                version=self.version,
                partitions=Partitions(value=self.partitions) if self.partitions else None,
            ),
            spec=artifacts_pb2.ArtifactSpec(),
            tags=self.tags,
        )

    def as_create_request(self) -> artifacts_pb2.CreateArtifactRequest:
        if not self.project or not self.domain:
            raise ValueError("Project and domain are required to create an artifact")
        name = self.name or UUID(int=random.getrandbits(128)).hex
        ak = ArtifactKey(project=self.project, domain=self.domain, name=name)

        spec = artifacts_pb2.ArtifactSpec(
            value=self.literal,
            type=self.literal_type,
        )
        partitions = self.partitions
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
            a.partitions = pb2.artifact_id.partitions.value if len(pb2.artifact_id.partitions.value) > 0 else None

        return a
