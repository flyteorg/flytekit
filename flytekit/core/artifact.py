from __future__ import annotations

import typing
from typing import Optional

from flytekit.loggers import logger
from flyteidl.artifact import artifacts_pb2 as artifact_idl
from flyteidl.core.identifier_pb2 import ArtifactID, ArtifactKey, TaskExecutionIdentifier, WorkflowExecutionIdentifier
from flyteidl.core.literals_pb2 import Literal
from flyteidl.core.types_pb2 import LiteralType

from flytekit.core.context_manager import FlyteContextManager

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
        def t1() -> Annotated[nn.Module, Artifact(name="my.artifact.name", tags={type: "validation"},
                              aliases={"version": "latest", "semver": "1.0.0"})]:
            ...

    """

    def __init__(
        self,
        uri: Optional[str] = None,
        project: Optional[str] = None,
        domain: Optional[str] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        python_val: Optional[typing.Any] = None,
        python_type: Optional[typing.Type] = None,
        literal: Optional[Literal] = None,
        literal_type: Optional[LiteralType] = None,
        tags: Optional[typing.Dict[str, str]] = None,
        aliases: Optional[typing.Dict[str, str]] = None,
        short_description: Optional[str] = None,
        long_description: Optional[str] = None,
        source: Optional[typing.Union[WorkflowExecutionIdentifier, TaskExecutionIdentifier, str]] = None,
    ):
        """
        Constructor used when instantiating something from the Artifact service.
        Can convert to dataclass in the future.
        Python fields will be missing when retrieved from the service.
        """
        self.uri = uri
        self.project = project
        self.domain = domain
        self.name = name
        self.version = version
        self.python_val = python_val
        self.python_type = python_type
        self.literal = literal
        self.literal_type = literal_type
        self.tags = tags
        self.aliases = aliases
        self.short_description = short_description
        self.long_description = long_description
        self.source = source

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
        )

    @classmethod
    def get(
        cls,
        uri: Optional[str],
        artifact_id: Optional[artifact_idl.ArtifactID],
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

    def as_query(self) -> artifact_idl.ArtifactQuery:
        """
        @task
        def t1() -> Artifact[nn.Module, name="models.nn.lidar", alias="latest", overwrite_alias=True]: ...

        @workflow
        def wf(model: nn.Module = Artifact.get_query(name="models.nn.lidar", alias="latest")): ...
        """
        return artifact_idl.ArtifactQuery(
            artifact_key=ArtifactKey(
                project=self.project,
                domain=self.domain,
                name=self.name,
            ),
            version=self.version,
            # todo: just get the first one for now, and skip tags
            alias=[artifact_idl.Alias(key=k, value=v) for k, v in self.aliases.items()][0] if self.aliases else None,
        )

    @classmethod
    def search(cls, query: artifact_idl.ArtifactQuery, remote: FlyteRemote) -> Optional[Artifact]:
        ...

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
        version: Optional[str] = None,
        literal_type: Optional[LiteralType] = None,
        tags: Optional[typing.Dict[str, str]] = None,
        aliases: Optional[typing.Dict[str, str]] = None,
    ) -> Artifact:
        """
        Use this for when you have a Python value you want to get an Artifact object out of.

        This function readies an Artifact for creation, it doesn't actually create it just yet since this is a
        network-less call. You will need to persist it with a FlyteRemote instance:
            remote.create_artifact(Artifact.initialize(...))

        Artifact.initialize("/path/to/file", tags={"tag1": "val1"})
        Artifact.initialize("/path/to/parquet", type=pd.DataFrame, aliases={"ver": "0.1.0"})

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
            aliases=aliases,
            name=name,
            version=version,
        )

    def to_flyte_idl(self) -> artifact_idl.Artifact:
        """
        Converts this object to the IDL representation.
        This is here instead of translator because it's in the interface, a relatively simple proto object
        that's exposed to the user.
        """
        return artifact_idl.Artifact(
            artifact_id=ArtifactID(
                artifact_key=ArtifactKey(
                    project=self.project,
                    domain=self.domain,
                    name=self.name,
                ),
                version=self.version,
            ),
            uri=self.uri,
            spec=artifact_idl.ArtifactSpec(
                tags=[artifact_idl.Tag(key=k, value=v) for k, v in self.tags.items()] if self.tags else None,
                aliases=[artifact_idl.Alias(key=k, value=v) for k, v in self.aliases.items()] if self.aliases else None,
            ),
        )
