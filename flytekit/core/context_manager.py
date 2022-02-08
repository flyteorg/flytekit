"""

.. autoclass:: flytekit.core.context_manager::ExecutionState.Mode
   :noindex:
.. autoclass:: flytekit.core.context_manager::ExecutionState.Mode.TASK_EXECUTION
   :noindex:
.. autoclass:: flytekit.core.context_manager::ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION
   :noindex:
.. autoclass:: flytekit.core.context_manager::ExecutionState.Mode.LOCAL_TASK_EXECUTION
   :noindex:

"""

from __future__ import annotations

import datetime as _datetime
import logging
import logging as _logging
import os
import pathlib
import re
import tempfile
import traceback
import typing
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Union

from docker_image import reference

from flytekit.clients import friendly as friendly_client  # noqa
from flytekit.configuration import images, internal
from flytekit.configuration import sdk as _sdk_config
from flytekit.configuration import secrets
from flytekit.core import mock_stats, utils
from flytekit.core.checkpointer import Checkpoint, SyncCheckpoint
from flytekit.core.data_persistence import FileAccessProvider, default_local_file_access_provider
from flytekit.core.node import Node
from flytekit.interfaces.cli_identifiers import WorkflowExecutionIdentifier
from flytekit.interfaces.stats import taggable
from flytekit.models.core import identifier as _identifier

# TODO: resolve circular import from flytekit.core.python_auto_container import TaskResolverMixin

# Enables static type checking https://docs.python.org/3/library/typing.html#typing.TYPE_CHECKING
if typing.TYPE_CHECKING:
    from flytekit.core.base_task import TaskResolverMixin

_DEFAULT_FLYTEKIT_ENTRYPOINT_FILELOC = "bin/entrypoint.py"


@dataclass(init=True, repr=True, eq=True, frozen=True)
class Image(object):
    """
    Image is a structured wrapper for task container images used in object serialization.

    Attributes:
        name (str): A user-provided name to identify this image.
        fqn (str): Fully qualified image name. This consists of
            #. a registry location
            #. a username
            #. a repository name
            For example: `hostname/username/reponame`
        tag (str): Optional tag used to specify which version of an image to pull
    """

    name: str
    fqn: str
    tag: str

    @property
    def full(self) -> str:
        """ "
        Return the full image name with tag.
        """
        return f"{self.fqn}:{self.tag}"


@dataclass(init=True, repr=True, eq=True, frozen=True)
class ImageConfig(object):
    """
    ImageConfig holds available images which can be used at registration time. A default image can be specified
    along with optional additional images. Each image in the config must have a unique name.

    Attributes:
        default_image (str): The default image to be used as a container for task serialization.
        images (List[Image]): Optional, additional images which can be used in task container definitions.
    """

    default_image: Optional[Image] = None
    images: Optional[List[Image]] = None

    def find_image(self, name) -> Optional[Image]:
        """
        Return an image, by name, if it exists.
        """
        lookup_images = self.images + [self.default_image] if self.images else [self.default_image]
        for i in lookup_images:
            if i.name == name:
                return i
        return None


_IMAGE_FQN_TAG_REGEX = re.compile(r"([^:]+)(?=:.+)?")


def look_up_image_info(name: str, tag: str, optional_tag: bool = False) -> Image:
    """
    Looks up the image tag from environment variable (should be set from the Dockerfile).
        FLYTE_INTERNAL_IMAGE should be the environment variable.

    This function is used when registering tasks/workflows with Admin.
    When using the canonical Python-based development cycle, the version that is used to register workflows
    and tasks with Admin should be the version of the image itself, which should ideally be something unique
    like the sha of the latest commit.

    :param optional_tag:
    :param name:
    :param Text tag: e.g. somedocker.com/myimage:someversion123
    :rtype: Text
    """
    ref = reference.Reference.parse(tag)
    if not optional_tag and ref["tag"] is None:
        raise AssertionError(f"Incorrectly formatted image {tag}, missing tag value")
    else:
        return Image(name=name, fqn=ref["name"], tag=ref["tag"])


def get_image_config(img_name: Optional[str] = None) -> ImageConfig:
    image_name = img_name if img_name else internal.IMAGE.get()
    default_img = look_up_image_info("default", image_name) if image_name is not None and image_name != "" else None
    other_images = [look_up_image_info(k, tag=v, optional_tag=True) for k, v in images.get_specified_images().items()]
    other_images.append(default_img)
    return ImageConfig(default_image=default_img, images=other_images)


class ExecutionParameters(object):
    """
    This is a run-time user-centric context object that is accessible to every @task method. It can be accessed using

    .. code-block:: python

        flytekit.current_context()

    This object provides the following
    * a statsd handler
    * a logging handler
    * the execution ID as an :py:class:`flytekit.models.core.identifier.WorkflowExecutionIdentifier` object
    * a working directory for the user to write arbitrary files to

    Please do not confuse this object with the :py:class:`flytekit.FlyteContext` object.
    """

    @dataclass(init=False)
    class Builder(object):
        stats: taggable.TaggableStats
        execution_date: datetime
        logging: _logging
        execution_id: str
        attrs: typing.Dict[str, typing.Any]
        working_dir: typing.Union[os.PathLike, utils.AutoDeletingTempDir]
        checkpoint: typing.Optional[Checkpoint]
        raw_output_prefix: str

        def __init__(self, current: typing.Optional[ExecutionParameters] = None):
            self.stats = current.stats if current else None
            self.execution_date = current.execution_date if current else None
            self.working_dir = current.working_directory if current else None
            self.execution_id = current.execution_id if current else None
            self.logging = current.logging if current else None
            self.checkpoint = current._checkpoint if current else None
            self.attrs = current._attrs if current else {}
            self.raw_output_prefix = current.raw_output_prefix if current else None

        def add_attr(self, key: str, v: typing.Any) -> ExecutionParameters.Builder:
            self.attrs[key] = v
            return self

        def build(self) -> ExecutionParameters:
            if not isinstance(self.working_dir, utils.AutoDeletingTempDir):
                pathlib.Path(self.working_dir).mkdir(parents=True, exist_ok=True)
            return ExecutionParameters(
                execution_date=self.execution_date,
                stats=self.stats,
                tmp_dir=self.working_dir,
                execution_id=self.execution_id,
                logging=self.logging,
                checkpoint=self.checkpoint,
                raw_output_prefix=self.raw_output_prefix,
                **self.attrs,
            )

    @staticmethod
    def new_builder(current: ExecutionParameters = None) -> Builder:
        return ExecutionParameters.Builder(current=current)

    def with_task_sandbox(self) -> Builder:
        prefix = self.working_directory
        if isinstance(self.working_directory, utils.AutoDeletingTempDir):
            prefix = self.working_directory.name
        task_sandbox_dir = tempfile.mkdtemp(prefix=prefix)
        p = pathlib.Path(task_sandbox_dir)
        cp_dir = p.joinpath("__cp")
        cp_dir.mkdir(exist_ok=True)
        cp = SyncCheckpoint(checkpoint_dest=str(cp_dir))
        b = self.new_builder(self)
        b.checkpoint = cp
        b.working_dir = task_sandbox_dir
        return b

    def builder(self) -> Builder:
        return ExecutionParameters.Builder(current=self)

    def __init__(
        self, execution_date, tmp_dir, stats, execution_id, logging, raw_output_prefix, checkpoint=None, **kwargs
    ):
        """
        Args:
            execution_date: Date when the execution is running
            tmp_dir: temporary directory for the execution
            stats: handle to emit stats
            execution_id: Identifier for the xecution
            logging: handle to logging
            checkpoint: Checkpoint Handle to the configured checkpoint system
        """
        self._stats = stats
        self._execution_date = execution_date
        self._working_directory = tmp_dir
        self._execution_id = execution_id
        self._logging = logging
        self._raw_output_prefix = raw_output_prefix
        # AutoDeletingTempDir's should be used with a with block, which creates upon entry
        self._attrs = kwargs
        # It is safe to recreate the Secrets Manager
        self._secrets_manager = SecretsManager()
        self._checkpoint = checkpoint

    @property
    def stats(self) -> taggable.TaggableStats:
        """
        A handle to a special statsd object that provides usefully tagged stats.
        TODO: Usage examples and better comments
        """
        return self._stats

    @property
    def logging(self) -> _logging:
        """
        A handle to a useful logging object.
        TODO: Usage examples
        """
        return self._logging

    @property
    def raw_output_prefix(self) -> str:
        return self._raw_output_prefix

    @property
    def working_directory(self) -> utils.AutoDeletingTempDir:
        """
        A handle to a special working directory for easily producing temporary files.

        TODO: Usage examples
        TODO: This does not always return a AutoDeletingTempDir
        """
        return self._working_directory

    @property
    def execution_date(self) -> datetime:
        """
        This is a datetime representing the time at which a workflow was started.  This is consistent across all tasks
        executed in a workflow or sub-workflow.

        .. note::

            Do NOT use this execution_date to drive any production logic.  It might be useful as a tag for data to help
            in debugging.
        """
        return self._execution_date

    @property
    def execution_id(self) -> str:
        """
        This is the identifier of the workflow execution within the underlying engine.  It will be consistent across all
        task executions in a workflow or sub-workflow execution.

        .. note::

            Do NOT use this execution_id to drive any production logic.  This execution ID should only be used as a tag
            on output data to link back to the workflow run that created it.
        """
        return self._execution_id

    @property
    def secrets(self) -> SecretsManager:
        return self._secrets_manager

    @property
    def checkpoint(self) -> Checkpoint:
        if self._checkpoint is None:
            raise NotImplementedError("Checkpointing is not available, please check the version of the platform.")
        return self._checkpoint

    def __getattr__(self, attr_name: str) -> typing.Any:
        """
        This houses certain task specific context. For example in Spark, it houses the SparkSession, etc
        """
        attr_name = attr_name.upper()
        if self._attrs and attr_name in self._attrs:
            return self._attrs[attr_name]
        raise AssertionError(f"{attr_name} not available as a parameter in Flyte context - are you in right task-type?")

    def has_attr(self, attr_name: str) -> bool:
        attr_name = attr_name.upper()
        if self._attrs and attr_name in self._attrs:
            return True
        return False

    def get(self, key: str) -> typing.Any:
        """
        Returns task specific context if present else raise an error. The returned context will match the key
        """
        return self.__getattr__(attr_name=key)


class SecretsManager(object):
    """
    This provides a secrets resolution logic at runtime.
    The resolution order is
      - Try env var first. The env var should have the configuration.SECRETS_ENV_PREFIX. The env var will be all upper
         cased
      - If not then try the file where the name matches lower case
        ``configuration.SECRETS_DEFAULT_DIR/<group>/configuration.SECRETS_FILE_PREFIX<key>``

    All configuration values can always be overridden by injecting an environment variable
    """

    class _GroupSecrets(object):
        """
        This is a dummy class whose sole purpose is to support "attribute" style lookup for secrets
        """

        def __init__(self, group: str, sm: typing.Any):
            self._group = group
            self._sm = sm

        def __getattr__(self, item: str) -> str:
            """
            Returns the secret that matches "group"."key"
            the key, here is the item
            """
            return self._sm.get(self._group, item)

    def __init__(self):
        self._base_dir = str(secrets.SECRETS_DEFAULT_DIR.get()).strip()
        self._file_prefix = str(secrets.SECRETS_FILE_PREFIX.get()).strip()
        self._env_prefix = str(secrets.SECRETS_ENV_PREFIX.get()).strip()

    def __getattr__(self, item: str) -> _GroupSecrets:
        """
        returns a new _GroupSecrets objects, that allows all keys within this group to be looked up like attributes
        """
        return self._GroupSecrets(item, self)

    def get(self, group: str, key: str) -> str:
        """
        Retrieves a secret using the resolution order -> Env followed by file. If not found raises a ValueError
        """
        self.check_group_key(group, key)
        env_var = self.get_secrets_env_var(group, key)
        fpath = self.get_secrets_file(group, key)
        v = os.environ.get(env_var)
        if v is not None:
            return v
        if os.path.exists(fpath):
            with open(fpath, "r") as f:
                return f.read().strip()
        raise ValueError(
            f"Unable to find secret for key {key} in group {group} " f"in Env Var:{env_var} and FilePath: {fpath}"
        )

    def get_secrets_env_var(self, group: str, key: str) -> str:
        """
        Returns a string that matches the ENV Variable to look for the secrets
        """
        self.check_group_key(group, key)
        return f"{self._env_prefix}{group.upper()}_{key.upper()}"

    def get_secrets_file(self, group: str, key: str) -> str:
        """
        Returns a path that matches the file to look for the secrets
        """
        self.check_group_key(group, key)
        return os.path.join(self._base_dir, group.lower(), f"{self._file_prefix}{key.lower()}")

    @staticmethod
    def check_group_key(group: str, key: str):
        if group is None or group == "":
            raise ValueError("secrets group is a mandatory field.")
        if key is None or key == "":
            raise ValueError("secrets key is a mandatory field.")


@dataclass
class EntrypointSettings(object):
    """
    This object carries information about the command, path and version of the entrypoint program that will be invoked
    to execute tasks at runtime.
    """

    path: Optional[str] = None
    command: Optional[str] = None
    version: int = 0


@dataclass
class FastSerializationSettings(object):
    """
    This object hold information about settings necessary to serialize an object so that it can be fast-registered.
    """

    enabled: bool = False
    # This is the location that the code should be copied into.
    destination_dir: Optional[str] = None

    # This is the zip file where the new code was uploaded to.
    distribution_location: Optional[str] = None


@dataclass(frozen=True)
class SerializationSettings(object):
    """
    These settings are provided while serializing a workflow and task, before registration. This is required to get
    runtime information at serialization time, as well as some defaults.

    Attributes:
        project (str): The project (if any) with which to register entities under.
        domain (str): The domain (if any) with which to register entities under.
        version (str): The version (if any) with which to register entities under.
        image_config (ImageConfig): The image config used to define task container images.
        env (Optional[Dict[str, str]]): Environment variables injected into task container definitions.
        flytekit_virtualenv_root (Optional[str]):  During out of container serialize the absolute path of the flytekit
            virtualenv at serialization time won't match the in-container value at execution time. This optional value
            is used to provide the in-container virtualenv path
        python_interpreter (Optional[str]): The python executable to use. This is used for spark tasks in out of
            container execution.
        entrypoint_settings (Optional[EntrypointSettings]): Information about the command, path and version of the
            entrypoint program.
        fast_serialization_settings (Optional[FastSerializationSettings]): If the code is being serialized so that it
            can be fast registered (and thus omit building a Docker image) this object contains additional parameters
            for serialization.
    """

    project: str
    domain: str
    version: str
    image_config: ImageConfig
    env: Optional[Dict[str, str]] = None
    flytekit_virtualenv_root: Optional[str] = None
    python_interpreter: Optional[str] = None
    entrypoint_settings: Optional[EntrypointSettings] = None
    fast_serialization_settings: Optional[FastSerializationSettings] = None

    @dataclass
    class Builder(object):
        project: str
        domain: str
        version: str
        image_config: ImageConfig
        env: Optional[Dict[str, str]] = None
        flytekit_virtualenv_root: Optional[str] = None
        python_interpreter: Optional[str] = None
        entrypoint_settings: Optional[EntrypointSettings] = None
        fast_serialization_settings: Optional[FastSerializationSettings] = None

        def with_fast_serialization_settings(self, fss: fast_serialization_settings) -> SerializationSettings.Builder:
            self.fast_serialization_settings = fss
            return self

        def build(self) -> SerializationSettings:
            return SerializationSettings(
                project=self.project,
                domain=self.domain,
                version=self.version,
                image_config=self.image_config,
                env=self.env,
                flytekit_virtualenv_root=self.flytekit_virtualenv_root,
                python_interpreter=self.python_interpreter,
                entrypoint_settings=self.entrypoint_settings,
                fast_serialization_settings=self.fast_serialization_settings,
            )

    def new_builder(self) -> Builder:
        """
        Creates a ``SerializationSettings.Builder`` that copies the existing serialization settings parameters and
        allows for customization.
        """
        return SerializationSettings.Builder(
            project=self.project,
            domain=self.domain,
            version=self.version,
            image_config=self.image_config,
            env=self.env,
            flytekit_virtualenv_root=self.flytekit_virtualenv_root,
            python_interpreter=self.python_interpreter,
            entrypoint_settings=self.entrypoint_settings,
            fast_serialization_settings=self.fast_serialization_settings,
        )

    def should_fast_serialize(self) -> bool:
        """
        Whether or not the serialization settings specify that entities should be serialized for fast registration.
        """
        return self.fast_serialization_settings is not None and self.fast_serialization_settings.enabled


@dataclass(frozen=True)
class CompilationState(object):
    """
    Compilation state is used during the compilation of a workflow or task. It stores the nodes that were
    created when walking through the workflow graph.

    Attributes:
        prefix (str): This is because we may one day want to be able to have subworkflows inside other workflows. If
            users choose to not specify their node names, then we can end up with multiple "n0"s. This prefix allows
            us to give those nested nodes a distinct name, as well as properly identify them in the workflow.
        mode (int): refer to :py:class:`flytekit.extend.ExecutionState.Mode`
        task_resolver (Optional[TaskResolverMixin]): Please see :py:class:`flytekit.extend.TaskResolverMixin`
        nodes (Optional[List]): Stores currently compiled nodes so far.
    """

    prefix: str
    mode: int = 1
    task_resolver: Optional[TaskResolverMixin] = None
    nodes: List = field(default_factory=list)

    def add_node(self, n: Node):
        self.nodes.append(n)

    def with_params(
        self,
        prefix: str,
        mode: Optional[int] = None,
        resolver: Optional[TaskResolverMixin] = None,
        nodes: Optional[List] = None,
    ) -> CompilationState:
        """
        Create a new CompilationState where the mode and task resolver are defaulted to the current object, but they
        and all other args are taken if explicitly provided as an argument.

        Usage:
            s.with_params("p", nodes=[])
        """
        return CompilationState(
            prefix=prefix if prefix else "",
            mode=mode if mode else self.mode,
            task_resolver=resolver if resolver else self.task_resolver,
            nodes=nodes if nodes else [],
        )


class BranchEvalMode(Enum):
    """
    This is a 3-way class, with the None value meaning that we are not within a conditional context. The other two
    values are
    * Active - This means that the next ``then`` should run
    * Skipped - The next ``then`` should not run
    """

    BRANCH_ACTIVE = "branch active"
    BRANCH_SKIPPED = "branch skipped"


@dataclass(init=False)
class ExecutionState(object):
    """
    This is the context that is active when executing a task or a local workflow. This carries the necessary state to
    execute.
    Some required things during execution deal with temporary directories, ExecutionParameters that are passed to the
    user etc.

    Attributes:
        mode (ExecutionState.Mode): Defines the context in which the task is executed (local, hosted, etc).
        working_dir (os.PathLike): Specifies the remote, external directory where inputs, outputs and other protobufs
            are uploaded
        engine_dir (os.PathLike):
        additional_context Optional[Dict[Any, Any]]: Free form dictionary used to store additional values, for example
            those used for dynamic, fast registration.
        branch_eval_mode Optional[BranchEvalMode]: Used to determine whether a branch node should execute.
        user_space_params Optional[ExecutionParameters]: Provides run-time, user-centric context such as a statsd
            handler, a logging handler, the current execution id and a working directory.
    """

    class Mode(Enum):
        """
        Defines the possible execution modes, which in turn affects execution behavior.
        """

        #: This is the mode that is used when a task execution mimics the actual runtime environment.
        #: NOTE: This is important to understand the difference between TASK_EXECUTION and LOCAL_TASK_EXECUTION
        #: LOCAL_TASK_EXECUTION, is the mode that is run purely locally and in some cases the difference between local
        #: and runtime environment may be different. For example for Dynamic tasks local_task_execution will just run it
        #: as a regular function, while task_execution will extract a runtime spec
        TASK_EXECUTION = 1

        #: This represents when flytekit is locally running a workflow. The behavior of tasks differs in this case
        #: because instead of running a task's user defined function directly, it'll need to wrap the return values in
        #: NodeOutput
        LOCAL_WORKFLOW_EXECUTION = 2

        #: This is the mode that is used to to indicate a purely local task execution - i.e. running without a container
        #: or propeller.
        LOCAL_TASK_EXECUTION = 3

    mode: Optional[ExecutionState.Mode]
    working_dir: os.PathLike
    engine_dir: Optional[Union[os.PathLike, str]]
    additional_context: Optional[Dict[Any, Any]]
    branch_eval_mode: Optional[BranchEvalMode]
    user_space_params: Optional[ExecutionParameters]

    def __init__(
        self,
        working_dir: os.PathLike,
        mode: Optional[ExecutionState.Mode] = None,
        engine_dir: Optional[Union[os.PathLike, str]] = None,
        additional_context: Optional[Dict[Any, Any]] = None,
        branch_eval_mode: Optional[BranchEvalMode] = None,
        user_space_params: Optional[ExecutionParameters] = None,
    ):
        if not working_dir:
            raise ValueError("Working directory is needed")
        self.working_dir = working_dir
        self.mode = mode
        self.engine_dir = engine_dir if engine_dir else os.path.join(self.working_dir, "engine_dir")
        pathlib.Path(self.engine_dir).mkdir(parents=True, exist_ok=True)
        self.additional_context = additional_context
        self.branch_eval_mode = branch_eval_mode
        self.user_space_params = user_space_params

    def take_branch(self):
        """
        Indicates that we are within an if-else block and the current branch has evaluated to true.
        Useful only in local execution mode
        """
        object.__setattr__(self, "branch_eval_mode", BranchEvalMode.BRANCH_ACTIVE)

    def branch_complete(self):
        """
        Indicates that we are within a conditional / ifelse block and the active branch is not done.
        Default to SKIPPED
        """
        object.__setattr__(self, "branch_eval_mode", BranchEvalMode.BRANCH_SKIPPED)

    def with_params(
        self,
        working_dir: Optional[os.PathLike] = None,
        mode: Optional[Mode] = None,
        engine_dir: Optional[os.PathLike] = None,
        additional_context: Optional[Dict[Any, Any]] = None,
        branch_eval_mode: Optional[BranchEvalMode] = None,
        user_space_params: Optional[ExecutionParameters] = None,
    ) -> ExecutionState:
        """
        Produces a copy of the current execution state and overrides the copy's parameters with passed parameter values.
        """
        if self.additional_context:
            if additional_context:
                additional_context = {**self.additional_context, **additional_context}
            else:
                additional_context = self.additional_context

        return ExecutionState(
            working_dir=working_dir if working_dir else self.working_dir,
            mode=mode if mode else self.mode,
            engine_dir=engine_dir if engine_dir else self.engine_dir,
            additional_context=additional_context,
            branch_eval_mode=branch_eval_mode if branch_eval_mode else self.branch_eval_mode,
            user_space_params=user_space_params if user_space_params else self.user_space_params,
        )


@dataclass(frozen=True)
class FlyteContext(object):
    """
    This is an internal-facing context object, that most users will not have to deal with. It's essentially a globally
    available grab bag of settings and objects that allows flytekit to do things like convert complex types, run and
    compile workflows, serialize Flyte entities, etc.

    Even though this object as a ``current_context`` function on it, it should not be called directly. Please use the
    :py:class:`flytekit.FlyteContextManager` object instead.

    Please do not confuse this object with the :py:class:`flytekit.ExecutionParameters` object.
    """

    file_access: FileAccessProvider
    level: int = 0
    flyte_client: Optional[friendly_client.SynchronousFlyteClient] = None
    compilation_state: Optional[CompilationState] = None
    execution_state: Optional[ExecutionState] = None
    serialization_settings: Optional[SerializationSettings] = None
    in_a_condition: bool = False
    origin_stackframe: Optional[traceback.FrameSummary] = None

    @property
    def user_space_params(self) -> Optional[ExecutionParameters]:
        if self.execution_state:
            return self.execution_state.user_space_params
        return None

    def set_stackframe(self, s: traceback.FrameSummary):
        object.__setattr__(self, "origin_stackframe", s)

    def get_origin_stackframe_repr(self) -> str:
        if self.origin_stackframe:
            f = self.origin_stackframe
            return f"StackOrigin({f.name}, {f.lineno}, {f.filename})"
        return ""

    def new_builder(self) -> Builder:
        return FlyteContext.Builder(
            level=self.level,
            file_access=self.file_access,
            flyte_client=self.flyte_client,
            serialization_settings=self.serialization_settings,
            compilation_state=self.compilation_state,
            execution_state=self.execution_state,
            in_a_condition=self.in_a_condition,
        )

    def enter_conditional_section(self) -> Builder:
        # logging.debug("Creating a nested condition")
        return self.new_builder().enter_conditional_section()

    def with_execution_state(self, es: ExecutionState) -> Builder:
        return self.new_builder().with_execution_state(es)

    def with_compilation_state(self, c: CompilationState) -> Builder:
        return self.new_builder().with_compilation_state(c)

    def with_new_compilation_state(self) -> Builder:
        return self.with_compilation_state(self.new_compilation_state())

    def with_file_access(self, fa: FileAccessProvider) -> Builder:
        return self.new_builder().with_file_access(fa)

    def with_serialization_settings(self, ss: SerializationSettings) -> Builder:
        return self.new_builder().with_serialization_settings(ss)

    def new_compilation_state(self, prefix: str = "") -> CompilationState:
        """
        Creates and returns a default compilation state. For most of the code this should be the entrypoint
        of compilation, otherwise the code should always uses - with_compilation_state
        """
        return CompilationState(prefix=prefix)

    def new_execution_state(self, working_dir: Optional[os.PathLike] = None) -> ExecutionState:
        """
        Creates and returns a new default execution state. This should be used at the entrypoint of execution,
        in all other cases it is preferable to use with_execution_state
        """
        if not working_dir:
            working_dir = self.file_access.local_sandbox_dir
        return ExecutionState(working_dir=working_dir, user_space_params=self.user_space_params)

    @staticmethod
    def current_context() -> Optional[FlyteContext]:
        """
        This method exists only to maintain backwards compatibility. Please use
        ``FlyteContextManager.current_context()`` instead.

        Users of flytekit should be wary not to confuse the object returned from this function
        with :py:func:`flytekit.current_context`
        """
        return FlyteContextManager.current_context()

    @dataclass
    class Builder(object):
        file_access: FileAccessProvider
        level: int = 0
        compilation_state: Optional[CompilationState] = None
        execution_state: Optional[ExecutionState] = None
        flyte_client: Optional[friendly_client.SynchronousFlyteClient] = None
        serialization_settings: Optional[SerializationSettings] = None
        in_a_condition: bool = False

        def build(self) -> FlyteContext:
            return FlyteContext(
                level=self.level + 1,
                file_access=self.file_access,
                compilation_state=self.compilation_state,
                execution_state=self.execution_state,
                flyte_client=self.flyte_client,
                serialization_settings=self.serialization_settings,
                in_a_condition=self.in_a_condition,
            )

        def enter_conditional_section(self) -> FlyteContext.Builder:
            """
            Used by the condition block to indicate that a new conditional section has been started.
            """

            if self.compilation_state:
                self.compilation_state = self.compilation_state.with_params(prefix=self.compilation_state.prefix)

            if self.execution_state:
                if self.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
                    if self.in_a_condition:
                        if self.execution_state.branch_eval_mode == BranchEvalMode.BRANCH_SKIPPED:
                            self.execution_state = self.execution_state.with_params()
                    else:
                        # In case of local workflow execution we should ensure a conditional section
                        # is created so that skipped branches result in tasks not being executed
                        self.execution_state = self.execution_state.with_params(
                            branch_eval_mode=BranchEvalMode.BRANCH_SKIPPED
                        )

            self.in_a_condition = True
            return self

        def with_execution_state(self, es: ExecutionState) -> FlyteContext.Builder:
            self.execution_state = es
            return self

        def with_compilation_state(self, c: CompilationState) -> FlyteContext.Builder:
            self.compilation_state = c
            return self

        def with_new_compilation_state(self) -> FlyteContext.Builder:
            return self.with_compilation_state(self.new_compilation_state())

        def with_file_access(self, fa: FileAccessProvider) -> FlyteContext.Builder:
            self.file_access = fa
            return self

        def with_serialization_settings(self, ss: SerializationSettings) -> FlyteContext.Builder:
            self.serialization_settings = ss
            return self

        def new_compilation_state(self, prefix: str = "") -> CompilationState:
            """
            Creates and returns a default compilation state. For most of the code this should be the entrypoint
            of compilation, otherwise the code should always uses - with_compilation_state
            """
            return CompilationState(prefix=prefix)

        def new_execution_state(self, working_dir: Optional[Union[os.PathLike, str]] = None) -> ExecutionState:
            """
            Creates and returns a new default execution state. This should be used at the entrypoint of execution,
            in all other cases it is preferable to use with_execution_state
            """
            if not working_dir:
                working_dir = self.file_access.get_random_local_directory()
            return ExecutionState(working_dir=working_dir)


class FlyteContextManager(object):
    """
    FlyteContextManager manages the execution context within Flytekit. It holds global state of either compilation
    or Execution. It is not thread-safe and can only be run as a single threaded application currently.
    Context's within Flytekit is useful to manage compilation state and execution state. Refer to ``CompilationState``
    and ``ExecutionState`` for for information. FlyteContextManager provides a singleton stack to manage these contexts.

    Typical usage is

    .. code-block:: python

        FlyteContextManager.initialize()
        with FlyteContextManager.with_context(o) as ctx:
          pass

        # If required - not recommended you can use
        FlyteContextManager.push_context()
        # but correspondingly a pop_context should be called
        FlyteContextManager.pop_context()
    """

    _OBJS: typing.List[FlyteContext] = []

    @staticmethod
    def get_origin_stackframe(limit=2) -> traceback.FrameSummary:
        ss = traceback.extract_stack(limit=limit + 1)
        if len(ss) > limit + 1:
            return ss[limit]
        return ss[0]

    @staticmethod
    def current_context() -> Optional[FlyteContext]:
        if FlyteContextManager._OBJS:
            return FlyteContextManager._OBJS[-1]
        return None

    @staticmethod
    def push_context(ctx: FlyteContext, f: Optional[traceback.FrameSummary] = None) -> FlyteContext:
        if not f:
            f = FlyteContextManager.get_origin_stackframe(limit=2)
        ctx.set_stackframe(f)
        FlyteContextManager._OBJS.append(ctx)
        t = "\t"
        logging.debug(
            f"{t * ctx.level}[{len(FlyteContextManager._OBJS)}] Pushing context - {'compile' if ctx.compilation_state else 'execute'}, branch[{ctx.in_a_condition}], {ctx.get_origin_stackframe_repr()}"
        )
        return ctx

    @staticmethod
    def pop_context() -> FlyteContext:
        ctx = FlyteContextManager._OBJS.pop()
        t = "\t"
        logging.debug(
            f"{t * ctx.level}[{len(FlyteContextManager._OBJS) + 1}] Popping context - {'compile' if ctx.compilation_state else 'execute'}, branch[{ctx.in_a_condition}], {ctx.get_origin_stackframe_repr()}"
        )
        if len(FlyteContextManager._OBJS) == 0:
            raise AssertionError(f"Illegal Context state! Popped, {ctx}")
        return ctx

    @staticmethod
    @contextmanager
    def with_context(b: FlyteContext.Builder) -> Generator[FlyteContext, None, None]:
        ctx = FlyteContextManager.push_context(b.build(), FlyteContextManager.get_origin_stackframe(limit=3))
        l = FlyteContextManager.size()
        try:
            yield ctx
        finally:
            # NOTE: Why? Do we have a loop here to ensure that we are popping all context up to the previously recorded
            # length? This is because it is possible that a conditional context may have leaked. Because of the syntax
            # of conditionals, if a conditional section fails to evaluate / compile, the context is not removed from the
            # stack. This is because context managers cannot be used in the conditional section.
            #   conditional().if_(...)......
            # Ideally we should have made conditional like so
            # with conditional() as c
            #      c.if_().....
            # the reason why we did not do that, was because, of the brevity and the assignment of outputs. Also, nested
            # conditionals using the context manager syntax is not easy to follow. So we wanted to optimize for the user
            # ergonomics
            # Also we know that top level construct like workflow and tasks always use context managers and that
            # context manager mutations are single threaded, hence we can safely cleanup leaks in this section
            # Also this is only in the error cases!
            while FlyteContextManager.size() >= l:
                FlyteContextManager.pop_context()

    @staticmethod
    def size() -> int:
        return len(FlyteContextManager._OBJS)

    @staticmethod
    def initialize():
        """
        Re-initializes the context and erases the entire context
        """
        # This is supplied so that tasks that rely on Flyte provided param functionality do not fail when run locally
        default_execution_id = _identifier.WorkflowExecutionIdentifier(project="local", domain="local", name="local")

        # Ensure a local directory is available for users to work with.
        user_space_path = os.path.join(_sdk_config.LOCAL_SANDBOX.get(), "user_space")
        pathlib.Path(user_space_path).mkdir(parents=True, exist_ok=True)

        # Note we use the SdkWorkflowExecution object purely for formatting into the ex:project:domain:name format users
        # are already acquainted with
        default_context = FlyteContext(file_access=default_local_file_access_provider)
        default_user_space_params = ExecutionParameters(
            execution_id=str(WorkflowExecutionIdentifier.promote_from_model(default_execution_id)),
            execution_date=_datetime.datetime.utcnow(),
            stats=mock_stats.MockStats(),
            logging=_logging,
            tmp_dir=user_space_path,
            raw_output_prefix=default_context.file_access._raw_output_prefix,
        )

        default_context = default_context.with_execution_state(
            default_context.new_execution_state().with_params(user_space_params=default_user_space_params)
        ).build()
        default_context.set_stackframe(s=FlyteContextManager.get_origin_stackframe())
        FlyteContextManager._OBJS = [default_context]


class FlyteEntities(object):
    """
    This is a global Object that tracks various tasks and workflows that are declared within a VM during the
     registration process
    """

    entities = []


FlyteContextManager.initialize()
