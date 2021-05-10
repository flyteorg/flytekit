from __future__ import annotations

import datetime as _datetime
import logging
import logging as _logging
import os
import pathlib
import re
import traceback
import typing
from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Generator, List, Optional

from docker_image import reference

from flytekit.clients import friendly as friendly_client  # noqa
from flytekit.common.core.identifier import WorkflowExecutionIdentifier as _SdkWorkflowExecutionIdentifier
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.configuration import images, internal
from flytekit.configuration import sdk as _sdk_config
from flytekit.engines.unit import mock_stats as _mock_stats
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models.core import identifier as _identifier

# TODO: resolve circular import from flytekit.core.python_auto_container import TaskResolverMixin

_DEFAULT_FLYTEKIT_ENTRYPOINT_FILELOC = "bin/entrypoint.py"


@dataclass(init=True, repr=True, eq=True, frozen=True)
class Image(object):
    name: str
    fqn: str
    tag: str

    @property
    def full(self):
        return f"{self.fqn}:{self.tag}"


@dataclass(init=True, repr=True, eq=True, frozen=True)
class ImageConfig(object):
    default_image: Image
    images: List[Image] = None

    def find_image(self, name) -> Optional[Image]:
        for i in self.images:
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


def get_image_config(img_name: str = None) -> ImageConfig:
    image_name = img_name if img_name else internal.IMAGE.get()
    default_img = look_up_image_info("default", image_name)
    other_images = [look_up_image_info(k, tag=v, optional_tag=True) for k, v in images.get_specified_images().items()]
    other_images.append(default_img)
    return ImageConfig(default_image=default_img, images=other_images)


@dataclass
class InstanceVar(object):
    module: str
    name: str
    o: Any


@dataclass
class EntrypointSettings(object):
    """
    This object carries information about the command, path and version of the entrypoint program that will be invoked
    to execute tasks at runtime.
    """

    path: str = None
    command: str = None
    version: int = 0


@dataclass(frozen=True)
class SerializationSettings(object):
    """
    These settings are provided while serializing a workflow and task, before registration. This is required to get
    runtime information at serialization time, as well as some defaults.
    """

    project: str
    domain: str
    version: str
    image_config: ImageConfig
    env: Optional[Dict[str, str]] = None
    flytekit_virtualenv_root: Optional[str] = None
    python_interpreter: Optional[str] = None
    entrypoint_settings: Optional[EntrypointSettings] = None


@dataclass(frozen=True)
class CompilationState(object):
    """
    Compilation state is used during the compilation of a workflow or task. It stores the nodes that were
    created when walking through the workflow graph.

    :param prefix: This is because we may one day want to be able to have subworkflows inside other workflows. If
      users choose to not specify their node names, then we can end up with multiple "n0"s. This prefix allows
      us to give those nested nodes a distinct name, as well as properly identify them in the workflow.
    :param task_resolver: Please see :py:class:`flytekit.extend.TaskResolverMixin`
    """

    prefix: str
    mode: int = 1
    task_resolver: Optional["TaskResolverMixin"] = None
    nodes: List = field(default_factory=list)

    def add_node(self, n: Node):
        self.nodes.append(n)

    def with_params(
        self,
        prefix: str,
        mode: Optional[int] = None,
        resolver: Optional["TaskResolverMixin"] = None,
        nodes: Optional[List] = None,
    ) -> CompilationState:
        """
        Create a new CompilationState where all the attributes are defaulted from the current CompilationState unless
        explicitly provided as an argument.

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
    BRANCH_ACTIVE = "branch active"
    BRANCH_SKIPPED = "branch skipped"


@dataclass(init=False)
class ExecutionState(object):
    """
    This is the context that is active when executing a task or a local workflow. This carries the necessary state to
    execute.
    Some required things during execution deal with temporary directories, ExecutionParameters that are passed to the
    user etc.
    """

    class Mode(Enum):
        # This is the mode that is used when a task execution mimics the actual runtime environment.
        # NOTE: This is important to understand the difference between TASK_EXECUTION and LOCAL_TASK_EXECUTION
        # LOCAL_TASK_EXECUTION, is the mode that is run purely locally and in some cases the difference between local
        # and runtime environment may be different. For example for Dynamic tasks local_task_execution will just run it
        # as a regular function, while task_execution will extract a runtime spec
        TASK_EXECUTION = 1

        # This represents when flytekit is locally running a workflow. The behavior of tasks differs in this case
        # because instead of running a task's user defined function directly, it'll need to wrap the return values in
        # NodeOutput
        LOCAL_WORKFLOW_EXECUTION = 2

        # This is the mode that is used to to indicate a purely local task execution - i.e. running without a container
        # or propeller.
        LOCAL_TASK_EXECUTION = 3

    mode: Mode
    working_dir: os.PathLike
    engine_dir: os.PathLike
    additional_context: Optional[Dict[Any, Any]]
    branch_eval_mode: Optional[BranchEvalMode]
    user_space_params: Optional[ExecutionParameters]

    def __init__(
        self,
        working_dir: os.PathLike,
        mode: Optional[Mode] = None,
        engine_dir: Optional[os.PathLike] = None,
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
        object.__setattr__(self, "_branch_eval_mode", BranchEvalMode.BRANCH_ACTIVE)

    def branch_complete(self):
        """
        Indicates that we are within a conditional / ifelse block and the active branch is not done.
        Default to SKIPPED
        """
        object.__setattr__(self, "_branch_eval_mode", BranchEvalMode.BRANCH_SKIPPED)

    def with_params(
        self,
        working_dir: Optional[os.PathLike] = None,
        mode: Optional[Mode] = None,
        engine_dir: Optional[os.PathLike] = None,
        additional_context: Optional[Dict[Any, Any]] = None,
        branch_eval_mode: Optional[BranchEvalMode] = None,
        user_space_params: Optional[ExecutionParameters] = None,
    ) -> ExecutionState:
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
    Top level context for FlyteKit. maintains information that is required either to compile or execute a workflow / task
    """

    file_access: Optional[_data_proxy.FileAccessProvider]
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

    @dataclass
    class Builder(object):
        file_access: _data_proxy.FileAccessProvider
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
        new_ctx = self.new_builder()
        if new_ctx.in_a_condition:
            raise NotImplementedError("Nested branches are not yet supported!")

        if new_ctx.compilation_state:
            prefix = new_ctx.compilation_state.prefix + "branch" if new_ctx.compilation_state.prefix else "branch"
            new_ctx.compilation_state = new_ctx.compilation_state.with_params(prefix=prefix)

        if new_ctx.execution_state:
            if new_ctx.execution_state.Mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
                """
                In case of local workflow execution we should ensure a conditional section
                is created so that skipped branches result in tasks not being executed
                """
                new_ctx.execution_state = new_ctx.execution_state.with_params(
                    branch_eval_mode=BranchEvalMode.BRANCH_SKIPPED
                )

        new_ctx.in_a_condition = True
        return new_ctx

    def with_execution_state(self, es: ExecutionState) -> Builder:
        new_ctx = self.new_builder()
        new_ctx.execution_state = es
        return new_ctx

    def with_compilation_state(self, c: CompilationState) -> Builder:
        new_ctx = self.new_builder()
        new_ctx.compilation_state = c
        return new_ctx

    def with_new_compilation_state(self) -> Builder:
        return self.with_compilation_state(self.new_compilation_state())

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
            working_dir = self.file_access.get_random_local_directory()
        return ExecutionState(working_dir=working_dir)

    def with_file_access(self, fa: _data_proxy.FileAccessProvider) -> Builder:
        new_ctx = self.new_builder()
        new_ctx.file_access = fa
        return new_ctx

    def with_serialization_settings(self, ss: SerializationSettings) -> Builder:
        new_ctx = self.new_builder()
        new_ctx.serialization_settings = ss
        return new_ctx


class FlyteContextManager(object):
    """
    FlyteContextManager manages the execution context within Flytekit. It holds global state of either compilation
    or Execution. It is not thread-safe and can only be run as a single threaded application currently.
    Context's within Flytekit is useful to manage compilation state and execution state. Refer to ``CompilationState``
    and ``ExecutionState`` for for information. FlyteContextManager provides a singleton stack to manage these contexts.

    Typical usage is:
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
    def current_context() -> FlyteContext:
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
            f"{t * ctx.level}[{len(FlyteContextManager._OBJS) + 1 }] Popping context - {'compile' if ctx.compilation_state else 'execute'}, branch[{ctx.in_a_condition}], {ctx.get_origin_stackframe_repr()}"
        )
        if len(FlyteContextManager._OBJS) == 0:
            raise AssertionError(f"Illegal Context state! Popped, {ctx}")
        return ctx

    @staticmethod
    @contextmanager
    def with_context(b: FlyteContext.Builder) -> Generator[FlyteContext, None, None]:
        c = FlyteContextManager.push_context(b.build(), FlyteContextManager.get_origin_stackframe(limit=3))
        try:
            yield c
        finally:
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
        # Note we use the SdkWorkflowExecution object purely for formatting into the ex:project:domain:name format users
        # are already acquainted with
        default_user_space_params = ExecutionParameters(
            execution_id=str(_SdkWorkflowExecutionIdentifier.promote_from_model(default_execution_id)),
            execution_date=_datetime.datetime.utcnow(),
            stats=_mock_stats.MockStats(),
            logging=_logging,
            tmp_dir=os.path.join(_sdk_config.LOCAL_SANDBOX.get(), "user_space"),
        )
        default_context = FlyteContext(file_access=_data_proxy.default_local_file_access_provider)
        default_context = default_context.with_execution_state(
            default_context.new_execution_state().with_params(user_space_params=default_user_space_params)
        ).build()
        default_context.set_stackframe(s=FlyteContextManager.get_origin_stackframe())
        FlyteContextManager._OBJS = [default_context]


class FlyteContext2(object):
    OBJS = []

    def __init__(
        self,
        parent=None,
        file_access: _data_proxy.FileAccessProvider = None,
        compilation_state: CompilationState = None,
        execution_state: ExecutionState = None,
        flyte_client: friendly_client.SynchronousFlyteClient = None,
        user_space_params: ExecutionParameters = None,
        serialization_settings: SerializationSettings = None,
    ):
        if parent is None and len(FlyteContext.OBJS) > 0:
            parent = FlyteContext.OBJS[-1]

        if compilation_state is not None and execution_state is not None:
            raise Exception("Can't specify both")

        self._parent: FlyteContext = parent
        self._file_access = file_access
        self._compilation_state = compilation_state
        self._execution_state = execution_state
        self._flyte_client = flyte_client
        self._user_space_params = user_space_params
        self._serialization_settings = serialization_settings

    def __enter__(self):
        # Should we auto-assign the parent here?
        # Or detect if self's parent is not [-1]?
        FlyteContext.OBJS.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        FlyteContext.OBJS.pop()

    @classmethod
    def current_context(cls) -> FlyteContext:
        if len(cls.OBJS) == 0:
            raise Exception("There should pretty much always be a base context object.")
        return cls.OBJS[-1]

    @contextmanager
    def new_context(
        self,
        file_access: _data_proxy.FileAccessProvider = None,
        compilation_state: CompilationState = None,
        execution_state: ExecutionState = None,
        flyte_client: friendly_client.SynchronousFlyteClient = None,
        user_space_params: ExecutionParameters = None,
        serialization_settings: SerializationSettings = None,
    ):
        new_ctx = FlyteContext(
            parent=self,
            file_access=file_access,
            compilation_state=compilation_state,
            execution_state=execution_state,
            flyte_client=flyte_client,
            user_space_params=user_space_params,
            serialization_settings=serialization_settings,
        )
        FlyteContext.OBJS.append(new_ctx)
        try:
            yield new_ctx
        finally:
            FlyteContext.OBJS.pop()

    @property
    def file_access(self) -> _data_proxy.FileAccessProvider:
        if self._file_access is not None:
            return self._file_access
        elif self._parent is not None:
            return self._parent.file_access
        else:
            raise Exception("No file_access initialized")

    @contextmanager
    def new_file_access_context(self, file_access_provider: _data_proxy.FileAccessProvider):
        new_ctx = FlyteContext(parent=self, file_access=file_access_provider)
        FlyteContext.OBJS.append(new_ctx)
        try:
            yield new_ctx
        finally:
            FlyteContext.OBJS.pop()

    @property
    def user_space_params(self) -> Optional[ExecutionParameters]:
        if self._user_space_params is not None:
            return self._user_space_params
        elif self._parent is not None:
            return self._parent.user_space_params
        else:
            raise Exception("No user_space_params initialized")

    @property
    def execution_state(self) -> Optional[ExecutionState]:
        return self._execution_state

    @contextmanager
    def new_execution_context(
        self,
        mode: ExecutionState.Mode,
        additional_context: Dict[Any, Any] = None,
        execution_params: Optional[ExecutionParameters] = None,
        working_dir: Optional[str] = None,
    ) -> Generator[FlyteContext, None, None]:
        # Create a working directory for the execution to use
        working_dir = working_dir or self.file_access.get_random_local_directory()
        engine_dir = os.path.join(working_dir, "engine_dir")
        pathlib.Path(engine_dir).mkdir(parents=True, exist_ok=True)
        if additional_context is None:
            additional_context = self.execution_state.additional_context if self.execution_state is not None else None
        elif self.execution_state is not None and self.execution_state.additional_context is not None:
            additional_context = {**self.execution_state.additional_context, **additional_context}
        exec_state = ExecutionState(
            mode=mode, working_dir=working_dir, engine_dir=engine_dir, additional_context=additional_context
        )

        # If a wf_params object was not given, use the default (defined at the bottom of this file)
        new_ctx = FlyteContext(
            parent=self,
            execution_state=exec_state,
            user_space_params=execution_params or default_user_space_params,
        )
        FlyteContext.OBJS.append(new_ctx)
        try:
            yield new_ctx
        finally:
            FlyteContext.OBJS.pop()

    @property
    def compilation_state(self) -> Optional[CompilationState]:
        if self._compilation_state is not None:
            return self._compilation_state
        elif self._parent is not None:
            return self._parent.compilation_state
        else:
            return None

    @contextmanager
    def new_compilation_context(
        self, prefix: Optional[str] = None, task_resolver: Optional["TaskResolverMixin"] = None
    ) -> Generator[FlyteContext, None, None]:
        """
        :param prefix: See CompilationState comments
        :param task_resolver: resolver for tasks within this compilation context
        """
        new_ctx = FlyteContext(
            parent=self, compilation_state=CompilationState(prefix=prefix or "", task_resolver=task_resolver)
        )
        FlyteContext.OBJS.append(new_ctx)
        try:
            yield new_ctx
        finally:
            FlyteContext.OBJS.pop()

    @property
    def serialization_settings(self) -> SerializationSettings:
        if self._serialization_settings is not None:
            return self._serialization_settings
        elif self._parent is not None:
            return self._parent.serialization_settings
        else:
            raise Exception("No serialization_settings initialized")

    @contextmanager
    def new_serialization_settings(
        self, serialization_settings: SerializationSettings
    ) -> Generator[FlyteContext, None, None]:
        new_ctx = FlyteContext(parent=self, serialization_settings=serialization_settings)
        FlyteContext.OBJS.append(new_ctx)
        try:
            yield new_ctx
        finally:
            FlyteContext.OBJS.pop()

    @property
    def flyte_client(self):
        if self._flyte_client is not None:
            return self._flyte_client
        elif self._parent is not None:
            return self._parent.flyte_client
        else:
            raise Exception("No flyte_client initialized")


# Hack... we'll think of something better in the future
class FlyteEntities(object):
    entities = []


FlyteContextManager.initialize()
