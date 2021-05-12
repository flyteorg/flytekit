from __future__ import annotations

import datetime as _datetime
import logging
import logging as _logging
import os
import pathlib
import re
import traceback
import typing
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
    Top level context for FlyteKit. maintains information that is required either to compile or execute a
    workflow / task
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
        return self.new_builder().enter_conditional_section()

    def with_execution_state(self, es: ExecutionState) -> Builder:
        return self.new_builder().with_execution_state(es)

    def with_compilation_state(self, c: CompilationState) -> Builder:
        return self.new_builder().with_compilation_state(c)

    def with_new_compilation_state(self) -> Builder:
        return self.with_compilation_state(self.new_compilation_state())

    def with_file_access(self, fa: _data_proxy.FileAccessProvider) -> Builder:
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
            working_dir = self.file_access.get_random_local_directory()
        return ExecutionState(working_dir=working_dir, user_space_params=self.user_space_params)

    @staticmethod
    def current_context() -> FlyteContext:
        """
        This method exists only to maintain backwards compatibility. Please use FlyteContextManager.current_context()
        If you are a user of flytekit, use
        ```
            import flytekit
            flytekit.current_context()
        ```
        """
        return FlyteContextManager.current_context()

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

        def enter_conditional_section(self) -> "Builder":
            if self.in_a_condition:
                raise NotImplementedError("Nested branches are not yet supported!")

            if self.compilation_state:
                prefix = self.compilation_state.prefix + "branch" if self.compilation_state.prefix else "branch"
                self.compilation_state = self.compilation_state.with_params(prefix=prefix)

            if self.execution_state:
                if self.execution_state.Mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
                    """
                    In case of local workflow execution we should ensure a conditional section
                    is created so that skipped branches result in tasks not being executed
                    """
                    self.execution_state = self.execution_state.with_params(
                        branch_eval_mode=BranchEvalMode.BRANCH_SKIPPED
                    )

            self.in_a_condition = True
            return self

        def with_execution_state(self, es: ExecutionState) -> "Builder":
            self.execution_state = es
            return self

        def with_compilation_state(self, c: CompilationState) -> "Builder":
            self.compilation_state = c
            return self

        def with_new_compilation_state(self) -> "Builder":
            return self.with_compilation_state(self.new_compilation_state())

        def with_file_access(self, fa: _data_proxy.FileAccessProvider) -> "Builder":
            self.file_access = fa
            return self

        def with_serialization_settings(self, ss: SerializationSettings) -> "Builder":
            self.serialization_settings = ss
            return self

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
            # NOTE: Why? Do we have a loop here to ensure that we are popping all context upto the previously recorded
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


class FlyteEntities(object):
    """
    This is a global Object that tracks various tasks and workflows that are declared within a VM during the
     registration process
    """

    entities = []


FlyteContextManager.initialize()
