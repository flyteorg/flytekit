from __future__ import annotations

import datetime as _datetime
import logging as _logging
import os
import pathlib
from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, Generator, List, Optional

from flytekit.clients import friendly as friendly_client  # noqa
from flytekit.common.core.identifier import WorkflowExecutionIdentifier as _SdkWorkflowExecutionIdentifier
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.configuration import sdk as _sdk_config
from flytekit.engines.unit import mock_stats as _mock_stats
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models.common import RawOutputDataConfig
from flytekit.models.core import identifier as _identifier


class RegistrationSettings(object):
    def __init__(
        self,
        project: str,
        domain: str,
        version: str,
        image: str,
        env: Optional[Dict[str, str]],
        iam_role: Optional[str] = None,
        service_account: Optional[str] = None,
        raw_output_data_config: Optional[str] = None,
    ):
        self._project = project
        self._domain = domain
        self._version = version
        self._image = image
        self._env = env or {}
        self._iam_role = iam_role
        self._service_account = service_account
        self._raw_output_data_config = raw_output_data_config

    @property
    def project(self) -> str:
        return self._project

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def version(self) -> str:
        return self._version

    @property
    def image(self) -> str:
        return self._image

    @property
    def env(self) -> Dict[str, str]:
        return self._env

    @property
    def iam_role(self) -> Optional[str]:
        return self._iam_role

    @property
    def service_account(self) -> Optional[str]:
        return self._service_account

    @property
    def raw_output_data_config(self) -> RawOutputDataConfig:
        return RawOutputDataConfig(self._raw_output_data_config or "")


class CompilationState(object):
    def __init__(self, prefix: str):
        """
        :param prefix: This is because we may one day want to be able to have subworkflows inside other workflows. If
          users choose to not specify their node names, then we can end up with multiple "node-0"s. This prefix allows
          us to give those nested nodes a distinct name, as well as properly identify them in the workflow.
          # TODO: Ketan to revisit this whole concept when we re-organize the new structure
        """
        from flytekit.annotated.node import Node

        self._nodes: List[Node] = []
        self._old_prefix = ""
        self._prefix = prefix
        self.mode = 1  # TODO: Turn into enum in the future, or remove if only one mode.
        # TODO Branch mode should just be a new Compilation state context. But for now we are just
        # storing the nodes separately
        self._branch = False
        self._branch_nodes: List[Node] = []

    @property
    def prefix(self) -> str:
        return self._prefix

    def add_node(self, n: Node):
        if self._branch:
            self._branch_nodes.append(n)
        else:
            self._nodes.append(n)

    @property
    def nodes(self):
        if self._branch:
            return self._branch_nodes
        return self._nodes

    def enter_conditional_section(self):
        """
        We cannot use a context manager here, so we will mimic the context manager API
        """
        self._branch = True
        self._old_prefix = self._prefix
        self._prefix = self._prefix + "branch"

    def exit_conditional_section(self):
        """
        Disables that we are in a branch
        """
        self._branch = False
        self._branch_nodes = []
        self._prefix = self._old_prefix

    def is_in_a_branch(self) -> bool:
        return self._branch


class BranchEvalMode(Enum):
    BRANCH_ACTIVE = "branch active"
    BRANCH_SKIPPED = "branch skipped"


class ExecutionState(object):
    class Mode(Enum):
        # This is the mode that will be selected when a task is supposed to just run its function, nothing more
        TASK_EXECUTION = 1

        # This represents when flytekit is locally running a workflow. The behavior of tasks differs in this case
        # because instead of running a task's user defined function directly, it'll need to wrap the return values in
        # NodeOutput
        LOCAL_WORKFLOW_EXECUTION = 2

    def __init__(
        self, mode: Mode, working_dir: os.PathLike, engine_dir: os.PathLike, additional_context: Dict[Any, Any] = None
    ):
        self._mode = mode
        self._working_dir = working_dir
        self._engine_dir = engine_dir
        self._additional_context = additional_context
        self._branch_eval_mode = None

    @property
    def working_dir(self) -> os.PathLike:
        return self._working_dir

    @property
    def engine_dir(self) -> os.PathLike:
        return self._engine_dir

    @property
    def additional_context(self) -> Dict[Any, Any]:
        return self._additional_context

    @property
    def mode(self) -> Mode:
        return self._mode

    @property
    def branch_eval_mode(self) -> Optional[BranchEvalMode]:
        return self._branch_eval_mode

    def enter_conditional_section(self):
        """
        We cannot use a context manager here, so we will mimic the context manager API
        Reason we cannot use is because branch is a functional api and the context block is not well defined
        TODO we might want to create a new node manager here, as we want to capture all nodes in this branch
             context
        """
        self._branch_eval_mode = BranchEvalMode.BRANCH_SKIPPED

    def take_branch(self):
        """
        Indicates that we are within an if-else block and the current branch has evaluated to true.
        Useful only in local execution mode
        """
        self._branch_eval_mode = BranchEvalMode.BRANCH_ACTIVE

    def branch_complete(self):
        """
        Indicates that we are within a conditional / ifelse block and the active branch is not done.
        Default to SKIPPED
        """
        self._branch_eval_mode = BranchEvalMode.BRANCH_SKIPPED

    def exit_conditional_section(self):
        """
        Removes any current branch logic
        """
        self._branch_eval_mode = None


class FlyteContext(object):
    OBJS = []

    def __init__(
        self,
        parent=None,
        file_access: _data_proxy.FileAccessProvider = None,
        compilation_state: CompilationState = None,
        execution_state: ExecutionState = None,
        flyte_client: friendly_client.SynchronousFlyteClient = None,
        user_space_params: ExecutionParameters = None,
        registration_settings: RegistrationSettings = None,
    ):
        # TODO: Should we have this auto-parenting feature?
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
        self._registration_settings = registration_settings

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
    ) -> Generator[FlyteContext, None, None]:

        # Create a working directory for the execution to use
        working_dir = self.file_access.get_random_local_directory()
        engine_dir = os.path.join(working_dir, "engine_dir")
        pathlib.Path(engine_dir).mkdir(parents=True, exist_ok=True)
        exec_state = ExecutionState(
            mode=mode, working_dir=working_dir, engine_dir=engine_dir, additional_context=additional_context
        )

        # If a wf_params object was not given, use the default (defined at the bottom of this file)
        new_ctx = FlyteContext(
            parent=self, execution_state=exec_state, user_space_params=execution_params or default_user_space_params
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
    def new_compilation_context(self, prefix: Optional[str] = None) -> Generator[FlyteContext, None, None]:
        """
        :param prefix: See CompilationState comments
        """
        new_ctx = FlyteContext(parent=self, compilation_state=CompilationState(prefix=prefix or ""))
        FlyteContext.OBJS.append(new_ctx)
        try:
            yield new_ctx
        finally:
            FlyteContext.OBJS.pop()

    @property
    def registration_settings(self) -> RegistrationSettings:
        if self._registration_settings is not None:
            return self._registration_settings
        elif self._parent is not None:
            return self._parent.registration_settings
        else:
            raise Exception("No registration_settings initialized")

    @contextmanager
    def new_registration_settings(
        self, registration_settings: RegistrationSettings
    ) -> Generator[FlyteContext, None, None]:
        new_ctx = FlyteContext(parent=self, registration_settings=registration_settings)
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
default_context = FlyteContext(
    user_space_params=default_user_space_params, file_access=_data_proxy.default_local_file_access_provider
)
FlyteContext.OBJS.append(default_context)
