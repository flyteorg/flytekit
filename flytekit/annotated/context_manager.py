from __future__ import annotations

import os
import pathlib
from contextlib import contextmanager
from typing import List, Optional, Generator, Dict, Any
import datetime as _datetime
import logging as _logging
from enum import Enum

from flytekit.clients import friendly as flyte_client
from flytekit.common.core.identifier import WorkflowExecutionIdentifier as _SdkWorkflowExecutionIdentifier
from flytekit.common import constants as _constants
from flytekit.common.exceptions import user as _user_exception
from flytekit.annotated.node import Node
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.configuration import sdk as _sdk_config, internal as _internal_config
from flytekit.interfaces.data import common as _common_data
from flytekit.interfaces.data.gcs import gcs_proxy as _gcs_proxy
from flytekit.interfaces.data.http import http_data_proxy as _http_data_proxy
from flytekit.interfaces.data.local import local_file_proxy as _local_file_proxy
from flytekit.interfaces.data.s3 import s3proxy as _s3proxy
from flytekit.models.core import identifier as _identifier
from flytekit.interfaces.stats.taggable import get_stats as _get_stats
from flytekit.engines.unit import mock_stats as _mock_stats
from flytekit.models import interface as _interface_models


class RegistrationSettings(object):
    def __init__(self, project: str, domain: str, version: str, image: str, env: Optional[Dict[str, str]]):
        self._project = project
        self._domain = domain
        self._version = version
        self._image = image
        self._env = env or {}

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


class CompilationState(object):
    def __init__(self):
        self.nodes: List[Node] = []
        self.mode = 1  # TODO: Turn into enum in the future, or remove if only one mode.


class ExecutionState(object):
    class Mode(Enum):
        # This is the mode that will be selected when a task is supposed to just run its function, nothing more
        TASK_EXECUTION = 1

        # This represents when flytekit is locally running a workflow. The behavior of tasks differs in this case
        # because instead of running a task's user defined function directly, it'll need to wrap the return values in
        # NodeOutput
        LOCAL_WORKFLOW_EXECUTION = 2

    def __init__(self, mode: Mode, working_dir: os.PathLike, engine_dir: os.PathLike,
                 additional_context: Dict[Any, Any] = None):
        self._mode = mode
        self._working_dir = working_dir
        self._engine_dir = engine_dir
        self._additional_context = additional_context

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


class FlyteContext(object):
    _DATA_PROXIES_BY_PATH = {
        "s3:/": _s3proxy.AwsS3Proxy(),
        "gs:/": _gcs_proxy.GCSProxy(),
        "http://": _http_data_proxy.HttpFileProxy(),
        "https://": _http_data_proxy.HttpFileProxy(),
    }
    _DATA_PROXIES_BY_CLOUD_PROVIDER = {
        _constants.CloudProvider.AWS: _s3proxy.AwsS3Proxy(),
        _constants.CloudProvider.GCP: _gcs_proxy.GCSProxy(),
    }

    OBJS = []

    def __init__(self, parent=None,
                 local_file_access: _local_file_proxy.LocalFileProxy = None,
                 data_proxy: _common_data.DataProxy = None,
                 compilation_state: CompilationState = None,
                 execution_state: ExecutionState = None,
                 flyte_client: flyte_client.SynchronousFlyteClient = None,
                 user_space_params: ExecutionParameters = None,
                 registration_settings: RegistrationSettings = None,
                 ):
        # TODO: Should we have this auto-parenting feature?
        if parent is None and len(FlyteContext.OBJS) > 0:
            parent = FlyteContext.OBJS[-1]

        if compilation_state is not None and execution_state is not None:
            raise Exception("Can't specify both")

        self._parent: FlyteContext = parent
        self._local_file_access = local_file_access
        self._data_proxy = data_proxy
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
    def current_context(cls) -> 'FlyteContext':
        if len(cls.OBJS) == 0:
            raise Exception("There should pretty much always be a base context object.")
        return cls.OBJS[-1]

    @property
    def local_file_access(self):
        if self._local_file_access is not None:
            return self._local_file_access
        elif self._parent is not None:
            return self._parent.local_file_access
        else:
            raise Exception('No local_file_access initialized')

    @contextmanager
    def new_local_file_context(self, proxy):
        new_ctx = FlyteContext(parent=self, local_file_access=proxy)
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
            raise Exception('No user_space_params initialized')

    @property
    def data_proxy(self) -> _common_data.DataProxy:
        if self._data_proxy is not None:
            return self._data_proxy
        elif self._parent is not None:
            return self._parent.data_proxy
        else:
            raise Exception('No local_file_access initialized')

    @contextmanager
    def new_data_proxy(self, proxy):
        new_ctx = FlyteContext(parent=self, data_proxy=proxy)
        FlyteContext.OBJS.append(new_ctx)
        try:
            yield new_ctx
        finally:
            FlyteContext.OBJS.pop()

    @contextmanager
    def new_data_proxy_by_cloud_provider(self, cloud_provider: str, raw_output_data_prefix: Optional[str] = None) -> \
            Generator[FlyteContext, None, None]:
        if cloud_provider == _constants.CloudProvider.AWS:
            proxy = _s3proxy.AwsS3Proxy(raw_output_data_prefix)
        elif cloud_provider == _constants.CloudProvider.GCP:
            proxy = _gcs_proxy.GCSProxy(raw_output_data_prefix)
        else:
            raise _user_exception.FlyteAssertion(
                "Configured cloud provider is not supported for data I/O.  Received: {}, expected one of: {}".format(
                    cloud_provider,
                    list(type(self)._DATA_PROXIES_BY_CLOUD_PROVIDER.keys())
                )
            )

        new_ctx = FlyteContext(parent=self, data_proxy=proxy)
        FlyteContext.OBJS.append(new_ctx)
        try:
            yield new_ctx
        finally:
            FlyteContext.OBJS.pop()

    @property
    def execution_state(self) -> Optional[ExecutionState]:
        return self._execution_state

    @contextmanager
    def new_execution_context(self, mode: ExecutionState.Mode,
                              additional_context: Dict[Any, Any] = None,
                              execution_params: Optional[ExecutionParameters] = None) -> Generator[
        FlyteContext, None, None]:

        # Create a working directory for the execution to use
        working_dir = self.local_file_access.get_random_path()
        engine_dir = os.path.join(working_dir, "engine_dir")
        pathlib.Path(engine_dir).mkdir(parents=True, exist_ok=True)
        exec_state = ExecutionState(mode=mode, working_dir=working_dir, engine_dir=engine_dir,
                                    additional_context=additional_context)

        # If a wf_params object was not given, use the default (defined at the bottom of this file)
        new_ctx = FlyteContext(parent=self, execution_state=exec_state,
                               user_space_params=execution_params or default_user_space_params)
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
    def new_compilation_context(self) -> Generator['FlyteContext', None, None]:
        new_ctx = FlyteContext(parent=self, compilation_state=CompilationState())
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
            raise Exception('No local_file_access initialized')

    @contextmanager
    def new_registration_settings(self, registration_settings):
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
            raise Exception('No flyte_client initialized')


# Hack... we'll think of something better in the future
class FlyteEntities(object):
    entities = []


# This is supplied so that tasks that rely on Flyte provided param functionality do not fail when run locally
default_execution_id = _identifier.WorkflowExecutionIdentifier(
    project='local',
    domain='local',
    name='local'
)
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
    local_file_access=_local_file_proxy.LocalFileProxy(_sdk_config.LOCAL_SANDBOX.get()),
    data_proxy=_local_file_proxy.LocalFileProxy(os.path.join(_sdk_config.LOCAL_SANDBOX.get(), "data_dir")),
    user_space_params=default_user_space_params,
)
FlyteContext.OBJS.append(default_context)
