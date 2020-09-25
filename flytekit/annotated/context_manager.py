import os
from contextlib import contextmanager
from typing import List, Optional, Generator, Dict, Any
import datetime as _datetime
import logging as _logging
from enum import Enum

from flytekit import __version__ as _api_version
from flytekit.clients import friendly as flyte_client
from flytekit.common import constants as _constants
from flytekit.common.exceptions import user as _user_exception
from flytekit.common.nodes import SdkNode
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


class CompilationState(object):
    def __init__(self):
        self.nodes: List[SdkNode] = []
        self.mode = 1  # TODO: Turn into enum in the future, or remove if only one mode.


class ExecutionState(object):
    class Mode(Enum):
        # This is the mode that will be selected when a task is being run on a Flyte cluster
        TASK_EXECUTION = 1

        # This represents when flytekit is locally running a workflow. The behavior of tasks differs in this case
        # because instead of running a task's user defined function directly, it'll need to wrap the return values in
        # NodeOutput
        LOCAL_WORKFLOW_EXECUTION = 2

    def __init__(self, mode: Mode, working_dir: os.PathLike, additional_context: Dict[Any, Any] = None):
        self._mode = mode
        self._working_dir = working_dir
        self._additional_context = additional_context

    @property
    def working_dir(self) -> os.PathLike:
        return self._working_dir

    @property
    def additional_context(self) -> Dict[Any,Any]:
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
    def user_space_params(self):
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

    @property
    def execution_state(self) -> Optional[ExecutionState]:
        return self._execution_state

    @contextmanager
    def new_execution_context(self, mode: ExecutionState.Mode,
                              cloud_provider: str,
                              additional_context: Dict[Any, Any] = None) -> Generator['FlyteContext', None, None]:
        proxy = self._DATA_PROXIES_BY_CLOUD_PROVIDER.get(cloud_provider, None)
        if proxy is None:
            raise _user_exception.FlyteAssertion(
                "Configured cloud provider is not supported for data I/O.  Received: {}, expected one of: {}".format(
                    cloud_provider,
                    list(type(self)._DATA_PROXIES_BY_CLOUD_PROVIDER.keys())
                )
            )
        # Create a working directory for the execution to use
        working_dir = self.local_file_access.get_random_path()
        exec_state = ExecutionState(mode=mode, working_dir=working_dir, additional_context=additional_context)

        # Create a more accurate object for users to use within tasks
        user_space_execution_params = ExecutionParameters(
            execution_id=_identifier.WorkflowExecutionIdentifier(
                project=_internal_config.EXECUTION_PROJECT.get(),
                domain=_internal_config.EXECUTION_DOMAIN.get(),
                name=_internal_config.EXECUTION_NAME.get()
            ),
            execution_date=_datetime.datetime.utcnow(),
            stats=_get_stats(
                # Stats metric path will be:
                # registration_project.registration_domain.app.module.task_name.user_stats
                # and it will be tagged with execution-level values for project/domain/wf/lp
                "{}.{}.{}.user_stats".format(
                    _internal_config.TASK_PROJECT.get() or _internal_config.PROJECT.get(),
                    _internal_config.TASK_DOMAIN.get() or _internal_config.DOMAIN.get(),
                    _internal_config.TASK_NAME.get() or _internal_config.NAME.get()
                ),
                tags={
                    'exec_project': _internal_config.EXECUTION_PROJECT.get(),
                    'exec_domain': _internal_config.EXECUTION_DOMAIN.get(),
                    'exec_workflow': _internal_config.EXECUTION_WORKFLOW.get(),
                    'exec_launchplan': _internal_config.EXECUTION_LAUNCHPLAN.get(),
                    'api_version': _api_version
                }
            ),
            logging=_logging,
            tmp_dir=os.path.join(working_dir, "user_space")
        )

        new_ctx = FlyteContext(parent=self, data_proxy=proxy, execution_state=exec_state,
                               user_space_params=user_space_execution_params)
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
    def flyte_client(self):
        if self._flyte_client is not None:
            return self._flyte_client
        elif self._parent is not None:
            return self._parent.flyte_client
        else:
            raise Exception('No flyte_client initialized')


# This is supplied so that tasks that rely on Flyte provided param functionality do not fail when run locally
default_user_space_params = ExecutionParameters(
    execution_id=_identifier.WorkflowExecutionIdentifier(
        project='local',
        domain='local',
        name='local'
    ),
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
