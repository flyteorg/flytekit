from contextlib import contextmanager
from typing import List, Optional, Generator

from flytekit.configuration import sdk as _sdk_config, platform as _platform_config
from flytekit.interfaces.data.s3 import s3proxy as _s3proxy
from flytekit.interfaces.data.gcs import gcs_proxy as _gcs_proxy
from flytekit.interfaces.data.local import local_file_proxy as _local_file_proxy
from flytekit.interfaces.data.http import http_data_proxy as _http_data_proxy
from flytekit.common.exceptions import user as _user_exception
from flytekit.common import utils as _common_utils, constants as _constants
from flytekit.common.nodes import SdkNode
from flytekit.interfaces.data import common as _common_data
from flytekit.clients import friendly as flyte_client


class CompilationState(object):
    def __init__(self):
        self.nodes: List[SdkNode] = []
        self.mode = 1  # TODO: Turn into enum in the future, or remove if only one mode.


class WorkflowExecutionState(object):
    """
    This class represents when flytekit is locally running a workflow. The behavior of tasks differs in this case
    because instead of running a task's user defined function directly, it'll need to wrap the return values in
    NodeOutput objects. This is because calling .with_cpu("2") doesn't make sense on a native Python type obviously.
    """


class FlyteContext(object):
    OBJS = []

    def __init__(self, parent=None,
                 local_file_access: _local_file_proxy.LocalFileProxy = None,
                 remote_data_proxy: _common_data.DataProxy = None,
                 compilation_state: CompilationState = None,
                 workflow_execution_state: WorkflowExecutionState = None,
                 flyte_client: flyte_client.SynchronousFlyteClient = None,
                 ):
        # TODO: Should we have this auto-parenting feature?
        if parent is None and len(FlyteContext.OBJS) > 0:
            parent = FlyteContext.OBJS[-1]

        if compilation_state is not None and workflow_execution_state is not None:
            raise Exception("Can't specify both")

        self._parent: FlyteContext = parent
        self._local_file_access = local_file_access
        self._remote_data_proxy = remote_data_proxy
        self._compilation_state = compilation_state
        self._workflow_execution_state = workflow_execution_state
        self._flyte_client = flyte_client

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
            return None  # raise exception?
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
    def new_local_file_proxy(self, proxy):
        new_ctx = FlyteContext(parent=self, local_file_access=proxy)
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
    def new_compilation_state(self) -> Generator['FlyteContext', None, None]:
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


default_context = FlyteContext(local_file_access=_local_file_proxy.LocalFileProxy(_sdk_config.LOCAL_SANDBOX.get()))
FlyteContext.OBJS.append(default_context)
