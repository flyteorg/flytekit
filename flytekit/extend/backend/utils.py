import asyncio
import functools

import grpc
from flyteidl.admin.agent_pb2 import PERMANENT_FAILURE, RETRYABLE_FAILURE, RUNNING, SUCCEEDED, State

import flytekit
from flytekit.extend.backend.base_agent import AsyncAgentBase
from flytekit.models.task import TaskTemplate


def async_wrapper(func):
    """Given a function, make so can be called in async or blocking contexts

    Leave obj=None if defining within a class. Pass the instance if attaching
    as an attribute of the instance.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print(args)
        print(kwargs)
        return asyncio.get_running_loop().run_in_executor(None, func, args, kwargs)

    return wrapper


def mirror_async_methods(obj):
    """Populate sync and async methods for obj

    For each method will create a sync version if the name refers to an async method
    (coroutine) and there is no override in the child class; will create an async
    method for the corresponding sync method if there is no implementation.

    Uses the methods specified in
    - async_methods: the set that an implementation is expected to provide
    - default_async_methods: that can be derived from their sync version in
      AbstractFileSystem
    - AsyncFileSystem: async-specific default coroutines
    """
    if isinstance(obj, AsyncAgentBase):
        return
    for method in dir(AsyncAgentBase):
        if not method.startswith("_"):
            continue
        smethod = method[1:]
        mth = async_wrapper(getattr(obj, smethod))
        setattr(obj, method, mth)


def convert_to_flyte_state(state: str) -> State:
    """
    Convert the state from the agent to the state in flyte.
    """
    state = state.lower()
    # timedout is the state of Databricks job. https://docs.databricks.com/en/workflows/jobs/jobs-2.0-api.html#runresultstate
    if state in ["failed", "timeout", "timedout", "canceled"]:
        return RETRYABLE_FAILURE
    elif state in ["done", "succeeded", "success"]:
        return SUCCEEDED
    elif state in ["running"]:
        return RUNNING
    raise ValueError(f"Unrecognized state: {state}")


def is_terminal_state(state: State) -> bool:
    """
    Return true if the state is terminal.
    """
    return state in [SUCCEEDED, RETRYABLE_FAILURE, PERMANENT_FAILURE]


def get_agent_secret(secret_key: str) -> str:
    return flytekit.current_context().secrets.get(secret_key)


def _get_grpc_context() -> grpc.ServicerContext:
    from unittest.mock import MagicMock

    grpc_ctx = MagicMock(spec=grpc.ServicerContext)
    return grpc_ctx


def render_task_template(tt: TaskTemplate, file_prefix: str) -> TaskTemplate:
    args = tt.container.args
    for i in range(len(args)):
        tt.container.args[i] = args[i].replace("{{.input}}", f"{file_prefix}/inputs.pb")
        tt.container.args[i] = args[i].replace("{{.outputPrefix}}", f"{file_prefix}/output")
        tt.container.args[i] = args[i].replace("{{.rawOutputDataPrefix}}", f"{file_prefix}/raw_output")
        tt.container.args[i] = args[i].replace("{{.checkpointOutputPrefix}}", f"{file_prefix}/checkpoint_output")
        tt.container.args[i] = args[i].replace("{{.prevCheckpointPrefix}}", f"{file_prefix}/prev_checkpoint")
    return tt
