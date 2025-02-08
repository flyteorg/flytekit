import asyncio
import functools
import inspect
from typing import Callable, Coroutine

from flyteidl.core.execution_pb2 import TaskExecution

import flytekit
from flytekit.models.task import TaskTemplate


def mirror_async_methods(func: Callable, **kwargs) -> Coroutine:
    if inspect.iscoroutinefunction(func):
        return func(**kwargs)
    return asyncio.get_running_loop().run_in_executor(None, functools.partial(func, **kwargs))


def convert_to_flyte_phase(state: str) -> TaskExecution.Phase:
    """
    Convert the state from the agent to the phase in flyte.
    """
    state = state.lower()
    if state in ["failed", "timeout", "timedout", "canceled", "cancelled", "skipped", "internal_error"]:
        return TaskExecution.FAILED
    elif state in ["done", "succeeded", "success", "completed"]:
        return TaskExecution.SUCCEEDED
    elif state in ["running", "terminating"]:
        return TaskExecution.RUNNING
    elif state in ["pending"]:
        return TaskExecution.INITIALIZING
    raise ValueError(f"Unrecognized state: {state}")


def is_terminal_phase(phase: TaskExecution.Phase) -> bool:
    """
    Return true if the phase is terminal.
    """
    return phase in [TaskExecution.SUCCEEDED, TaskExecution.ABORTED, TaskExecution.FAILED]


def get_agent_secret(secret_key: str) -> str:
    return flytekit.current_context().secrets.get(key=secret_key)


def render_task_template(tt: TaskTemplate, file_prefix: str) -> TaskTemplate:
    args = tt.container.args
    for i in range(len(args)):
        tt.container.args[i] = args[i].replace("{{.input}}", f"{file_prefix}/inputs.pb")
        tt.container.args[i] = args[i].replace("{{.outputPrefix}}", f"{file_prefix}")
        tt.container.args[i] = args[i].replace("{{.rawOutputDataPrefix}}", f"{file_prefix}/raw_output")
        tt.container.args[i] = args[i].replace("{{.checkpointOutputPrefix}}", f"{file_prefix}/checkpoint_output")
        tt.container.args[i] = args[i].replace("{{.prevCheckpointPrefix}}", f"{file_prefix}/prev_checkpoint")
    return tt
