import datetime
import logging
import os
import time
from typing import TYPE_CHECKING, Any

from flytekit.configuration import Config
from flytekit.remote import FlyteRemote
from flytekit.remote.executions import FlyteWorkflowExecution

if TYPE_CHECKING:
    from flytekit.remote import FlyteRemote

logger = logging.getLogger(__name__)


def get_default_flyte_remote() -> FlyteRemote:
    return FlyteRemote(config=Config.auto())


def get_platform_url() -> str:
    return os.getenv("FLYTE_PLATFORM_URL", "localhost:30080")


def get_flyte_base_console_url() -> str:
    platform_url = get_platform_url()
    return os.environ.get("FLYTE_CONSOLE_BASE_URL", f"http://{platform_url}/console")


def get_execution_url(execution: FlyteWorkflowExecution) -> str:
    """Return execution URL if user previously called execute, raise otherwise."""
    base_url = get_flyte_base_console_url()
    return (
        f"{base_url}/projects/{execution.id.project}/domains/"
        f"{execution.id.domain}/executions/{execution.id.name}"
    )


def get_flyte_workflow_execution_outputs(
    execution: FlyteWorkflowExecution,
    remote: FlyteRemote,
    log_interval: int | None = None,
) -> Any:
    """Return outputs after workflow has completed."""
    finished_execution = wait_for_workflow_execution_to_finish(
        execution,
        log_interval=log_interval,
        remote=remote,
    )
    if finished_execution.outputs:
        output_keys = list(finished_execution.outputs.literals.keys())
        if len(output_keys) == 1:
            return finished_execution.outputs.get(output_keys[0])
        else:
            return tuple(finished_execution.outputs.get(key) for key in output_keys)
    else:
        raise ValueError("Workflow aborted.")


def wait_for_workflow_execution_to_finish(
    execution: FlyteWorkflowExecution,
    remote: FlyteRemote,
    log_interval: int | None = None,
) -> FlyteWorkflowExecution:
    """Wait for workflow to complete and return execution.

    Args:
        execution: FlyteWorkflowExecution object
        log_interval: Interval for logging (in seconds). Defaults to 60.

    Raises:
        ValueError: If workflow execution failed

    Returns:
        Synced workflow execution
    """
    log_interval = log_interval or 60
    start_time = time.time()
    log_idx = 0
    if execution.is_done:
        execution = remote.sync_execution(execution)
        return execution

    logger.info("Waiting for workflow to complete before returning output.")
    while not execution.is_done:
        current_time = time.time()
        seconds_elapsed = current_time - start_time
        if seconds_elapsed >= log_interval * log_idx:
            elapsed = datetime.timedelta(seconds=seconds_elapsed)
            logger.info(f"Syncing execution. Total time elapsed: {elapsed}.")
            log_idx += 1
        execution = remote.sync_execution(execution)
        time.sleep(5)
    if execution.error:
        raise ValueError(execution.closure.error.message)
    elapsed = datetime.timedelta(seconds=time.time() - start_time)
    logger.info(f"Workflow completed. Total time elapsed: {elapsed}")
    return execution
