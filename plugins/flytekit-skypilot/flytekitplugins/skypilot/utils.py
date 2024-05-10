from asyncio.subprocess import PIPE
from decimal import ROUND_CEILING, Decimal
from typing import Optional, Tuple, Any, Dict


from flyteidl.core.execution_pb2 import TaskExecution
from typing import List
from flytekit import FlyteContextManager
import flytekit
from flytekitplugins.skypilot.cloud_registry import BaseCloudCredentialProvider, \
    CloudRegistry, CloudCredentialError, CloudNotInstalledError
from flytekit.core.resources import Resources
from sky.skylet.job_lib import JobStatus
import subprocess
import rich_click as _click

SKYPILOT_STATUS_TO_FLYTE_PHASE = {
    "INIT": TaskExecution.RUNNING,
    "PENDING": TaskExecution.RUNNING,
    "SETTING_UP": TaskExecution.RUNNING,
    "RUNNING": TaskExecution.RUNNING,
    "SUCCEEDED": TaskExecution.SUCCEEDED,
    "FAILED": TaskExecution.FAILED,
    "FAILED_SETUP": TaskExecution.FAILED,
    "CANCELLED": TaskExecution.FAILED,
}


def skypilot_status_to_flyte_phase(status: JobStatus) -> TaskExecution.Phase:
    """
    Map Skypilot status to Flyte phase.
    """
    return SKYPILOT_STATUS_TO_FLYTE_PHASE[status.value]


# use these commands from entrypoint to help resolve the task_template.container.args
@_click.group()
def _pass_through():
    pass


@_pass_through.command("pyflyte-execute")
@_click.option("--inputs", required=True)
@_click.option("--output-prefix", required=True)
@_click.option("--raw-output-data-prefix", required=False)
@_click.option("--checkpoint-path", required=False)
@_click.option("--prev-checkpoint", required=False)
@_click.option("--test", is_flag=True)
@_click.option("--dynamic-addl-distro", required=False)
@_click.option("--dynamic-dest-dir", required=False)
@_click.option("--resolver", required=False)
@_click.argument(
    "resolver-args",
    type=_click.UNPROCESSED,
    nargs=-1,
)
def execute_task_cmd(
    inputs,
    output_prefix,
    raw_output_data_prefix,
    test,
    prev_checkpoint,
    checkpoint_path,
    dynamic_addl_distro,
    dynamic_dest_dir,
    resolver,
    resolver_args,
):
    pass


@_pass_through.command("pyflyte-fast-execute")
@_click.option("--additional-distribution", required=False)
@_click.option("--dest-dir", required=False)
@_click.argument("task-execute-cmd", nargs=-1, type=_click.UNPROCESSED)
def fast_execute_task_cmd(additional_distribution: str, dest_dir: str, task_execute_cmd: List[str]):
    # Insert the call to fast before the unbounded resolver args
    pass
     


@_pass_through.command("pyflyte-map-execute")
@_click.option("--inputs", required=True)
@_click.option("--output-prefix", required=True)
@_click.option("--raw-output-data-prefix", required=False)
@_click.option("--max-concurrency", type=int, required=False)
@_click.option("--test", is_flag=True)
@_click.option("--dynamic-addl-distro", required=False)
@_click.option("--dynamic-dest-dir", required=False)
@_click.option("--resolver", required=True)
@_click.option("--checkpoint-path", required=False)
@_click.option("--prev-checkpoint", required=False)
@_click.argument(
    "resolver-args",
    type=_click.UNPROCESSED,
    nargs=-1,
)
def map_execute_task_cmd(
    inputs,
    output_prefix,
    raw_output_data_prefix,
    max_concurrency,
    test,
    dynamic_addl_distro,
    dynamic_dest_dir,
    resolver,
    resolver_args,
    prev_checkpoint,
    checkpoint_path,
):
    pass


ENTRYPOINT_MAP = {
    execute_task_cmd.name: execute_task_cmd,
    fast_execute_task_cmd.name: fast_execute_task_cmd,
    map_execute_task_cmd.name: map_execute_task_cmd,
}

def execute_cmd_to_path(cmd: List[str]) -> Dict[str, Any]:
    assert len(cmd) > 0
    args = {}
    for entrypoint_name, cmd_entrypoint in ENTRYPOINT_MAP.items():
        if entrypoint_name == cmd[0]:
            ctx = cmd_entrypoint.make_context(info_name="", args=cmd[1:])
            args.update(ctx.params)
            if cmd_entrypoint.name == fast_execute_task_cmd.name:
                pyflyte_ctx = ENTRYPOINT_MAP[ctx.params["task_execute_cmd"][0]].make_context(
                    info_name="", 
                    args=list(ctx.params["task_execute_cmd"])[1:]
                )
                args.update(pyflyte_ctx.params)
            break
    
    # raise error if args is empty or cannot find raw_output_data_prefix
    if not args or args.get("raw_output_data_prefix", None) is None:
        raise ValueError(f"Bad command for {cmd}")
    return args
        