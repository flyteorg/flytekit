import asyncio
import shlex
import subprocess
from asyncio.subprocess import PIPE
from decimal import ROUND_CEILING, Decimal
from typing import Optional, Tuple

from flyteidl.admin.agent_pb2 import PERMANENT_FAILURE, RETRYABLE_FAILURE, RUNNING, SUCCEEDED, State
from kubernetes.utils.quantity import parse_quantity

from flytekit.core.resources import Resources

MMCLOUD_STATUS_TO_FLYTE_STATE = {
    "Submitted": RUNNING,
    "Initializing": RUNNING,
    "Starting": RUNNING,
    "Executing": RUNNING,
    "Capturing": RUNNING,
    "Floating": RUNNING,
    "Suspended": RUNNING,
    "Suspending": RUNNING,
    "Resuming": RUNNING,
    "Completed": SUCCEEDED,
    "Cancelled": PERMANENT_FAILURE,
    "Cancelling": PERMANENT_FAILURE,
    "FailToComplete": RETRYABLE_FAILURE,
    "FailToExecute": RETRYABLE_FAILURE,
    "CheckpointFailed": RETRYABLE_FAILURE,
    "Timedout": RETRYABLE_FAILURE,
    "NoAvailableHost": RETRYABLE_FAILURE,
    "Unknown": RETRYABLE_FAILURE,
    "WaitingForLicense": PERMANENT_FAILURE,
}


def mmcloud_status_to_flyte_state(status: str) -> State:
    """
    Map MMCloud status to Flyte state.
    """
    return MMCLOUD_STATUS_TO_FLYTE_STATE[status]


def flyte_to_mmcloud_resources(
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
) -> Tuple[int, int, int, int]:
    """
    Map Flyte (K8s) resources to MMCloud resources.
    """
    B_IN_GIB = 1073741824

    # MMCloud does not support cpu under 1 or mem under 1Gi
    req_cpu = max(Decimal(1), parse_quantity(requests.cpu)) if requests and requests.cpu else None
    req_mem = max(Decimal(B_IN_GIB), parse_quantity(requests.mem)) if requests and requests.mem else None
    lim_cpu = max(Decimal(1), parse_quantity(limits.cpu)) if limits and limits.cpu else None
    lim_mem = max(Decimal(B_IN_GIB), parse_quantity(limits.mem)) if limits and limits.mem else None

    # Convert Decimal to int
    # Round up so that resource demands are met
    max_cpu = int(lim_cpu.to_integral_value(rounding=ROUND_CEILING)) if lim_cpu else None
    max_mem = int(lim_mem.to_integral_value(rounding=ROUND_CEILING)) if lim_mem else None

    # Use the maximum as the minimum if no minimum is specified
    # Use min_cpu 1 and min_mem 1Gi if neither minimum nor maximum are specified
    min_cpu = int(req_cpu.to_integral_value(rounding=ROUND_CEILING)) if req_cpu else max_cpu or 1
    min_mem = int(req_mem.to_integral_value(rounding=ROUND_CEILING)) if req_mem else max_mem or B_IN_GIB

    if min_cpu and max_cpu and min_cpu > max_cpu:
        raise ValueError("cpu request cannot be greater than cpu limit")
    if min_mem and max_mem and min_mem > max_mem:
        raise ValueError("mem request cannot be greater than mem limit")

    # Convert B to GiB
    min_mem = (min_mem + B_IN_GIB - 1) // B_IN_GIB if min_mem else None
    max_mem = (max_mem + B_IN_GIB - 1) // B_IN_GIB if max_mem else None

    return min_cpu, min_mem, max_cpu, max_mem


async def async_check_output(*args, **kwargs):
    """
    This behaves similarly to subprocess.check_output().
    """
    process = await asyncio.create_subprocess_exec(*args, stdout=PIPE, stderr=PIPE, **kwargs)
    stdout, stderr = await process.communicate()
    returncode = process.returncode
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, shlex.join(args), output=stdout, stderr=stderr)
    return stdout
