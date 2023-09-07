import asyncio
import shlex
import subprocess
from asyncio.subprocess import PIPE
from decimal import ROUND_CEILING, Decimal

from flyteidl.admin.agent_pb2 import PERMANENT_FAILURE, RETRYABLE_FAILURE, RUNNING, SUCCEEDED, State
from kubernetes.utils.quantity import parse_quantity

from flytekit.models.task import Resources

FLOAT_STATUS_TO_FLYTE_STATE = {
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


def float_status_to_flyte_state(status: str) -> State:
    """
    Map float status to Flyte state.
    """
    return FLOAT_STATUS_TO_FLYTE_STATE[status]


def flyte_to_float_resources(resources: Resources) -> tuple[int, int, int, int]:
    """
    Map Flyte (K8s) resources to float resources.
    """
    requests = resources.requests
    limits = resources.limits

    B_IN_GIB = 1073741824

    # Defaults
    req_cpu = Decimal(1)
    req_mem = Decimal(B_IN_GIB)

    for request in requests:
        if request.name == Resources.ResourceName.CPU:
            # float does not support cpu under 1
            req_cpu = max(req_cpu, parse_quantity(request.value))
        elif request.name == Resources.ResourceName.MEMORY:
            # float does not support mem under 1Gi
            req_mem = max(req_mem, parse_quantity(request.value))

    # Placeholders
    lim_cpu = Decimal(0)
    lim_mem = Decimal(0)

    for limit in limits:
        if limit.name == Resources.ResourceName.CPU:
            lim_cpu = parse_quantity(limit.value)
        elif limit.name == Resources.ResourceName.MEMORY:
            lim_mem = parse_quantity(limit.value)

    # Convert Decimal to int
    # Round up so that resource demands are met
    min_cpu = int(req_cpu.to_integral_value(rounding=ROUND_CEILING))
    min_mem = int(req_mem.to_integral_value(rounding=ROUND_CEILING))
    max_cpu = int(lim_cpu.to_integral_value(rounding=ROUND_CEILING))
    max_mem = int(lim_mem.to_integral_value(rounding=ROUND_CEILING))

    # Ignore resource limits if requests are greater than limits
    max_cpu = max(min_cpu, max_cpu)
    max_mem = max(min_mem, max_mem)

    # Convert B to GiB
    min_mem = (min_mem + B_IN_GIB - 1) // B_IN_GIB
    max_mem = (max_mem + B_IN_GIB - 1) // B_IN_GIB

    return min_cpu, min_mem, max_cpu, max_mem


async def async_check_output(*args, **kwargs):
    """
    This should behave similarly to subprocess.check_output().
    """
    process = await asyncio.create_subprocess_exec(*args, stdout=PIPE, stderr=PIPE, **kwargs)
    stdout, stderr = await process.communicate()
    returncode = process.returncode
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, shlex.join(args), output=stdout, stderr=stderr)
    return stdout
