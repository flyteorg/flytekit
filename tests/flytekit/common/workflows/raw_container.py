from __future__ import absolute_import, division, print_function

from flytekit.common.tasks.raw_container import SdkRawContainerTask
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input

square = SdkRawContainerTask(
    inputs={"val": Types.Integer},
    outputs={"out": Types.Integer},
    image="alpine",
    command=["bash", "-c", "cd /var/flyte/data; mkdir outputs; paste ./inputs/val | awk '{print ($1 + $1)}' > ./outputs/out"],
)

echo = SdkRawContainerTask(
    inputs={"x": Types.Integer},
    image="alpine",
    command=["echo", "{{.inputs.x}}"],
)


@workflow_class
class RawContainerWorkflow(object):
    val = Input(Types.Integer)
    sq = square(val=val)
    ec = echo(x=sq.outputs.out)
