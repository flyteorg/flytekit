from __future__ import absolute_import, division, print_function

from flytekit.common.tasks.raw_container import SdkRawContainerTask
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input

square = SdkRawContainerTask(
    input_data_dir="/var/inputs",
    output_data_dir="/var/outputs",
    inputs={"val": Types.Integer},
    outputs={"out": Types.Integer},
    image="alpine",
    command=["sh", "-c", "cd /var/; paste ./inputs/val | awk '{print ($1 * $1)}' > ./outputs/out"],
)

echo = SdkRawContainerTask(
    input_data_dir="/var/flyte/inputs",
    inputs={"x": Types.Integer},
    image="alpine",
    command=["sh" "-c", "ls /var/flyte/inputs; cat /var/flyte/inputs/inputs.json"],
)


@workflow_class
class RawContainerWorkflow(object):
    val = Input(Types.Integer)
    sq = square(val=val)
    ec = echo(x=sq.outputs.out)
