from __future__ import absolute_import, division, print_function

from flytekit.common.tasks.raw_container import SdkRawContainerTask
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import Input, Output, workflow_class

square = SdkRawContainerTask(
    input_data_dir="/var/inputs",
    output_data_dir="/var/outputs",
    inputs={"val": Types.Integer},
    outputs={"out": Types.Integer},
    image="alpine",
    command=[
        "sh",
        "-c",
        "echo $(( {{.Inputs.val}} * {{.Inputs.val}} )) | tee /var/outputs/out",
    ],
)

sum = SdkRawContainerTask(
    input_data_dir="/var/flyte/inputs",
    output_data_dir="/var/flyte/outputs",
    inputs={"x": Types.Integer, "y": Types.Integer},
    outputs={"out": Types.Integer},
    image="alpine",
    command=[
        "sh",
        "-c",
        "echo $(( {{.Inputs.x}} + {{.Inputs.y}} )) | tee /var/flyte/outputs/out",
    ],
)


@workflow_class
class RawContainerWorkflow(object):
    val1 = Input(Types.Integer)
    val2 = Input(Types.Integer)
    sq1 = square(val=val1)
    sq2 = square(val=val2)
    sm = sum(x=sq1.outputs.out, y=sq2.outputs.out)
    sum_of_squares = Output(sm.outputs.out, sdk_type=Types.Integer)
