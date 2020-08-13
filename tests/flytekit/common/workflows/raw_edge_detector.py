from flytekit.common.tasks.raw_container import SdkRawContainerTask
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import Input, Output, workflow_class

edges = SdkRawContainerTask(
    input_data_dir="/inputs",
    output_data_dir="/outputs",
    inputs={"image": Types.Blob, "script": Types.Blob},
    outputs={"edges": Types.Blob},
    image="jjanzic/docker-python3-opencv",
    command=["python", "{{.inputs.script}}", "/inputs/image", "/outputs/edges"],
)


@workflow_class
class EdgeDetector(object):
    script = Input(Types.Blob)
    image = Input(Types.Blob)
    edge_task = edges(script=script, image=image)
    out = Output(edge_task.outputs.edges, sdk_type=Types.Blob)
