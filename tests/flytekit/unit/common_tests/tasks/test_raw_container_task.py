from common.tasks.raw_container import SdkRawContainerTask
from sdk.types import Types


def test_raw_container_task_definition():
    tk = SdkRawContainerTask(
        inputs={"x": Types.Integer},
        outputs={"y": Types.Integer},
        image="my-image",
        command=["echo", "hello, world!"],
        gpu_limit="1",
        gpu_request="1",
    )
    assert not tk.serialize() is None


