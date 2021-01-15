from flytekit.legacy.tasks.raw_container import SdkRawContainerTask
from flytekit.legacy.sdk import Types


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


def test_raw_container_task_definition_no_outputs():
    tk = SdkRawContainerTask(
        inputs={"x": Types.Integer},
        image="my-image",
        command=["echo", "hello, world!"],
        gpu_limit="1",
        gpu_request="1",
    )
    assert not tk.serialize() is None
    task_instance = tk(x=3)
    assert task_instance.inputs[0].binding.scalar.primitive.integer == 3
