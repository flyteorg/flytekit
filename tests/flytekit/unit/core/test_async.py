import pytest
from flytekit.core.task import task, eager
from flytekit.core.worker_queue import Controller
from flytekit.utils.asyn import loop_manager
from flytekit.core.context_manager import FlyteContextManager
from flytekit.configuration import Config, DataConfig, S3Config, FastSerializationSettings, ImageConfig, SerializationSettings, Image
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.tools.translator import get_serializable
from collections import OrderedDict

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


@task
def add_one(x: int) -> int:
    return x + 1


@eager(environment={"a": "b"})
async def simple_eager_workflow(x: int) -> int:
    # This is the normal way of calling tasks. Call normal tasks in an effectively async way by hanging and waiting for
    # the result.
    out = add_one(x=x)
    return out


@pytest.mark.asyncio
async def test_easy_1():
    res = await simple_eager_workflow(x=1)
    print(res)
    assert res == 2


@pytest.mark.skip
def test_easy_2():
    ctx = FlyteContextManager.current_context()
    dc = Config.for_sandbox().data_config
    raw_output = f"s3://my-s3-bucket/testing/async_test/raw_output/"
    print(f"Using raw output location: {raw_output}")
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output, data_config=dc)
    c = Controller.for_sandbox()
    c.remote._interactive_mode_enabled = True
    with FlyteContextManager.with_context(
            ctx.with_file_access(provider).with_client(c.remote.client).with_worker_queue(c)
    ):
        res = loop_manager.run_sync(simple_eager_workflow.run_with_backend, x=1)
        assert res == 2


def test_serialization():
    se_spec = get_serializable(OrderedDict(), serialization_settings, simple_eager_workflow)
    assert se_spec.template.metadata.is_eager
    assert len(se_spec.template.container.env) == 2
