import pytest
from flytekit.core.task import task, eager
from flytekit.core.worker_queue import Controller
from flytekit.utils.asyn import loop_manager
from flytekit.core.context_manager import FlyteContextManager
from flytekit.configuration import Config, DataConfig, S3Config, FastSerializationSettings
from flytekit.core.data_persistence import FileAccessProvider


@task
def add_one(x: int) -> int:
    return x + 1


@eager
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


@pytest.mark.sandbox_test
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
