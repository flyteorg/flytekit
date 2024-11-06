import pytest
from flytekit.core.task import task
from flytekit.utils.asyn import loop_manager
from flytekit.experimental.eager_function import eager
from flytekit.core.context_manager import FlyteContextManager
from flytekit.configuration import Config, DataConfig, S3Config
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.remote.remote import FlyteRemote


@task
def add_one(x: int) -> int:
    return x + 1


# @task
# async def double(x: int) -> int:
#     return x * 2


@eager
async def simple_eager_workflow(x: int) -> int:
    # This is the normal way of calling tasks. Call normal tasks in an effectively async way by hanging and waiting for
    # the result.
    out = add_one(x=x)
    return out

    # This is the pattern for async tasks.
    # doubled = double(x=x)
    # other_double = double(x=x)
    # doubled, other_double = asyncio.gather(doubled, other_double)
    # if out - doubled < 0:
    #     return -1
    # return await double(x=out)


@pytest.mark.asyncio
async def test_easy_1():
    print('hi')
    res = await simple_eager_workflow(x=1)
    print(res)


@pytest.mark.sandbox
def test_easy_2():
    ctx = FlyteContextManager.current_context()
    remote = FlyteRemote(Config.for_sandbox())
    dc = Config.for_sandbox().data_config
    raw_output = f"s3://my-s3-bucket/testing/async_test/raw_output/"
    print(f"Using raw output location: {raw_output}")
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output, data_config=dc)

    with FlyteContextManager.with_context(ctx.with_file_access(provider).with_client(remote.client)) as ctx:
        res = loop_manager.run_sync(simple_eager_workflow.run_with_backend, ctx, x=1)
        print(res)
