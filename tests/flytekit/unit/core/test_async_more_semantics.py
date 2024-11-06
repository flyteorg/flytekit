import asyncio
import typing
import pytest

from flytekit import task
from flytekit.configuration import Config
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.experimental.eager_function import eager
from flytekit.remote.remote import FlyteRemote
from flytekit.utils.asyn import loop_manager


@task
def add_one(x: int) -> int:
    return x + 1


# enable in the future
# @task
# async def a_double(x: int) -> int:
#     return x * 2

@task
def double(x: int) -> int:
    return x * 2


@eager
async def base_wf(x: int) -> int:
    out = add_one(x=x)
    doubled = double(x=x)
    if out - doubled < 0:
        return -1
    return double(x=out)


@eager
async def parent_wf() -> typing.Tuple[int, int]:
    print("hi")
    t1 = asyncio.create_task(base_wf(x=1))
    t2 = asyncio.create_task(base_wf(x=2))

    # Since eager workflows are also async tasks, we can use the general async pattern with them.
    i1, i2 = await asyncio.gather(t1, t2)
    return i1, i2


@pytest.mark.asyncio
async def test_nested_all_local():
    res = await parent_wf()
    print(res)


@pytest.mark.sandbox
def test_nested_local_backend():
    ctx = FlyteContextManager.current_context()
    remote = FlyteRemote(Config.for_sandbox())
    dc = Config.for_sandbox().data_config
    raw_output = f"s3://my-s3-bucket/testing/async_test/raw_output/"
    print(f"Using raw output location: {raw_output}")
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output, data_config=dc)

    with FlyteContextManager.with_context(ctx.with_file_access(provider).with_client(remote.client)) as ctx:
        res = loop_manager.run_sync(parent_wf.run_with_backend, ctx)
        print(res)


@eager
async def level_3(x: int) -> int:
    out = add_one(x=x)
    return out


@eager
async def level_2(x: int) -> int:
    out = add_one(x=x)
    level_3_res = await level_3(x=out)
    final_res = double(x=level_3_res)
    return final_res


@eager
async def level_1() -> typing.Tuple[int, int]:
    i1 = add_one(x=5)
    t2 = asyncio.create_task(level_2(x=1))

    i2 = await asyncio.gather(t2)
    return i1, i2
