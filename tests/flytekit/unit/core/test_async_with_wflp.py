import asyncio
import typing
import pytest

from flytekit.core.task import task, eager
from flytekit.core.workflow import workflow
from flytekit.configuration import Config
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.remote.remote import FlyteRemote
from flytekit.utils.asyn import loop_manager


@task
def add_one(x: int) -> int:
    return x + 1


@task
async def a_double(x: int) -> int:
    return x * 2


@task
def double(x: int) -> int:
    return x * 2


@workflow
def level_3_subwf(x: int) -> int:
    out = add_one(x=x)
    return out


ctx = FlyteContextManager.current_context()
level_3_lp = LaunchPlan.get_or_create(level_3_subwf, "level_3_lp")


@eager
async def level_2(x: int) -> int:
    out = add_one(x=x)
    level_3_res = level_3_subwf(x=out)
    level_3_lp_res = level_3_lp(x=level_3_res)
    final_res = double(x=level_3_lp_res)
    return final_res


@eager
async def level_1() -> typing.Tuple[int, int]:
    i1 = add_one(x=5)
    t2 = asyncio.create_task(level_2(x=1))

    # don't forget the comma
    i2, = await asyncio.gather(t2)
    return i1, i2


@pytest.mark.asyncio
async def test_nested_level_1_local():
    res = await level_1()
    print(res)
    assert res == (6, 8)


@pytest.mark.skip
def test_nested_local_backend():
    ctx = FlyteContextManager.current_context()
    remote = FlyteRemote(Config.for_sandbox())
    dc = Config.for_sandbox().data_config
    raw_output = f"s3://my-s3-bucket/testing/async_test/raw_output/"
    print(f"Using raw output location: {raw_output}")
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output, data_config=dc)

    with FlyteContextManager.with_context(ctx.with_file_access(provider).with_client(remote.client)):
        res = loop_manager.run_sync(level_1.run_with_backend)
        print(res)
        assert res == (42, 42)
