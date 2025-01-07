import asyncio
import typing
import pytest

from flytekit.core.task import task, eager
from flytekit import task, ImageSpec, eager
from flytekit.configuration import (
    SerializationSettings,
    StatsConfig,
    Config,
)
from flytekit.core.context_manager import (
    ExecutionParameters,
    ExecutionState,
    OutputMetadataTracker,
    FlyteContextManager,
)
from flytekit.core.data_persistence import FileAccessProvider

from flytekit.core.context_manager import FlyteContextManager
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


@eager
async def base_wf(x: int) -> int:
    out = add_one(x=x)
    doubled = a_double(x=x)
    if out - await doubled < 0:
        return -1
    final = double(x=out)
    return final


@eager
async def parent_wf(a: int, b: int) -> typing.Tuple[int, int]:
    print("hi")
    t1 = asyncio.create_task(base_wf(x=a))
    t2 = asyncio.create_task(base_wf(x=b))
    # Test this again in the future
    # currently behaving as python does.
    # print("hi2", flush=True)
    # await asyncio.sleep(0.01)
    # time.sleep(5)
    # Since eager workflows are also async tasks, we can use the general async pattern with them.
    i1, i2 = await asyncio.gather(t1, t2)
    return i1, i2


@pytest.mark.asyncio
async def test_nested_all_local():
    res = await parent_wf(a=1, b=2)
    print(res)
    assert res == (4, -1)


@pytest.mark.skip
def test_nested_local_backend():
    ctx = FlyteContextManager.current_context()
    remote = FlyteRemote(Config.for_sandbox())
    dc = Config.for_sandbox().data_config
    raw_output = f"s3://my-s3-bucket/testing/async_test/raw_output/"
    print(f"Using raw output location: {raw_output}")
    provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output, data_config=dc)

    with FlyteContextManager.with_context(ctx.with_file_access(provider).with_client(remote.client)):
        res = loop_manager.run_sync(parent_wf.run_with_backend, a=1, b=100)
        print(res)
        # Nested eagers just run against the backend like any other task.
        assert res == (42, 44)


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

    # don't forget the comma
    i2, = await asyncio.gather(t2)
    return i1, i2


@pytest.mark.asyncio
async def test_nested_level_1_local():
    res = await level_1()
    print(res)
    assert res == (6, 6)


@pytest.mark.skip
def test_nested_local_backend_level():
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


@pytest.mark.skip
def test_local_sandbox_run():
    # export FLYTE_PLATFORM_URL='flyte-sandbox-grpc.flyte:8089'
    # export FLYTE_PLATFORM_INSECURE=true
    # export FLYTE_AWS_ENDPOINT: 'http://flyte-sandbox-minio.flyte:9000'
    # export FLYTE_AWS_ACCESS_KEY_ID: minio
    # export FLYTE_AWS_SECRET_ACCESS_KEY: miniostorage

    # create the right mode for execution (taken from setup_execution in entrypoint)

    remote = FlyteRemote(config=Config.for_sandbox(), default_project="flytesnacks", default_domain="development")

    exe_name = "test_local_run_1"
    user_workspace_dir = "/Users/ytong/temp/user_space"
    raw_data_dir = "/Users/ytong/temp/raw_data"
    checkpoints = "/Users/ytong/temp/checkpoints"
    task_name = train.name

    execution_parameters = ExecutionParameters(
        execution_id=_identifier.WorkflowExecutionIdentifier(
            project=remote.default_project,
            domain=remote.default_domain,
            name=exe_name,
        ),
        execution_date=datetime.datetime.now(datetime.timezone.utc),
        stats=_get_stats(
            cfg=StatsConfig.auto(),
            # Stats metric path will be:
            # registration_project.registration_domain.app.module.task_name.user_stats
            # and it will be tagged with execution-level values for project/domain/wf/lp
            prefix=f"{remote.default_project}.{remote.default_domain}.{task_name}.user_stats",
            tags={
                "exec_project": remote.default_project,
                "exec_domain": remote.default_domain,
                "exec_workflow": task_name,
                "exec_launchplan": task_name,
                "api_version": flytekit._version.version,
            },
        ),
        logging=user_space_logger,
        tmp_dir=user_workspace_dir,
        raw_output_prefix=raw_data_dir,
        output_metadata_prefix=raw_data_dir,
        checkpoint=checkpoints,
        task_id=_identifier.Identifier(_identifier.ResourceType.TASK, remote.default_project, remote.default_domain, task_name, "v123123"),
    )

    metadata = {
        "flyte-execution-project": remote.default_project,
        "flyte-execution-domain": remote.default_domain,
        "flyte-execution-launchplan": task_name,
        "flyte-execution-workflow": task_name,
        "flyte-execution-name": exe_name,
    }
    try:
        file_access = FileAccessProvider(
            local_sandbox_dir=user_workspace_dir,
            raw_output_prefix=raw_data_dir,
            execution_metadata=metadata,
        )
    except TypeError:  # would be thrown from DataPersistencePlugins.find_plugin
        logger.error(f"No data plugin found for raw output prefix {raw_data_dir}")
        raise

    ctx = FlyteContextManager.current_context()
    ctx = ctx.new_builder().with_file_access(file_access).build()

    es = ctx.new_execution_state().with_params(
        mode=ExecutionState.Mode.EAGER_EXECUTION,
        user_space_params=execution_parameters,
    )
    # create new output metadata tracker
    omt = OutputMetadataTracker()
    # This is copied from the registered task. In the truly interactive case, we'd have to reconstruct this.
    ss_txt = "H4sIAAAAAAAC/+1UTW/bMAz9K4HPs5XaTroF2KVrtmLL0sBLP7BhMGSZVrTIkqsPF27R/z5JTtfssOtO9cGg+B75SJryY8RaTKEkUjSMRovJY1RDgy03ZQCCR+DWG89I9GYSNXfCe4hKGj4YSKSiKFh/jD0LRIN91qgbsuQkjTk2oIO/ZtRbi4mwnD85R5DTzvHj/wi+qHzdrAf9BebF9+u3m2GfLi+uZz09EqU7J8skume8jkHUgHup0GDKzlackSPVf6d6lX+VH+V/+m3vlPwFxPuisLxaYLLXgSxbzMS4/T1w2bUgwgL3oDSTATk7z26hKNZ30+n2CtKb+AzbNEiB6A867kCZKRV00od4sBvMToqSCQOqU+DeHkGyM6h3gahiAo2cLEzgcKvKniljMXeUUklp/goKRKxNqUExzNkDNq5IdzKGCarD/wMErjjUzjbKgu/RjYOJkVmzsYqQOQxLG8UqG0AuSWB5hs4WCLVDrLO4smQPBh1NDh0NC52fzjYfPt6uNtuL1eVyuSwuV5+Lb+tP2/x9eJAvuK5PTudpNW3ypsH5u6yqgUynOclhPsNQk8RgldCHyH8tLa0i8NL8lWtWuyWQgiIqkVYEuVnvbJUQ2SIrXMWYuZIqSweDrGMfyI4ZPf0GkD/aSXQFAAA="
    ss = SerializationSettings.from_transport(ss_txt)
    cb = ctx.new_builder().with_execution_state(es).with_output_metadata_tracker(omt).with_serialization_settings(ss)

    with FlyteContextManager.with_context(cb) as ctx:
        best = train.run(remote, ss)
        print(f"completed training with best value: {best:.4f}")

        """
        prints
        - trial #11: 	9.97
        - trial #7: 	6.39
        - trial #5: 	5.83
        - trial #9: 	4.21
        - trial #3: 	4.16
        - trial #49: 	3.93
        - trial #47: 	3.85
        completed training with best value: 3.8489
        """