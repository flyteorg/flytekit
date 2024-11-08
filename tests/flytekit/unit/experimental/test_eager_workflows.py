import asyncio
import mock
import os
import sys
import typing
from pathlib import Path

import hypothesis.strategies as st
import pytest
from hypothesis import given

from flytekit import dynamic, task, workflow

from flytekit.bin.entrypoint import _get_working_loop, _dispatch_execute
from flytekit.core import context_manager
from flytekit.core.promise import VoidPromise
from flytekit.exceptions.user import FlyteValidationException
from flytekit.experimental import EagerException, eager
from flytekit.models import literals as _literal_models
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.types.structured import StructuredDataset

INTEGER_ST = st.integers(min_value=-10_000_000, max_value=10_000_000)


@task
def add_one(x: int) -> int:
    return x + 1


@task
def double(x: int) -> int:
    return x * 2


@task
def gt_0(x: int) -> bool:
    return x > 0


@task
def raises_exc(x: int) -> int:
    if x == 0:
        raise TypeError
    return x


@dynamic
def dynamic_wf(x: int) -> int:
    out = add_one(x=x)
    return double(x=out)


@given(x_input=INTEGER_ST)
@pytest.mark.hypothesis
def test_simple_eager_workflow(x_input: int):
    """Testing simple eager workflow with just tasks."""

    @eager
    async def eager_wf(x: int) -> int:
        out = await add_one(x=x)
        return await double(x=out)

    result = asyncio.run(eager_wf(x=x_input))
    assert result == (x_input + 1) * 2


@given(x_input=INTEGER_ST)
@pytest.mark.hypothesis
def test_conditional_eager_workflow(x_input: int):
    """Test eager workflow with conditional logic."""

    @eager
    async def eager_wf(x: int) -> int:
        if await gt_0(x=x):
            return -1
        return 1

    result = asyncio.run(eager_wf(x=x_input))
    if x_input > 0:
        assert result == -1
    else:
        assert result == 1


@given(x_input=INTEGER_ST)
@pytest.mark.hypothesis
def test_try_except_eager_workflow(x_input: int):
    """Test eager workflow with try/except logic."""

    @eager
    async def eager_wf(x: int) -> int:
        try:
            return await raises_exc(x=x)
        except EagerException:
            return -1

    result = asyncio.run(eager_wf(x=x_input))
    if x_input == 0:
        assert result == -1
    else:
        assert result == x_input


@given(x_input=INTEGER_ST, n_input=st.integers(min_value=1, max_value=20))
@pytest.mark.hypothesis
def test_gather_eager_workflow(x_input: int, n_input: int):
    """Test eager workflow with asyncio gather."""

    @eager
    async def eager_wf(x: int, n: int) -> typing.List[int]:
        results = await asyncio.gather(*[add_one(x=x) for _ in range(n)])
        return results

    results = asyncio.run(eager_wf(x=x_input, n=n_input))
    assert results == [x_input + 1 for _ in range(n_input)]


@given(x_input=INTEGER_ST)
@pytest.mark.hypothesis
def test_eager_workflow_with_dynamic_exception(x_input: int):
    """Test eager workflow with dynamic workflow is not supported."""

    @eager
    async def eager_wf(x: int) -> typing.List[int]:
        return await dynamic_wf(x=x)

    with pytest.raises(EagerException, match="Eager workflows currently do not work with dynamic workflows"):
        asyncio.run(eager_wf(x=x_input))


@eager
async def nested_eager_wf(x: int) -> int:
    return await add_one(x=x)


@given(x_input=INTEGER_ST)
@pytest.mark.hypothesis
def test_nested_eager_workflow(x_input: int):
    """Testing running nested eager workflows."""

    @eager
    async def eager_wf(x: int) -> int:
        out = await nested_eager_wf(x=x)
        return await double(x=out)

    result = asyncio.run(eager_wf(x=x_input))
    assert result == (x_input + 1) * 2


@given(x_input=INTEGER_ST)
@pytest.mark.hypothesis
def test_eager_workflow_within_workflow(x_input: int):
    """Testing running eager workflow within a static workflow."""

    @eager
    async def eager_wf(x: int) -> int:
        return await add_one(x=x)

    @workflow
    def wf(x: int) -> int:
        out = eager_wf(x=x)
        return double(x=out)

    result = wf(x=x_input)
    assert result == (x_input + 1) * 2


@workflow
def subworkflow(x: int) -> int:
    return add_one(x=x)


@given(x_input=INTEGER_ST)
@pytest.mark.hypothesis
def test_workflow_within_eager_workflow(x_input: int):
    """Testing running a static workflow within an eager workflow."""

    @eager
    async def eager_wf(x: int) -> int:
        out = await subworkflow(x=x)
        return await double(x=out)

    result = asyncio.run(eager_wf(x=x_input))
    assert result == (x_input + 1) * 2


@given(x_input=INTEGER_ST)
@pytest.mark.hypothesis
def test_local_task_eager_workflow_exception(x_input: int):
    """Testing simple eager workflow with a local function task doesn't work."""

    @task
    def local_task(x: int) -> int:
        return x

    @eager
    async def eager_wf_with_local(x: int) -> int:
        return await local_task(x=x)

    with pytest.raises(TypeError):
        asyncio.run(eager_wf_with_local(x=x_input))


@given(x_input=INTEGER_ST)
@pytest.mark.filterwarnings("ignore:coroutine 'AsyncEntity.__call__' was never awaited")
@pytest.mark.hypothesis
def test_local_workflow_within_eager_workflow_exception(x_input: int):
    """Cannot call a locally-defined workflow within an eager workflow"""

    @workflow
    def local_wf(x: int) -> int:
        return add_one(x=x)

    @eager
    async def eager_wf(x: int) -> int:
        out = await local_wf(x=x)
        return await double(x=out)

    with pytest.raises(FlyteValidationException):
        asyncio.run(eager_wf(x=x_input))


@task
def create_structured_dataset() -> StructuredDataset:
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2, 3]})
    return StructuredDataset(dataframe=df)


@task
def create_file() -> FlyteFile:
    fname = "/tmp/flytekit_test_file"
    with open(fname, "w") as fh:
        fh.write("some data\n")
    return FlyteFile(path=fname)


@task
def create_directory() -> FlyteDirectory:
    dirname = "/tmp/flytekit_test_dir"
    Path(dirname).mkdir(exist_ok=True, parents=True)
    with open(os.path.join(dirname, "file"), "w") as tmp:
        tmp.write("some data\n")
    return FlyteDirectory(path=dirname)


@pytest.mark.skipif("pandas" not in sys.modules, reason="Pandas is not installed.")
@pytest.mark.hypothesis
def test_eager_workflow_with_offloaded_types():
    """Test eager workflow that eager workflows work with offloaded types."""
    import pandas as pd

    @eager
    async def eager_wf_structured_dataset() -> int:
        dataset = await create_structured_dataset()
        df = dataset.open(pd.DataFrame).all()
        return df["a"].sum()

    @eager
    async def eager_wf_flyte_file() -> str:
        file = await create_file()
        with open(file.path) as f:
            data = f.read().strip()
        return data

    @eager
    async def eager_wf_flyte_directory() -> str:
        directory = await create_directory()
        with open(os.path.join(directory.path, "file")) as f:
            data = f.read().strip()
        return data

    result = asyncio.run(eager_wf_structured_dataset())
    assert result == 6

    result = asyncio.run(eager_wf_flyte_file())
    assert result == "some data"

    result = asyncio.run(eager_wf_flyte_directory())
    assert result == "some data"


@mock.patch("flytekit.core.utils.load_proto_from_file")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.get_data")
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.put_data")
@mock.patch("flytekit.core.utils.write_proto_to_file")
def test_eager_workflow_dispatch(mock_write_to_file, mock_put_data, mock_get_data, mock_load_proto, event_loop):
    """Test that event loop is preserved after executing eager workflow via dispatch."""

    @eager
    async def eager_wf():
        await asyncio.sleep(0.1)
        return

    ctx = context_manager.FlyteContext.current_context()
    with context_manager.FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ) as ctx:
        _dispatch_execute(ctx, lambda: eager_wf, "inputs path", "outputs prefix")
        loop_after_execute = asyncio.get_event_loop_policy().get_event_loop()
        assert event_loop == loop_after_execute
