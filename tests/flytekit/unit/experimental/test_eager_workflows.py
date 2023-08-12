import asyncio
import typing

import hypothesis.strategies as st
import pytest
from hypothesis import given, infer, settings

from flytekit import dynamic, task, workflow
from flytekit.core.type_engine import TypeTransformerFailedError
from flytekit.experimental import EagerException, eager


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


@given(x_input=infer)
@settings(deadline=1000, max_examples=5)
def test_simple_eager_workflow(x_input: int):
    """Testing simple eager workflow with just tasks."""

    @eager
    async def eager_wf(x: int) -> int:
        out = await add_one(x=x)
        return await double(x=out)

    result = asyncio.run(eager_wf(x=x_input))
    assert result == (x_input + 1) * 2


@given(x_input=infer)
@settings(deadline=1000, max_examples=5)
def test_conditional_eager_workflow(x_input: int):
    """Test eager workfow with conditional logic."""

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


@given(x_input=infer)
@settings(deadline=1000, max_examples=5)
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


@given(x_input=infer, n_input=st.integers(min_value=1, max_value=20))
@settings(deadline=1000, max_examples=5)
def test_gather_eager_workflow(x_input: int, n_input: int):
    """Test eager workflow with asyncio gather."""

    @eager
    async def eager_wf(x: int, n: int) -> typing.List[int]:
        results = await asyncio.gather(*[add_one(x=x) for _ in range(n)])
        return results

    results = asyncio.run(eager_wf(x=x_input, n=n_input))
    assert results == [x_input + 1 for _ in range(n_input)]


@given(x_input=infer)
@settings(deadline=1000, max_examples=5)
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


@given(x_input=infer)
@settings(deadline=1000, max_examples=5)
def test_nested_eager_workflow(x_input: int):
    """Testing running nested eager workflows."""

    @eager
    async def eager_wf(x: int) -> int:
        out = await nested_eager_wf(x=x)
        return await double(x=out)

    result = asyncio.run(eager_wf(x=x_input))
    assert result == (x_input + 1) * 2


@given(x_input=infer)
@settings(deadline=1000, max_examples=5)
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


@given(x_input=infer)
@settings(deadline=1000, max_examples=5)
def test_workflow_within_eager_workflow(x_input: int):
    """Testing running a static workflow within an eager workflow."""

    @eager
    async def eager_wf(x: int) -> int:
        out = await subworkflow(x=x)
        return await double(x=out)

    result = asyncio.run(eager_wf(x=x_input))
    assert result == (x_input + 1) * 2


@given(x_input=infer)
@settings(deadline=1000, max_examples=5)
def test_local_task_eager_workflow_exception(x_input: int):
    """Testing simple eager workflow with just tasks."""

    @task
    def local_task(x: int) -> int:
        return x

    @eager
    async def eager_wf_with_local(x: int) -> int:
        return await local_task(x=x)

    with pytest.raises(TypeError):
        asyncio.run(eager_wf_with_local(x=x_input))


@given(x_input=infer)
@settings(deadline=1000, max_examples=5)
@pytest.mark.filterwarnings("ignore:coroutine 'AsyncEntity.__call__' was never awaited")
def test_local_workflow_within_eager_workflow_exception(x_input: int):
    """Cannot call a locally-defined workflow within an eager workflow"""

    @workflow
    def local_wf(x: int) -> int:
        return add_one(x=x)

    @eager
    async def eager_wf(x: int) -> int:
        out = await local_wf(x=x)
        return await double(x=out)

    with pytest.raises(TypeTransformerFailedError):
        asyncio.run(eager_wf(x=x_input))
