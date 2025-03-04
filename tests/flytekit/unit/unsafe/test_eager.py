import pytest

from flytekit import task
from flytekit.unsafe import eager


@pytest.mark.asyncio
async def test_import_eager_from_unsafe():
    assert eager is not None

    @task
    def add_one(x: int) -> int:
        return x + 1

    @eager
    async def eager_addition(x: int) -> int:
        return add_one(x)

    res = await eager_addition(x=5)

    assert res == 6
