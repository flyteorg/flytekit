import tempfile
import typing
from contextlib import contextmanager
from pathlib import Path

import pytest

from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.local_cache import LocalTaskCache


@pytest.fixture
def flyte_context() -> FlyteContext:
    """Provide the current FlyteContext for testing.

    This eliminates the need to manually call ``FlyteContextManager.current_context()``
    in every test that needs a context for type transformations, file access, or other
    context-dependent operations.

    Usage::

        def test_type_transform(flyte_context):
            from flytekit.core.type_engine import TypeEngine
            lt = TypeEngine.to_literal_type(int)
            lv = TypeEngine.to_literal(flyte_context, 42, int, lt)
            assert lv.scalar.primitive.integer == 42
    """
    return FlyteContextManager.current_context()


@pytest.fixture
def flyte_cache():
    """Initialize and clear the local task cache before and after each test.

    Prevents stale cached results from prior test runs from leaking into the current test.
    This addresses a common pain point where ``cache=True`` on tasks causes flaky tests
    because the on-disk cache (``~/.flyte/local-cache``) persists between test runs.

    See https://github.com/flyteorg/flyte/issues/5657

    Usage::

        def test_cached_task(flyte_cache):
            @task(cache=True, cache_version="v1")
            def add(a: int, b: int) -> int:
                return a + b

            assert add(a=1, b=2) == 3
            # Cache is automatically cleared after the test
    """
    LocalTaskCache.initialize()
    LocalTaskCache.clear()
    yield
    LocalTaskCache.clear()


@pytest.fixture
def flyte_tmp_dir() -> typing.Generator[Path, None, None]:
    """Provide a temporary directory that is cleaned up after the test.

    Useful for tests involving ``FlyteFile``, ``FlyteDirectory``, or any operation
    that needs to write files to disk.

    Usage::

        def test_file_output(flyte_tmp_dir):
            output_path = flyte_tmp_dir / "result.txt"
            output_path.write_text("hello")
            assert output_path.read_text() == "hello"
    """
    with tempfile.TemporaryDirectory() as td:
        yield Path(td)


@contextmanager
def workflow_dry_run() -> typing.Generator[None, None, None]:
    """Context manager that sets up a clean local execution environment.

    Initializes and clears the local cache, then cleans up after the block completes.
    Useful for running a workflow locally in tests without worrying about cached state.

    Usage::

        from flytekit.testing.fixtures import workflow_dry_run

        def test_my_workflow():
            with workflow_dry_run():
                result = my_workflow(x=1, y=2)
                assert result == 3
    """
    LocalTaskCache.initialize()
    LocalTaskCache.clear()
    try:
        yield
    finally:
        LocalTaskCache.clear()
