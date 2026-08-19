import pytest

from flytekit import task, workflow
from flytekit.core.context_manager import FlyteContext
from flytekit.core.local_cache import LocalTaskCache
from flytekit.core.type_engine import TypeEngine
from flytekit.testing.fixtures import flyte_cache, flyte_context, flyte_tmp_dir, workflow_dry_run


class TestFlyteContextFixture:
    def test_returns_flyte_context(self, flyte_context):
        assert isinstance(flyte_context, FlyteContext)

    def test_context_has_file_access(self, flyte_context):
        assert flyte_context.file_access is not None

    def test_type_transform_with_context(self, flyte_context):
        lt = TypeEngine.to_literal_type(int)
        lv = TypeEngine.to_literal(flyte_context, 42, int, lt)
        assert lv.scalar.primitive.integer == 42


class TestFlyteCacheFixture:
    def test_cache_is_cleared(self, flyte_cache):
        assert LocalTaskCache._initialized is True

    def test_cached_task_works(self, flyte_cache):
        call_count = 0

        @task(cache=True, cache_version="test-v1")
        def add(a: int, b: int) -> int:
            nonlocal call_count
            call_count += 1
            return a + b

        result1 = add(a=1, b=2)
        result2 = add(a=1, b=2)
        assert result1 == 3
        assert result2 == 3
        assert call_count == 1  # second call should hit cache

    def test_cache_isolated_between_tests_a(self, flyte_cache):
        """First test in a pair that verifies cache isolation."""

        @task(cache=True, cache_version="isolation-v1")
        def multiply(a: int, b: int) -> int:
            return a * b

        assert multiply(a=3, b=4) == 12

    def test_cache_isolated_between_tests_b(self, flyte_cache):
        """Second test verifying the cache was cleared between tests."""
        call_count = 0

        @task(cache=True, cache_version="isolation-v1")
        def multiply(a: int, b: int) -> int:
            nonlocal call_count
            call_count += 1
            return a * b

        multiply(a=3, b=4)
        assert call_count == 1  # should NOT hit cache from previous test


class TestFlyteTmpDirFixture:
    def test_provides_path(self, flyte_tmp_dir):
        from pathlib import Path

        assert isinstance(flyte_tmp_dir, Path)
        assert flyte_tmp_dir.exists()
        assert flyte_tmp_dir.is_dir()

    def test_can_write_files(self, flyte_tmp_dir):
        test_file = flyte_tmp_dir / "test.txt"
        test_file.write_text("hello flytekit")
        assert test_file.read_text() == "hello flytekit"

    def test_can_create_subdirectories(self, flyte_tmp_dir):
        sub = flyte_tmp_dir / "subdir"
        sub.mkdir()
        assert sub.exists()


class TestWorkflowDryRun:
    def test_basic_workflow(self):
        @task
        def add_one(x: int) -> int:
            return x + 1

        @workflow
        def simple_wf(x: int) -> int:
            return add_one(x=x)

        with workflow_dry_run():
            result = simple_wf(x=5)
            assert result == 6

    def test_cached_workflow(self):
        call_count = 0

        @task(cache=True, cache_version="dry-run-v1")
        def square(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * x

        @workflow
        def square_wf(x: int) -> int:
            return square(x=x)

        with workflow_dry_run():
            result = square_wf(x=4)
            assert result == 16
