from flytekit.core.task import task
from flytekit.core.workflow import workflow


def test_single_task_workflow():
    @task(cache=True, cache_version="v1")
    def is_even(n: int) -> bool:
        import time
        time.sleep(2)
        return n % 2 == 0

    @task(cache=False)
    def uncached_task(a: int, b: int) -> int:
        return a + b

    @workflow
    def check_evenness(n: int) -> bool:
        uncached_task(a=n, b=n)
        return is_even(n=n)

    assert check_evenness(n=1) is False
    assert check_evenness(n=8) is True


def test_shared_tasks_in_two_separate_workflows():
    @task(cache=True, cache_version="0.0.1")
    def is_even(n: int) -> bool:
        import time
        time.sleep(2)
        return n % 2 == 0

    @workflow
    def check_evenness_wf1(n: int) -> bool:
        return is_even(n=n)

    @workflow
    def check_evenness_wf2(n: int) -> bool:
        return is_even(n=n)

    assert check_evenness_wf1(n=42) is True
    assert check_evenness_wf1(n=99) is False

    # The next two executions of the *_wf2 workflow are going to
    # hit the cache for the calls to `is_even`
    assert check_evenness_wf2(n=42) is True
    assert check_evenness_wf2(n=99) is False
