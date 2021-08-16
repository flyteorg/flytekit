from pytest import fixture

from flytekit.core.local_cache import LocalCache
from flytekit.core.task import task
from flytekit.core.workflow import workflow

# Global counter used to validate number of calls to cache
n_cached_task_calls = 0


@fixture(scope="function", autouse=True)
def setup():
    global n_cached_task_calls
    n_cached_task_calls = 0

    LocalCache.initialize()
    LocalCache.clear()


def test_single_task_workflow():
    @task(cache=True, cache_version="v1")
    def is_even(n: int) -> bool:
        global n_cached_task_calls
        n_cached_task_calls += 1
        return n % 2 == 0

    @task(cache=False)
    def uncached_task(a: int, b: int) -> int:
        return a + b

    @workflow
    def check_evenness(n: int) -> bool:
        uncached_task(a=n, b=n)
        return is_even(n=n)

    assert n_cached_task_calls == 0
    assert check_evenness(n=1) is False
    # Confirm task is called
    assert n_cached_task_calls == 1
    assert check_evenness(n=1) is False
    # Subsequent calls of the workflow with the same parameter do not bump the counter
    assert n_cached_task_calls == 1
    assert check_evenness(n=1) is False
    assert n_cached_task_calls == 1

    # Run workflow with a different parameter and confirm counter is bumped
    assert check_evenness(n=8) is True
    assert n_cached_task_calls == 2
    # Run workflow again with the same parameter and confirm the counter is not bumped
    assert check_evenness(n=8) is True
    assert n_cached_task_calls == 2


def test_shared_tasks_in_two_separate_workflows():
    @task(cache=True, cache_version="0.0.1")
    def is_odd(n: int) -> bool:
        global n_cached_task_calls
        n_cached_task_calls += 1
        return n % 2 == 1

    @workflow
    def check_oddness_wf1(n: int) -> bool:
        return is_odd(n=n)

    @workflow
    def check_oddness_wf2(n: int) -> bool:
        return is_odd(n=n)

    assert n_cached_task_calls == 0
    assert check_oddness_wf1(n=42) is False
    assert check_oddness_wf1(n=99) is True
    assert n_cached_task_calls == 2

    # The next two executions of the *_wf2 workflow are going to
    # hit the cache for the calls to `is_odd`
    assert check_oddness_wf2(n=42) is False
    assert check_oddness_wf2(n=99) is True
    assert n_cached_task_calls == 2

# TODO add test with typing.List[str]

def test_sql_task():
    sql = SQLTask(
        "my-query",
        query_template="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds }}' LIMIT 10",
        inputs=kwtypes(ds=datetime.datetime),
        outputs=kwtypes(results=FlyteSchema),
        metadata=TaskMetadata(retries=2),
    )

    @task(cache=True, cache_version="NaN")
    def t1() -> datetime.datetime:
        return datetime.datetime.now()

    @workflow
    def my_wf() -> FlyteSchema:
        dt = t1()
        return sql(ds=dt)

    with task_mock(sql) as mock:
        mock.return_value = pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})
        assert (my_wf().open().all() == pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()
