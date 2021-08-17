import datetime
import typing
from dataclasses import dataclass

import pandas
from dataclasses_json import dataclass_json
from pytest import fixture

from flytekit import SQLTask, kwtypes
from flytekit.core.local_cache import LocalCache
from flytekit.core.task import TaskMetadata, task
from flytekit.core.testing import task_mock
from flytekit.core.workflow import workflow
from flytekit.types.schema import FlyteSchema

# Global counter used to validate number of calls to cache
n_cached_task_calls = 0


@fixture(scope="function", autouse=True)
def setup():
    global n_cached_task_calls
    n_cached_task_calls = 0

    LocalCache.initialize()
    LocalCache.clear()


def test_1():
    @task(cache=True, cache_version="v1")
    def f1(n: int) -> int:
        global n_cached_task_calls
        n_cached_task_calls += 1

        return n

    @task(cache=True, cache_version="v1")
    def f2(n: int) -> int:
        global n_cached_task_calls
        n_cached_task_calls += 1

        return n + 1

    @workflow
    def wf(n: int) -> (int, int):
        n_f1 = f1(n=n)
        n_f2 = f2(n=n)
        return n_f1, n_f2

    # This is demonstrating that calls to f1 and f2 are cached by input parameters.
    assert wf(n=1) == (1, 2)
    assert 1 == 2


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
        metadata=TaskMetadata(retries=2, cache=True, cache_version="0.1"),
    )

    @task(cache=True, cache_version="0.1.2")
    def t1() -> datetime.datetime:
        global n_cached_task_calls
        n_cached_task_calls += 1
        return datetime.datetime.now()

    @workflow
    def my_wf() -> FlyteSchema:
        dt = t1()
        return sql(ds=dt)

    with task_mock(sql) as mock:
        mock.return_value = pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})
        assert n_cached_task_calls == 0
        assert (my_wf().open().all() == pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()
        assert n_cached_task_calls == 1
        # The second and third calls hit the cache
        assert (my_wf().open().all() == pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()
        assert n_cached_task_calls == 1
        assert (my_wf().open().all() == pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()
        assert n_cached_task_calls == 1


def test_wf_custom_types():
    @dataclass_json
    @dataclass
    class MyCustomType(object):
        x: int
        y: str

    @task(cache=True, cache_version="a.b.c")
    def t1(a: int) -> MyCustomType:
        global n_cached_task_calls
        n_cached_task_calls += 1
        return MyCustomType(x=a, y="t1")

    @task(cache=True, cache_version="v1")
    def t2(a: MyCustomType, b: str) -> (MyCustomType, int):
        global n_cached_task_calls
        n_cached_task_calls += 1
        return MyCustomType(x=a.x, y=f"{a.y} {b}"), 5

    @workflow
    def my_wf(a: int, b: str) -> (MyCustomType, int):
        return t2(a=t1(a=a), b=b)

    assert n_cached_task_calls == 0
    c, v = my_wf(a=10, b="hello")
    assert v == 5
    assert c.x == 10
    assert c.y == "t1 hello"
    assert n_cached_task_calls == 2
    c, v = my_wf(a=10, b="hello")
    assert v == 5
    assert c.x == 10
    assert c.y == "t1 hello"
    assert n_cached_task_calls == 2


def test_wf_schema_to_df():
    schema1 = FlyteSchema[kwtypes(x=int, y=str)]

    @task(cache=True, cache_version="v0")
    def t1() -> schema1:
        global n_cached_task_calls
        n_cached_task_calls += 1

        s = schema1()
        s.open().write(pandas.DataFrame(data={"x": [1, 2], "y": ["3", "4"]}))
        return s

    @task(cache=True, cache_version="v1")
    def t2(df: pandas.DataFrame) -> int:
        global n_cached_task_calls
        n_cached_task_calls += 1

        return len(df.columns.values)

    @workflow
    def wf() -> int:
        return t2(df=t1())

    assert n_cached_task_calls == 0
    x = wf()
    assert x == 2
    assert n_cached_task_calls == 2
    # Second call does not bump the counter
    x = wf()
    assert x == 2
    assert n_cached_task_calls == 2


def test_dict_wf_with_constants():
    @task(cache=True, cache_version="v99")
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        global n_cached_task_calls
        n_cached_task_calls += 1

        return a + 2, "world"

    @task(cache=True, cache_version="v101")
    def t2(a: typing.Dict[str, str]) -> str:
        global n_cached_task_calls
        n_cached_task_calls += 1

        return " ".join([v for k, v in a.items()])

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = t2(a={"key1": b, "key2": y})
        return x, d

    assert n_cached_task_calls == 0
    x = my_wf(a=5, b="hello")
    assert x == (7, "hello world")
    assert n_cached_task_calls == 2
    # Second call does not bump the counter
    x = my_wf(a=5, b="hello")
    assert x == (7, "hello world")
    assert n_cached_task_calls == 2
