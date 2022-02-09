import datetime
from dataclasses import dataclass

import pandas as pd
from dataclasses_json import dataclass_json

from flytekit import SQLTask, kwtypes
from flytekit.core import context_manager
from flytekit.core.task import TaskMetadata, task
from flytekit.core.testing import patch, task_mock
from flytekit.core.workflow import workflow
from flytekit.types.schema import FlyteSchema, SchemaOpenMode


def test_wf1_with_sql():
    sql = SQLTask(
        "my-query",
        query_template="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds }}' LIMIT 10",
        inputs=kwtypes(ds=datetime.datetime),
        outputs=kwtypes(results=FlyteSchema),
        metadata=TaskMetadata(retries=2),
    )

    @task
    def t1() -> datetime.datetime:
        return datetime.datetime.now()

    @workflow
    def my_wf() -> FlyteSchema:
        dt = t1()
        return sql(ds=dt)

    with task_mock(sql) as mock:
        mock.return_value = pd.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})
        assert (my_wf().open().all() == pd.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()
    assert context_manager.FlyteContextManager.size() == 1


def test_wf1_with_sql_with_patch():
    sql = SQLTask(
        "my-query",
        query_template="SELECT * FROM hive.city.fact_airport_sessions WHERE ds = '{{ .Inputs.ds }}' LIMIT 10",
        inputs=kwtypes(ds=datetime.datetime),
        outputs=kwtypes(results=FlyteSchema),
        metadata=TaskMetadata(retries=2),
    )

    @task
    def t1() -> datetime.datetime:
        return datetime.datetime.now()

    @workflow
    def my_wf() -> FlyteSchema:
        dt = t1()
        return sql(ds=dt)

    @patch(sql)
    def test_user_demo_test(mock_sql):
        mock_sql.return_value = pd.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})
        assert (my_wf().open().all() == pd.DataFrame(data={"x": [1, 2], "y": ["3", "4"]})).all().all()

    # Have to call because tests inside tests don't run
    test_user_demo_test()
    assert context_manager.FlyteContextManager.size() == 1


def test_wf_typed_schema():
    schema1 = FlyteSchema[kwtypes(x=int, y=str)]

    @task
    def t1() -> schema1:
        s = schema1()
        s.open().write(pd.DataFrame(data={"x": [1, 2], "y": ["3", "4"]}))
        return s

    @task
    def t2(s: FlyteSchema[kwtypes(x=int, y=str)]) -> FlyteSchema[kwtypes(x=int)]:
        df = s.open().all()
        return df[s.column_names()[:-1]]

    @workflow
    def wf() -> FlyteSchema[kwtypes(x=int)]:
        return t2(s=t1())

    w = t1()
    assert w is not None
    df = w.open(override_mode=SchemaOpenMode.READ).all()
    result_df = df.reset_index(drop=True) == pd.DataFrame(data={"x": [1, 2], "y": ["3", "4"]}).reset_index(drop=True)
    assert result_df.all().all()

    df = t2(s=w.as_readonly())
    df = df.open(override_mode=SchemaOpenMode.READ).all()
    result_df = df.reset_index(drop=True) == pd.DataFrame(data={"x": [1, 2]}).reset_index(drop=True)
    assert result_df.all().all()

    x = wf()
    df = x.open().all()
    result_df = df.reset_index(drop=True) == pd.DataFrame(data={"x": [1, 2]}).reset_index(drop=True)
    assert result_df.all().all()


def test_wf_schema_to_df():
    schema1 = FlyteSchema[kwtypes(x=int, y=str)]

    @task
    def t1() -> schema1:
        s = schema1()
        s.open().write(pd.DataFrame(data={"x": [1, 2], "y": ["3", "4"]}))
        return s

    @task
    def t2(df: pd.DataFrame) -> int:
        return len(df.columns.values)

    @workflow
    def wf() -> int:
        return t2(df=t1())

    x = wf()
    assert x == 2


def test_flyte_schema_dataclass():
    TestSchema = FlyteSchema[kwtypes(some_str=str)]

    @dataclass_json
    @dataclass
    class InnerResult:
        number: int
        schema: TestSchema

    @dataclass_json
    @dataclass
    class Result:
        result: InnerResult
        schema: TestSchema

    schema = TestSchema()

    @task
    def t1(x: int) -> Result:

        return Result(result=InnerResult(number=x, schema=schema), schema=schema)

    @workflow
    def wf(x: int) -> Result:
        return t1(x=x)

    assert wf(x=10) == Result(result=InnerResult(number=10, schema=schema), schema=schema)
