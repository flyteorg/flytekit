import pandas
import pyspark
from flytekitplugins.spark.task import Spark

import flytekit
from flytekit import kwtypes, task, workflow
from flytekit.types.schema import FlyteSchema


def test_wf1_with_spark():
    @task(task_config=Spark())
    def my_spark(a: int) -> (int, str):
        session = flytekit.current_context().spark_session
        assert session.sparkContext.appName == "FlyteSpark: ex:local:local:local"
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = my_spark(a=a)
        d = t2(a=y, b=b)
        return x, d

    x = my_wf(a=5, b="hello ")
    assert x == (7, "hello world")


def test_spark_dataframe_input():
    my_schema = FlyteSchema[kwtypes(name=str, age=int)]

    @task
    def my_dataset() -> my_schema:
        return pandas.DataFrame(data={"name": ["Alice"], "age": [5]})

    @task(task_config=Spark())
    def my_spark(df: pyspark.sql.DataFrame) -> my_schema:
        session = flytekit.current_context().spark_session
        new_df = session.createDataFrame([("Bob", 10)], my_schema.column_names())
        return df.union(new_df)

    @workflow
    def my_wf() -> my_schema:
        df = my_dataset()
        return my_spark(df=df)

    x = my_wf()
    assert x
    reader = x.open()
    df2 = reader.all()
    assert df2 is not None


def test_spark_dataframe_return():
    my_schema = FlyteSchema[kwtypes(name=str, age=int)]

    @task(task_config=Spark())
    def my_spark(a: int) -> my_schema:
        session = flytekit.current_context().spark_session
        df = session.createDataFrame([("Alice", a)], my_schema.column_names())
        print(type(df))
        return df

    @workflow
    def my_wf(a: int) -> my_schema:
        return my_spark(a=a)

    x = my_wf(a=5)
    reader = x.open(pandas.DataFrame)
    df2 = reader.all()
    result_df = df2.reset_index(drop=True) == pandas.DataFrame(data={"name": ["Alice"], "age": [5]}).reset_index(
        drop=True
    )
    assert result_df.all().all()
