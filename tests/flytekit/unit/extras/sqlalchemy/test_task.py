import pandas
import pytest

from flytekit import kwtypes, task, workflow
from flytekit.extras.sqlalchemy.task import SQLAlchemyConfig, SQLAlchemyTask

# https://www.sqlitetutorial.net/sqlite-sample-database/
from flytekit.types.schema import FlyteSchema

EXAMPLE_DB = "mysql+pymysql://root@localhost/tracks"

# This task belongs to test_task_static but is intentionally here to help test tracking
tk = SQLAlchemyTask(
    "test",
    query_template="select * from tracks",
    task_config=SQLAlchemyConfig(
        uri=EXAMPLE_DB,
    ),
)

@pytest.fixture(scope="function")
def mysql_server(mysql):
    mysql.query("create tables tracks (TrackId bigint, Name text)")
    mysql.query("insert into tracks values (0, 'Sue'), (1, 'L'), (2, 'M'), (3, 'Ji'), (4, 'Po')")

def test_task_static(mysql_server):
    assert tk.output_columns is None

    df = tk()
    assert df is not None


def test_task_schema(mysql_server):
    sql_task = SQLAlchemyTask(
        "test",
        query_template="select TrackId, Name from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        output_schema_type=FlyteSchema[kwtypes(TrackId=int, Name=str)],
        task_config=SQLAlchemyConfig(
            uri=EXAMPLE_DB,
        ),
    )

    assert sql_task.output_columns is not None
    df = sql_task(limit=1)
    assert df is not None


def test_workflow(mysql_server):
    @task
    def my_task(df: pandas.DataFrame) -> int:
        return len(df[df.columns[0]])

    sql_task = SQLAlchemyTask(
        "test",
        query_template="select * from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        task_config=SQLAlchemyConfig(
            uri=EXAMPLE_DB,
        ),
    )

    @workflow
    def wf(limit: int) -> int:
        return my_task(df=sql_task(limit=limit))

    assert wf(limit=5) == 5
