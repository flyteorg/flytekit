import os
import pandas
import pytest
import shutil
from subprocess import Popen
import tempfile
import time

import doltcli as dolt

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
def sql_server():
    p = None
    try:
        d = tempfile.TemporaryDirectory()
        db_path = os.path.join(d.name, "tracks")
        db = dolt.Dolt.init(db_path)
        db.sql("create table tracks (TrackId bigint, Name text)")
        db.sql("insert into tracks values (0, 'Sue'), (1, 'L'), (2, 'M'), (3, 'Ji'), (4, 'Po')")
        db.sql("select dolt_commit('-am', 'Init tracks')")
        p = Popen(args=["dolt", "sql-server", "-l", "trace"], cwd=db_path)
        time.sleep(1)
        yield db
    finally:
        if p is not None:
            p.kill()
        if os.path.exists(d.name):
            shutil.rmtree(d.name)

def test_task_static(sql_server):
    assert tk.output_columns is None

    df = tk()
    assert df is not None


def test_task_schema(sql_server):
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


def test_workflow(sql_server):
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
