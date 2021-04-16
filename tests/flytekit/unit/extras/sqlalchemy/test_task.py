import os
import shutil
import tempfile
import time
from subprocess import Popen

import doltcli as dolt
import pandas
import pytest

from flytekit import Secret, current_context, kwtypes, task, workflow
from flytekit.extras.sqlalchemy.task import SQLAlchemyConfig, SQLAlchemyTask
from flytekit.types.schema import FlyteSchema

OK_EXAMPLE_DB = "mysql+pymysql://root@localhost:3307/tracks"
BAD_EXAMPLE_DB = "mysql+pymysql://root1@localhost/tracks"

# This task belongs to test_task_static but is intentionally here to help test tracking
tk = SQLAlchemyTask(
    "test",
    query_template="select * from tracks",
    task_config=SQLAlchemyConfig(
        uri=OK_EXAMPLE_DB,
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
        p = Popen(args=["dolt", "sql-server", "-l", "trace", "--port", "3307"], cwd=db_path)
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
            uri=OK_EXAMPLE_DB,
        ),
    )

    assert sql_task.output_columns is not None
    df = sql_task(limit=1)
    assert df is not None


def test_workflow(sql_server):
    @task
    def my_task(df: pandas.DataFrame) -> int:
        return len(df[df.columns[0]])

    os.environ[current_context().secrets.get_secrets_env_var("group", "key")] = "root"
    user_secret = Secret(group="group", key="key")
    sql_task = SQLAlchemyTask(
        "test",
        query_template="select * from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        task_config=SQLAlchemyConfig(
            uri=BAD_EXAMPLE_DB,
            connect_args=dict(port=3307),
            secret_connect_args=dict(
                user=user_secret,
            ),
        ),
        secret_requests=[user_secret],
    )

    @workflow
    def wf(limit: int) -> int:
        return my_task(df=sql_task(limit=limit))

    assert wf(limit=5) == 5
