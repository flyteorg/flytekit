import contextlib
import os
import shutil
import sqlite3
import tempfile
from typing import Iterator

import pandas
import pytest
from flytekitplugins.sqlalchemy import SQLAlchemyConfig, SQLAlchemyTask
from flytekitplugins.sqlalchemy.task import SQLAlchemyTaskExecutor

from flytekit import kwtypes, task, workflow
from flytekit.core.context_manager import SecretsManager
from flytekit.models.security import Secret
from flytekit.types.schema import FlyteSchema

tk = SQLAlchemyTask(
    "test",
    query_template="select * from tracks",
    task_config=SQLAlchemyConfig(
        uri="sqlite://",
    ),
)


@pytest.fixture(scope="function")
def sql_server() -> Iterator[str]:
    d = tempfile.TemporaryDirectory()
    try:
        db_path = os.path.join(d.name, "tracks.db")
        with contextlib.closing(sqlite3.connect(db_path)) as con:
            con.execute("create table tracks (TrackId bigint, Name text)")
            con.execute("insert into tracks values (0, 'Sue'), (1, 'L'), (2, 'M'), (3, 'Ji'), (4, 'Po')")
            con.commit()
        yield f"sqlite:///{db_path}"
    finally:
        if os.path.exists(d.name):
            shutil.rmtree(d.name)


def test_task_static(sql_server):
    tk = SQLAlchemyTask(
        "test",
        query_template="select * from tracks",
        task_config=SQLAlchemyConfig(
            uri=sql_server,
        ),
    )

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
            uri=sql_server,
        ),
    )

    assert sql_task.output_columns is not None
    df = sql_task(limit=1)
    assert df is not None


def test_workflow(sql_server):
    @task
    def my_task(df: pandas.DataFrame) -> int:
        return len(df[df.columns[0]])

    insert_task = SQLAlchemyTask(
        "test",
        query_template="insert into tracks values (5, 'flyte')",
        output_schema_type=None,
        task_config=SQLAlchemyConfig(uri=sql_server),
    )

    sql_task = SQLAlchemyTask(
        "test",
        query_template="select * from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        task_config=SQLAlchemyConfig(uri=sql_server),
    )

    @workflow
    def wf(limit: int) -> int:
        insert_task()
        return my_task(df=sql_task(limit=limit))

    assert wf(limit=10) == 6


def test_task_serialization(sql_server):
    sql_task = SQLAlchemyTask(
        "test",
        query_template="select TrackId, Name from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        output_schema_type=FlyteSchema[kwtypes(TrackId=int, Name=str)],
        task_config=SQLAlchemyConfig(uri=sql_server),
    )

    tt = sql_task.serialize_to_model(sql_task.SERIALIZE_SETTINGS)

    assert tt.container is not None

    assert tt.container.args == [
        "pyflyte-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--resolver",
        "flytekit.core.python_customized_container_task.default_task_template_resolver",
        "--",
        "{{.taskTemplatePath}}",
        "flytekitplugins.sqlalchemy.task.SQLAlchemyTaskExecutor",
    ]

    assert tt.custom["query_template"] == "select TrackId, Name from tracks limit {{.inputs.limit}}"
    assert tt.container.image != ""


def test_task_serialization_deserialization_with_secret(sql_server):
    secret_group = "foo"
    secret_name = "bar"

    sec = SecretsManager()
    os.environ[sec.get_secrets_env_var(secret_group, secret_name)] = "IMMEDIATE"

    sql_task = SQLAlchemyTask(
        "test",
        query_template="select 1;",
        inputs=kwtypes(limit=int),
        output_schema_type=FlyteSchema[kwtypes(TrackId=int, Name=str)],
        task_config=SQLAlchemyConfig(
            uri=sql_server,
            # As sqlite3 doesn't really support passwords, we pass another connect_arg as a secret
            secret_connect_args={"isolation_level": Secret(group=secret_group, key=secret_name)},
        ),
    )

    tt = sql_task.serialize_to_model(sql_task.SERIALIZE_SETTINGS)

    assert tt.container is not None

    assert tt.container.args == [
        "pyflyte-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--resolver",
        "flytekit.core.python_customized_container_task.default_task_template_resolver",
        "--",
        "{{.taskTemplatePath}}",
        "flytekitplugins.sqlalchemy.task.SQLAlchemyTaskExecutor",
    ]

    assert tt.custom["query_template"] == "select 1;"
    assert tt.container.image != ""

    assert "secret_connect_args" in tt.custom
    assert "isolation_level" in tt.custom["secret_connect_args"]
    assert tt.custom["secret_connect_args"]["isolation_level"]["group"] == secret_group
    assert tt.custom["secret_connect_args"]["isolation_level"]["key"] == secret_name
    assert tt.custom["secret_connect_args"]["isolation_level"]["group_version"] is None
    assert tt.custom["secret_connect_args"]["isolation_level"]["mount_requirement"] == 0

    executor = SQLAlchemyTaskExecutor()
    r = executor.execute_from_model(tt)

    assert r.iat[0, 0] == 1
