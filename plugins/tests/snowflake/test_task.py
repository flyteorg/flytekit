import contextlib
import os
import shutil
import sqlite3
import tempfile

import pandas
import pytest
from flytekitplugins.snowflake import SnowflakeConfig, SnowflakeTask

from flytekit import kwtypes, task, workflow, Secret
from flytekit.types.schema import FlyteSchema

tk = SnowflakeTask(
    "test",
    query_template="select * from tracks",
    task_config=SnowflakeConfig(
        account_name="hoa61102",
        username="haytham",
        password=Secret(group="snowflake", key="password"),
        db="DEMO_DB",
        schema="PUBLIC",
        warehouse="COMPUTE_WH"
    ),
)


def test_task_serialization():
    sql_task = SnowflakeTask(
        "test",
        query_template="select * from tracks",
        task_config=SnowflakeConfig(
            account_name="hoa61102",
            username="haytham",
            password=Secret(group="snowflake", key="password"),
            db="DEMO_DB",
            schema="PUBLIC",
            warehouse="COMPUTE_WH"
        ),
    )

    tt = sql_task.serialize_to_model(sql_task.SERIALIZE_SETTINGS)

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
        "flytekitplugins.snowflake.task.SnowflakeTaskExecutor",
    ]

    assert tt.custom["query_template"] == "select TrackId, Name from tracks limit {{.inputs.limit}}"
    assert tt.container.image != ""
