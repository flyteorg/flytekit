from collections import OrderedDict

import pandas

from flytekit import kwtypes, task, workflow
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.extras.sqlite3.task import SQLite3Config, SQLite3Task

# https://www.sqlitetutorial.net/sqlite-sample-database/
from flytekit.types.schema import FlyteSchema

EXAMPLE_DB = "https://cdn.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"

# This task belongs to test_task_static but is intentionally here to help test tracking
tk = SQLite3Task(
    "test",
    query_template="select * from tracks",
    task_config=SQLite3Config(
        uri=EXAMPLE_DB,
        compressed=True,
    ),
)


def test_task_static():
    assert tk.output_columns is None

    df = tk()
    assert df is not None


def test_task_schema():
    sql_task = SQLite3Task(
        "test",
        query_template="select TrackId, Name from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        output_schema_type=FlyteSchema[kwtypes(TrackId=int, Name=str)],
        task_config=SQLite3Config(
            uri=EXAMPLE_DB,
            compressed=True,
        ),
    )

    assert sql_task.output_columns is not None
    df = sql_task(limit=1)
    assert df is not None


def test_workflow():
    @task
    def my_task(df: pandas.DataFrame) -> int:
        return len(df[df.columns[0]])

    sql_task = SQLite3Task(
        "test",
        query_template="select * from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        task_config=SQLite3Config(
            uri=EXAMPLE_DB,
            compressed=True,
        ),
    )

    @workflow
    def wf(limit: int) -> int:
        return my_task(df=sql_task(limit=limit))

    assert wf(limit=5) == 5


def test_fdsafdsa():
    sql_task = SQLite3Task(
        "test",
        query_template="select TrackId, Name from tracks limit {{.inputs.limit}}",
        inputs=kwtypes(limit=int),
        output_schema_type=FlyteSchema[kwtypes(TrackId=int, Name=str)],
        task_config=SQLite3Config(
            uri=EXAMPLE_DB,
            compressed=True,
        ),
    )
    sql_task._instantiated_in = "test.module"
    sql_task._lhs = "sample_sql_task"

    assert sql_task.output_columns is not None
    df = sql_task(limit=1)
    assert df is not None

    serialization_settings = context_manager.SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
        env={},
    )
    srz_t = get_serializable(OrderedDict(), serialization_settings, sql_task)
    print(srz_t)
    idl_task = srz_t.to_flyte_idl()
    print(type(idl_task))
    cont = sql_task.get_container(serialization_settings)
    print(f"Container ==============")
    print(cont)

    from google.protobuf import json_format as _json_format

    from flytekit.common.tasks.task import SdkTask

    new_custom = _json_format.MessageToDict(idl_task.custom)
    print(new_custom)
