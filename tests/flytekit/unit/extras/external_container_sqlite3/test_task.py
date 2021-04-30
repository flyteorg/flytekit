import pandas

from flytekit import kwtypes, task, workflow
from flytekit.extras.external_container_sqlite3.task import SQLite3Config, SQLite3Task

# https://www.sqlitetutorial.net/sqlite-sample-database/
from flytekit.types.schema import FlyteSchema

EXAMPLE_DB = "https://cdn.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"


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


def test_task_serialization():
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
        "flytekit.core.python_third_party_task.default_task_template_resolver",
        "--",
        "{{.taskTemplatePath}}",
        "flytekit.extras.external_container_sqlite3.task.SQLite3TaskExecutor",
    ]

    assert tt.custom["query_template"] == "select TrackId, Name from tracks limit {{.inputs.limit}}"
    assert tt.container.image != ""
