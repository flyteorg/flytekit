import pytest

from flytekit.taskplugins.sqlite.task import SQLite3Config, SQLite3SelectTask

# https://www.sqlitetutorial.net/sqlite-sample-database/
from flytekit.types import FlyteFile

EXAMPLE_DB = "https://cdn.sqlitetutorial.net/wp-content/uploads/2018/03/chinook.zip"


def test_task_static():
    tk = SQLite3SelectTask(
        "test",
        query_template="select * from tracks",
        task_config=SQLite3Config(
            sqlite_db_mode=SQLite3Config.Mode.STATIC, static_file_uri=EXAMPLE_DB, compressed=True,
        ),
    )
    df = tk()
    assert df is not None


def test_task_dynamic():
    tk = SQLite3SelectTask("test", query_template="select * from tracks", task_config=SQLite3Config(compressed=True))
    with pytest.raises(AssertionError):
        tk()

    df = tk(sqlite=FlyteFile.from_path(EXAMPLE_DB))
    print(df)
