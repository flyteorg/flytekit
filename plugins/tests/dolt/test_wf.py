import os
import shutil
import tempfile

import doltcli as dolt
import pandas
import pytest
from flytekitplugins.dolt.schema import DoltConfig, DoltTable

from flytekit import task, workflow


@pytest.fixture(scope="function")
def doltdb_path():
    d = tempfile.TemporaryDirectory()
    try:
        db_path = os.path.join(d.name, "foo")
        yield db_path
    finally:
        shutil.rmtree(d.name)


@pytest.fixture(scope="function")
def dolt_config(doltdb_path):
    yield DoltConfig(
        db_path=doltdb_path,
        tablename="foo",
    )


@pytest.fixture(scope="function")
def db(doltdb_path):
    try:
        db = dolt.Dolt.init(doltdb_path)
        db.sql("create table bar (name text primary key, count bigint)")
        db.sql("insert into bar values ('Dilly', 3)")
        db.sql("select dolt_commit('-am', 'Initialize bar table')")
        yield db
    finally:
        pass


def test_dolt_table_write(db, dolt_config):
    @task
    def my_dolt(a: int) -> DoltTable:
        df = pandas.DataFrame([("Alice", a)], columns=["name", "count"])
        return DoltTable(data=df, config=dolt_config)

    @workflow
    def my_wf(a: int) -> DoltTable:
        return my_dolt(a=a)

    x = my_wf(a=5)
    assert x
    assert (x.data == pandas.DataFrame([("Alice", 5)], columns=["name", "count"])).all().all()


def test_dolt_table_read(db, dolt_config):
    @task
    def my_dolt(t: DoltTable) -> str:
        df = t.data
        return df.name.values[0]

    @workflow
    def my_wf(t: DoltTable) -> str:
        return my_dolt(t=t)

    dolt_config.tablename = "bar"
    x = my_wf(t=DoltTable(config=dolt_config))
    assert x == "Dilly"


def test_dolt_table_read_task_config(db, dolt_config):
    @task
    def my_dolt(t: DoltTable) -> str:
        df = t.data
        return df.name.values[0]

    @task
    def my_table() -> DoltTable:
        dolt_config.tablename = "bar"
        t = DoltTable(config=dolt_config)
        return t

    @workflow
    def my_wf() -> str:
        t = my_table()
        return my_dolt(t=t)

    x = my_wf()
    assert x == "Dilly"


def test_dolt_table_read_mixed_config(db, dolt_config):
    @task
    def my_dolt(t: DoltTable) -> str:
        df = t.data
        return df.name.values[0]

    @task
    def my_table(conf: DoltConfig) -> DoltTable:
        return DoltTable(config=conf)

    @workflow
    def my_wf(conf: DoltConfig) -> str:
        t = my_table(conf=conf)
        return my_dolt(t=t)

    dolt_config.tablename = "bar"
    x = my_wf(conf=dolt_config)

    assert x == "Dilly"


def test_dolt_sql_read(db, dolt_config):
    @task
    def my_dolt(t: DoltTable) -> str:
        df = t.data
        return df.name.values[0]

    @workflow
    def my_wf(t: DoltTable) -> str:
        return my_dolt(t=t)

    dolt_config.tablename = None
    dolt_config.sql = "select * from bar"
    x = my_wf(t=DoltTable(config=dolt_config))
    assert x == "Dilly"
