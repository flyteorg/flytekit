from __future__ import absolute_import

import pytest

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.sdk.tasks import inputs, outputs, python_task
from flytekit.sdk.types import Types


def test_generic_schema():
    @inputs(a=Types.Schema())
    @outputs(b=Types.Schema())
    @python_task
    def fake_task(wf_params, a, b):
        pass


def test_typed_schema():
    @inputs(a=Types.Schema([("a", Types.Integer), ("b", Types.Integer)]))
    @outputs(b=Types.Schema([("a", Types.Integer), ("b", Types.Integer)]))
    @python_task
    def fake_task(wf_params, a, b):
        pass


def test_bad_definition():
    with pytest.raises(_user_exceptions.FlyteValueException):
        Types.Schema([])


def test_bad_column_types():
    with pytest.raises(_user_exceptions.FlyteTypeException):
        Types.Schema([("a", Types.Blob)])
    with pytest.raises(_user_exceptions.FlyteTypeException):
        Types.Schema([("a", Types.MultiPartBlob)])
    with pytest.raises(_user_exceptions.FlyteTypeException):
        Types.Schema([("a", Types.MultiPartCSV)])
    with pytest.raises(_user_exceptions.FlyteTypeException):
        Types.Schema([("a", Types.CSV)])
    with pytest.raises(_user_exceptions.FlyteTypeException):
        Types.Schema([("a", Types.Schema())])


def test_create_from_hive_query():
    s, q = Types.Schema().create_from_hive_query("SELECT * FROM table", known_location="s3://somewhere/")

    assert s.mode == "wb"
    assert s.local_path is None
    assert s.remote_location == "s3://somewhere/"
    assert "SELECT * FROM table" in q
    assert s.remote_location in q
