import pytest

from flytekit.legacy.sdk import types as _sdk_types


def test_integer():
    with pytest.raises(Exception):
        _sdk_types.Types.Integer()


def test_float():
    with pytest.raises(Exception):
        _sdk_types.Types.Float()


def test_string():
    with pytest.raises(Exception):
        _sdk_types.Types.String()


def test_bool():
    with pytest.raises(Exception):
        _sdk_types.Types.Boolean()


def test_datetime():
    with pytest.raises(Exception):
        _sdk_types.Types.Datetime()


def test_timedelta():
    with pytest.raises(Exception):
        _sdk_types.Types.Timedelta()
