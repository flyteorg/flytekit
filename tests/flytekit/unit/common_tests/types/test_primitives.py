from __future__ import absolute_import

import datetime

import pytest
from dateutil import tz

from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.types import base_sdk_types, primitives
from flytekit.models import types as literal_types


def test_integer():
    # Check type specification
    assert primitives.Integer.to_flyte_literal_type().simple == literal_types.SimpleType.INTEGER

    # Test value behavior
    obj = primitives.Integer.from_python_std(1)
    assert obj.to_python_std() == 1
    assert primitives.Integer.from_flyte_idl(obj.to_flyte_idl()) == obj

    for val in [
        1.0,
        "abc",
        True,
        False,
        datetime.datetime.now(),
        datetime.timedelta(seconds=1),
    ]:
        with pytest.raises(user_exceptions.FlyteTypeException):
            primitives.Integer.from_python_std(val)

    obj = primitives.Integer.from_python_std(None)
    assert obj.to_python_std() is None
    assert primitives.Integer.from_flyte_idl(obj.to_flyte_idl()) == obj

    # Test string parsing
    with pytest.raises(user_exceptions.FlyteTypeException):
        primitives.Integer.from_string("books")
    obj = primitives.Integer.from_string("299792458")
    assert obj.to_python_std() == 299792458
    assert primitives.Integer.from_flyte_idl(obj.to_flyte_idl()) == obj

    assert obj.short_string() == "Integer(299792458)"
    assert obj.verbose_string() == "Integer(299792458)"


def test_float():
    # Check type specification
    assert primitives.Float.to_flyte_literal_type().simple == literal_types.SimpleType.FLOAT

    # Test value behavior
    obj = primitives.Float.from_python_std(1.0)
    assert obj.to_python_std() == 1.0
    assert primitives.Float.from_flyte_idl(obj.to_flyte_idl()) == obj

    for val in [
        1,
        "abc",
        True,
        False,
        datetime.datetime.now(),
        datetime.timedelta(seconds=1),
    ]:
        with pytest.raises(user_exceptions.FlyteTypeException):
            primitives.Float.from_python_std(val)

    obj = primitives.Float.from_python_std(None)
    assert obj.to_python_std() is None
    assert primitives.Float.from_flyte_idl(obj.to_flyte_idl()) == obj

    # Test string parsing
    with pytest.raises(user_exceptions.FlyteTypeException):
        primitives.Float.from_string("lightning")
    obj = primitives.Float.from_string("2.71828")
    assert obj.to_python_std() == 2.71828
    assert primitives.Float.from_flyte_idl(obj.to_flyte_idl()) == obj

    assert obj.short_string() == "Float(2.71828)"
    assert obj.verbose_string() == "Float(2.71828)"


def test_boolean():
    # Check type specification
    assert primitives.Boolean.to_flyte_literal_type().simple == literal_types.SimpleType.BOOLEAN

    # Test value behavior
    obj = primitives.Boolean.from_python_std(True)
    assert obj.to_python_std() is True
    assert primitives.Boolean.from_flyte_idl(obj.to_flyte_idl()) == obj

    for val in [1, 1.0, "abc", datetime.datetime.now(), datetime.timedelta(seconds=1)]:
        with pytest.raises(user_exceptions.FlyteTypeException):
            primitives.Boolean.from_python_std(val)

    obj = primitives.Boolean.from_python_std(None)
    assert obj.to_python_std() is None
    assert primitives.Boolean.from_flyte_idl(obj.to_flyte_idl()) == obj

    # Test string parsing
    with pytest.raises(user_exceptions.FlyteTypeException):
        primitives.Boolean.from_string("lightning")
    obj = primitives.Boolean.from_string("false")
    assert not obj.to_python_std()
    assert primitives.Boolean.from_flyte_idl(obj.to_flyte_idl()) == obj
    obj = primitives.Boolean.from_string("False")
    assert not obj.to_python_std()
    obj = primitives.Boolean.from_string("0")
    assert not obj.to_python_std()
    obj = primitives.Boolean.from_string("true")
    assert obj.to_python_std()
    obj = primitives.Boolean.from_string("True")
    assert obj.to_python_std()
    obj = primitives.Boolean.from_string("1")
    assert obj.to_python_std()
    assert primitives.Boolean.from_flyte_idl(obj.to_flyte_idl()) == obj

    assert obj.short_string() == "Boolean(True)"
    assert obj.verbose_string() == "Boolean(True)"


def test_string():
    # Check type specification
    assert primitives.String.to_flyte_literal_type().simple == literal_types.SimpleType.STRING

    # Test value behavior
    obj = primitives.String.from_python_std("abc")
    assert obj.to_python_std() == "abc"
    assert primitives.String.from_flyte_idl(obj.to_flyte_idl()) == obj

    for val in [
        1,
        1.0,
        True,
        False,
        datetime.datetime.now(),
        datetime.timedelta(seconds=1),
    ]:
        with pytest.raises(user_exceptions.FlyteTypeException):
            primitives.String.from_python_std(val)

    obj = primitives.String.from_python_std(None)
    assert obj.to_python_std() is None
    assert primitives.String.from_flyte_idl(obj.to_flyte_idl()) == obj

    # Test string parsing
    my_string = "this is a string"
    obj = primitives.String.from_string(my_string)
    assert obj.to_python_std() == my_string
    assert primitives.String.from_flyte_idl(obj.to_flyte_idl()) == obj

    assert obj.short_string() == "String('this is a string')"
    assert obj.verbose_string() == "String('this is a string')"

    with pytest.raises(user_exceptions.FlyteTypeException):
        primitives.String.from_string([])

    with pytest.raises(user_exceptions.FlyteTypeException):
        primitives.String.from_string({})


class UTC(datetime.tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)


def test_datetime():
    # Check type specification
    assert primitives.Datetime.to_flyte_literal_type().simple == literal_types.SimpleType.DATETIME

    # Test value behavior
    dt = datetime.datetime.now(tz=tz.UTC)
    obj = primitives.Datetime.from_python_std(dt)
    assert primitives.Datetime.from_flyte_idl(obj.to_flyte_idl()) == obj
    assert obj.to_python_std() == dt

    # Timezone is required
    with pytest.raises(user_exceptions.FlyteValueException):
        primitives.Datetime.from_python_std(datetime.datetime.now())

    for val in [1, 1.0, "abc", True, False, datetime.timedelta(seconds=1)]:
        with pytest.raises(user_exceptions.FlyteTypeException):
            primitives.Datetime.from_python_std(val)

    obj = primitives.Datetime.from_python_std(None)
    assert obj.to_python_std() is None
    assert primitives.Datetime.from_flyte_idl(obj.to_flyte_idl()) == obj

    # Test string parsing
    with pytest.raises(user_exceptions.FlyteTypeException):
        primitives.Datetime.from_string("not a real date")
    obj = primitives.Datetime.from_string("2018-05-15 4:32pm UTC")
    test_dt = datetime.datetime(2018, 5, 15, 16, 32, 0, 0, UTC())
    assert obj.short_string() == "Datetime(2018-05-15 16:32:00+00:00)"
    assert obj.verbose_string() == "Datetime(2018-05-15 16:32:00+00:00)"
    assert obj.to_python_std() == test_dt
    assert primitives.Datetime.from_flyte_idl(obj.to_flyte_idl()) == obj


def test_timedelta():
    # Check type specification
    assert primitives.Timedelta.to_flyte_literal_type().simple == literal_types.SimpleType.DURATION

    # Test value behavior
    obj = primitives.Timedelta.from_python_std(datetime.timedelta(seconds=1))
    assert obj.to_python_std() == datetime.timedelta(seconds=1)
    assert primitives.Timedelta.from_flyte_idl(obj.to_flyte_idl()) == obj

    for val in [1.0, "abc", True, False, datetime.datetime.now()]:
        with pytest.raises(user_exceptions.FlyteTypeException):
            primitives.Timedelta.from_python_std(val)

    obj = primitives.Timedelta.from_python_std(None)
    assert obj.to_python_std() is None
    assert primitives.Timedelta.from_flyte_idl(obj.to_flyte_idl()) == obj

    # Test string parsing
    with pytest.raises(user_exceptions.FlyteTypeException):
        primitives.Timedelta.from_string("not a real duration")
    obj = primitives.Timedelta.from_string("15 hours, 1.1 second")
    test_d = datetime.timedelta(hours=15, seconds=1, milliseconds=100)
    assert obj.short_string() == "Timedelta(15:00:01.100000)"
    assert obj.verbose_string() == "Timedelta(15:00:01.100000)"
    assert obj.to_python_std() == test_d
    assert primitives.Timedelta.from_flyte_idl(obj.to_flyte_idl()) == obj


def test_void():
    # Check type specification
    with pytest.raises(user_exceptions.FlyteAssertion):
        base_sdk_types.Void.to_flyte_literal_type()

    # Test value behavior
    for val in [
        1,
        1.0,
        "abc",
        True,
        False,
        datetime.datetime.now(),
        datetime.timedelta(seconds=1),
        None,
    ]:
        assert base_sdk_types.Void.from_python_std(val).to_python_std() is None

    obj = base_sdk_types.Void()
    assert base_sdk_types.Void.from_flyte_idl(obj.to_flyte_idl()) == obj

    assert obj.short_string() == "Void()"
    assert obj.verbose_string() == "Void()"


def test_generic():
    # Check type specification
    assert primitives.Generic.to_flyte_literal_type().simple == literal_types.SimpleType.STRUCT

    # Test value behavior
    d = {"a": [1, 2, 3], "b": "abc", "c": 1, "d": {"a": 1}}
    obj = primitives.Generic.from_python_std(d)
    assert obj.to_python_std() == d
    assert primitives.Generic.from_flyte_idl(obj.to_flyte_idl()) == obj

    for val in [
        1.0,
        "abc",
        True,
        False,
        datetime.datetime.now(),
        datetime.timedelta(seconds=1),
    ]:
        with pytest.raises(user_exceptions.FlyteTypeException):
            primitives.Generic.from_python_std(val)

    obj = primitives.Generic.from_python_std(None)
    assert obj.to_python_std() is None
    assert primitives.Generic.from_flyte_idl(obj.to_flyte_idl()) == obj

    # Test string parsing
    with pytest.raises(user_exceptions.FlyteValueException):
        primitives.Generic.from_string("1")
    obj = primitives.Generic.from_string('{"a": 1.0}')
    assert obj.to_python_std() == {"a": 1.0}
    assert primitives.Generic.from_flyte_idl(obj.to_flyte_idl()) == obj
