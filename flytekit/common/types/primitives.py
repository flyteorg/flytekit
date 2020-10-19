import datetime as _datetime
import json as _json

import six as _six
from dateutil import parser as _parser
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct
from pytimeparse import parse as _parse_duration_string

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types as _base_sdk_types
from flytekit.models import literals as _literals
from flytekit.models import types as _idl_types


class Integer(_base_sdk_types.FlyteSdkValue):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: Integer
        """
        try:
            return cls(int(string_value))
        except (ValueError, TypeError):
            raise _user_exceptions.FlyteTypeException(
                _six.text_type,
                int,
                additional_msg="String not castable to Integer SDK type:" " {}".format(string_value),
            )

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        if type(t_value) not in _six.integer_types:
            raise _user_exceptions.FlyteTypeException(type(t_value), _six.integer_types, t_value)
        return cls(t_value)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(simple=_idl_types.SimpleType.INTEGER)

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: Integer
        """
        return cls(literal_model.scalar.primitive.integer)

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "Integer"

    def __init__(self, value):
        """
        :param int value: Int value to wrap
        """
        super(Integer, self).__init__(scalar=_literals.Scalar(primitive=_literals.Primitive(integer=value)))

    def to_python_std(self):
        """
        :rtype: int
        """
        return self.scalar.primitive.integer

    def short_string(self):
        """
        :rtype: Text
        """
        return "Integer({})".format(self.scalar.primitive.integer)


class Float(_base_sdk_types.FlyteSdkValue):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: Float
        """
        try:
            return cls(float(string_value))
        except ValueError:
            raise _user_exceptions.FlyteTypeException(
                _six.text_type,
                float,
                additional_msg="String not castable to Float SDK type:" " {}".format(string_value),
            )

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        if type(t_value) != float:
            raise _user_exceptions.FlyteTypeException(type(t_value), float, t_value)
        return cls(t_value)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(simple=_idl_types.SimpleType.FLOAT)

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: Float
        """
        return cls(literal_model.scalar.primitive.float_value)

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "Float"

    def __init__(self, value):
        """
        :param float value: value to wrap
        """
        super(Float, self).__init__(scalar=_literals.Scalar(primitive=_literals.Primitive(float_value=value)))

    def to_python_std(self):
        """
        :rtype: float
        """
        return self.scalar.primitive.float_value

    def short_string(self):
        """
        :rtype: Text
        """
        return "Float({})".format(self.scalar.primitive.float_value)


class Boolean(_base_sdk_types.FlyteSdkValue):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: Boolean
        """
        if string_value == "1" or string_value.lower() == "true":
            return cls(True)
        elif string_value == "0" or string_value.lower() == "false":
            return cls(False)
        raise _user_exceptions.FlyteTypeException(
            _six.text_type, bool, additional_msg="String not castable to Boolean SDK " "type: {}".format(string_value),
        )

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        if type(t_value) != bool:
            raise _user_exceptions.FlyteTypeException(type(t_value), bool, t_value)
        return cls(t_value)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(simple=_idl_types.SimpleType.BOOLEAN)

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: bool
        """
        return cls(literal_model.scalar.primitive.boolean)

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "Boolean"

    def __init__(self, value):
        """
        :param bool value: value to wrap
        """
        super(Boolean, self).__init__(scalar=_literals.Scalar(primitive=_literals.Primitive(boolean=value)))

    def to_python_std(self):
        """
        :rtype: bool
        """
        return self.scalar.primitive.boolean

    def short_string(self):
        """
        :rtype: Text
        """
        return "Boolean({})".format(self.scalar.primitive.boolean)


class String(_base_sdk_types.FlyteSdkValue):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: String
        """
        if type(string_value) == dict or type(string_value) == list:
            raise _user_exceptions.FlyteTypeException(
                type(string_value),
                _six.text_type,
                additional_msg="Should not cast native Python type to string {}".format(string_value),
            )
        return cls(string_value)

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        Creates an object of this type from the model primitive defining it.
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        if type(t_value) not in set([str, _six.text_type]):
            raise _user_exceptions.FlyteTypeException(type(t_value), set([str, _six.text_type]), t_value)
        return cls(t_value)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(simple=_idl_types.SimpleType.STRING)

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: String
        """
        return cls(literal_model.scalar.primitive.string_value)

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "String"

    def __init__(self, value):
        """
        :param Text value: value to wrap
        """
        super(String, self).__init__(scalar=_literals.Scalar(primitive=_literals.Primitive(string_value=value)))

    def to_python_std(self):
        """
        :rtype: Text
        """
        return self.scalar.primitive.string_value

    def short_string(self):
        """
        :rtype: Text
        """
        _TRUNCATE_LENGTH = 100
        return "String('{}'{})".format(
            self.scalar.primitive.string_value[:_TRUNCATE_LENGTH],
            " ..." if len(self.scalar.primitive.string_value) > _TRUNCATE_LENGTH else "",
        )

    def verbose_string(self):
        """
        :rtype: Text
        """
        return "String('{}')".format(self.scalar.primitive.string_value)


class Datetime(_base_sdk_types.FlyteSdkValue):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: Datetime
        """
        try:
            python_std_datetime = _parser.parse(string_value)
        except ValueError:
            raise _user_exceptions.FlyteTypeException(
                _six.text_type,
                _datetime.datetime,
                additional_msg="String not castable to Datetime " "SDK type: {}".format(string_value),
            )

        return cls.from_python_std(python_std_datetime)

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        elif type(t_value) != _datetime.datetime:
            raise _user_exceptions.FlyteTypeException(type(t_value), _datetime.datetime, t_value)
        elif t_value.tzinfo is None:
            raise _user_exceptions.FlyteValueException(
                t_value, "Datetime objects in Flyte must be timezone aware.  " "tzinfo was found to be None.",
            )
        return cls(t_value)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(simple=_idl_types.SimpleType.DATETIME)

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: Datetime
        """
        return cls(literal_model.scalar.primitive.datetime)

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "Datetime"

    def __init__(self, value):
        """
        :param datetime.datetime value: value to wrap
        """
        super(Datetime, self).__init__(scalar=_literals.Scalar(primitive=_literals.Primitive(datetime=value)))

    def to_python_std(self):
        """
        :rtype: datetime.datetime
        """
        return self.scalar.primitive.datetime

    def short_string(self):
        """
        :rtype: Text
        """
        return "Datetime({})".format(_six.text_type(self.scalar.primitive.datetime))


class Timedelta(_base_sdk_types.FlyteSdkValue):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value: Uses https://github.com/wroberts/pytimeparse for parsing
        :rtype: Timedelta
        """
        td = _parse_duration_string(string_value)
        if td is None:
            raise _user_exceptions.FlyteTypeException(
                _six.text_type,
                _datetime.timedelta,
                additional_msg="Could not convert string to" " time delta: {}".format(string_value),
            )
        return cls.from_python_std(_datetime.timedelta(seconds=td))

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        elif type(t_value) != _datetime.timedelta:
            raise _user_exceptions.FlyteTypeException(type(t_value), _datetime.timedelta, t_value)

        return cls(t_value)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(simple=_idl_types.SimpleType.DURATION)

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: Timedelta
        """
        return cls(literal_model.scalar.primitive.duration)

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "Timedelta"

    def __init__(self, value):
        """
        :param datetime.timedelta value: value to wrap
        """
        super(Timedelta, self).__init__(scalar=_literals.Scalar(primitive=_literals.Primitive(duration=value)))

    def to_python_std(self):
        """
        :rtype: datetime.timedelta
        """
        return self.scalar.primitive.duration

    def short_string(self):
        """
        :rtype: Text
        """
        return "Timedelta({})".format(self.scalar.primitive.duration)


class Generic(_base_sdk_types.FlyteSdkValue):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value: Should be a JSON formatted string
        :rtype: Generic
        """
        try:
            t = _json_format.Parse(string_value, _struct.Struct())
        except Exception:
            raise _user_exceptions.FlyteValueException(string_value, "Could not be parsed from JSON.")
        return cls(t)

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        elif not isinstance(t_value, dict):
            raise _user_exceptions.FlyteTypeException(type(t_value), dict, t_value)

        try:
            t = _json.dumps(t_value)
        except Exception:
            raise _user_exceptions.FlyteValueException(t_value, "Is not JSON serializable.")

        return cls(_json_format.Parse(t, _struct.Struct()))

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(simple=_idl_types.SimpleType.STRUCT)

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: Generic
        """
        return cls(literal_model.scalar.generic)

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "Generic"

    def __init__(self, value):
        """
        :param _struct.Struct value: value to wrap
        """
        super(Generic, self).__init__(scalar=_literals.Scalar(generic=value))

    def to_python_std(self):
        """
        :rtype: dict[Text, T]
        """
        return _json.loads(_json_format.MessageToJson(self.scalar.generic))

    def short_string(self):
        """
        :rtype: Text
        """
        return "Generic({})".format(self.to_python_std())

    def long_string(self):
        """
        :rtype: Text
        """
        return "Generic({})".format(self.to_python_std())
