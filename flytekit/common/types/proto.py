from __future__ import absolute_import

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types as _base_sdk_types
from flytekit.models import types as _idl_types, literals as _literals
from google.protobuf import reflection as _proto_reflection

import base64 as _base64
import six as _six


def create_protobuf(pb_type):
    """
    :param T pb_type:
    :rtype: ProtobufType
    """
    if not isinstance(pb_type, _proto_reflection.GeneratedProtocolMessageType):
        raise _user_exceptions.FlyteTypeException(
            expected_type=_proto_reflection.GeneratedProtocolMessageType,
            received_type=type(pb_type),
            received_value=pb_type
        )

    class _Protobuf(Protobuf):
        _pb_type = pb_type
    return _Protobuf


class ProtobufType(_base_sdk_types.FlyteSdkType):

    @property
    def pb_type(cls):
        """
        :rtype: GeneratedProtocolMessageType
        """
        return cls._pb_type

    @property
    def descriptor(cls):
        """
        :rtype: Text
        """
        return "{}.{}".format(cls.pb_type.__module__, cls.pb_type.__name__)

    @property
    def tag(cls):
        """
        :rtype: Text
        """
        return "{}{}".format(Protobuf.TAG_PREFIX, cls.descriptor)


class Protobuf(_six.with_metaclass(ProtobufType, _base_sdk_types.FlyteSdkValue)):

    PB_FIELD_KEY = "pb_type"
    TAG_PREFIX = "{}=".format(PB_FIELD_KEY)

    def __init__(self, pb_object):
        """
        :param T pb_object:
        """
        data = pb_object.SerializeToString()
        super(Protobuf, self).__init__(
            scalar=_literals.Scalar(
                binary=_literals.Binary(
                    value=bytes(data) if _six.PY2 else data,
                    tag=type(self).tag
                )
            )
        )

    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value: b64 encoded string of bytes
        :rtype: Protobuf
        """
        try:
            decoded = _base64.b64decode(string_value)
        except TypeError:
            raise _user_exceptions.FlyteValueException(string_value, "The string is not valid base64-encoded.")
        pb_obj = cls.pb_type()
        pb_obj.ParseFromString(decoded)
        return cls(pb_obj)

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return isinstance(other, ProtobufType) and other.pb_type is cls.pb_type

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: _base_sdk_types.FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        elif isinstance(t_value, cls.pb_type):
            return cls(t_value)
        else:
            raise _user_exceptions.FlyteTypeException(
                type(t_value),
                cls.pb_type,
                received_value=t_value
            )

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(
            simple=_idl_types.SimpleType.BINARY,
            metadata={
                cls.PB_FIELD_KEY: cls.descriptor
            }
        )

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: Protobuf
        """
        if literal_model.scalar.binary.tag != cls.tag:
            raise _user_exceptions.FlyteTypeException(
                literal_model.scalar.binary.tag,
                cls.pb_type,
                received_value=_base64.b64encode(literal_model.scalar.binary.value),
                additional_msg="Can not deserialize as proto tags don't match."
            )
        pb_obj = cls.pb_type()
        pb_obj.ParseFromString(literal_model.scalar.binary.value)
        return cls(pb_obj)

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "Types.Proto({})".format(cls.descriptor)

    def to_python_std(self):
        """
        :returns: The protobuf object as defined by the user.
        :rtype: T
        """
        pb_obj = type(self).pb_type()
        pb_obj.ParseFromString(self.scalar.binary.value)
        return pb_obj

    def short_string(self):
        """
        :rtype: Text
        """
        return "{}".format(self.to_python_std())
