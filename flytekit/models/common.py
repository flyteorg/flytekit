import abc as _abc
import json as _json

import six as _six
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct


class FlyteABCMeta(_abc.ABCMeta):
    def __instancecheck__(cls, instance):
        if cls in type(instance).__mro__:
            return True
        return super(FlyteABCMeta, cls).__instancecheck__(instance)


class FlyteType(FlyteABCMeta):
    def __repr__(cls):
        return cls.short_class_string()

    def __str__(cls):
        return cls.verbose_class_string()

    def short_class_string(cls):
        """
        :rtype: Text
        """
        return super(FlyteType, cls).__repr__()

    def verbose_class_string(cls):
        """
        :rtype: Text
        """
        return cls.short_class_string()

    @_abc.abstractmethod
    def from_flyte_idl(cls, idl_object):
        pass


class FlyteIdlEntity(object, metaclass=FlyteType):
    def __eq__(self, other):
        return isinstance(other, FlyteIdlEntity) and other.to_flyte_idl() == self.to_flyte_idl()

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return self.short_string()

    def __str__(self):
        return self.verbose_string()

    def __hash__(self):
        return hash(self.to_flyte_idl().SerializeToString(deterministic=True))

    def short_string(self):
        """
        :rtype: Text
        """
        return _six.text_type(self.to_flyte_idl())

    def verbose_string(self):
        """
        :rtype: Text
        """
        return self.short_string()

    @property
    def is_empty(self):
        return len(self.to_flyte_idl().SerializeToString()) == 0

    @_abc.abstractmethod
    def to_flyte_idl(self):
        pass


class FlyteCustomIdlEntity(FlyteIdlEntity):
    @classmethod
    def from_flyte_idl(cls, idl_object):
        """

        :param _struct.Struct idl_object:
        :return: FlyteCustomIdlEntity
        """
        return cls.from_dict(idl_dict=_json_format.MessageToDict(idl_object))

    def to_flyte_idl(self):
        return _json_format.Parse(_json.dumps(self.to_dict()), _struct.Struct())

    @_abc.abstractmethod
    def from_dict(self, idl_dict):
        pass

    @_abc.abstractmethod
    def to_dict(self):
        """
        Converts self to a dictionary.
        :rtype: dict[Text, T]
        """
        pass
