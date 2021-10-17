import abc as _abc
import json as _json

import six as _six
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flyteidl.admin import common_pb2 as _common_pb2


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


class AuthRole(FlyteIdlEntity):
    def __init__(self, assumable_iam_role=None, kubernetes_service_account=None):
        """
        At most one of assumable_iam_role or kubernetes_service_account can be set.
        :param Text assumable_iam_role: IAM identity with set permissions policies.
        :param Text kubernetes_service_account: Provides an identity for workflow execution resources. Flyte deployment
            administrators are responsible for handling permissions as they relate to the service account.
        """
        self._assumable_iam_role = assumable_iam_role
        self._kubernetes_service_account = kubernetes_service_account

    @property
    def assumable_iam_role(self):
        """
        The IAM role to execute the workflow with
        :rtype: Text
        """
        return self._assumable_iam_role

    @property
    def kubernetes_service_account(self):
        """
        The kubernetes service account to execute the workflow with
        :rtype: Text
        """
        return self._kubernetes_service_account

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.launch_plan_pb2.Auth
        """
        return _common_pb2.AuthRole(
            assumable_iam_role=self.assumable_iam_role if self.assumable_iam_role else None,
            kubernetes_service_account=self.kubernetes_service_account if self.kubernetes_service_account else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.launch_plan_pb2.Auth pb2_object:
        :rtype: Auth
        """
        return cls(
            assumable_iam_role=pb2_object.assumable_iam_role,
            kubernetes_service_account=pb2_object.kubernetes_service_account,
        )