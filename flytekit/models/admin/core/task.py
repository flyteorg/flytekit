from flyteidl.core import tasks_pb2 as _core_task

from flytekit.models import common as _common


class RuntimeMetadata(_common.FlyteIdlEntity):
    class RuntimeType(object):
        OTHER = 0
        FLYTE_SDK = 1

    def __init__(self, type, version, flavor):
        """
        :param int type: Enum type from RuntimeMetadata.RuntimeType
        :param Text version: Version string for SDK version.  Can be used for metrics or managing breaking changes in
            Admin or Propeller
        :param Text flavor: Optional extra information about runtime environment (e.g. Python, GoLang, etc.)
        """
        self._type = type
        self._version = version
        self._flavor = flavor

    @property
    def type(self):
        """
        Enum type from RuntimeMetadata.RuntimeType
        :rtype: int
        """
        return self._type

    @property
    def version(self):
        """
        Version string for SDK version.  Can be used for metrics or managing breaking changes in Admin or Propeller
        :rtype: Text
        """
        return self._version

    @property
    def flavor(self):
        """
        Optional extra information about runtime environment (e.g. Python, GoLang, etc.)
        :rtype: Text
        """
        return self._flavor

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.tasks_pb2.RuntimeMetadata
        """
        return _core_task.RuntimeMetadata(type=self.type, version=self.version, flavor=self.flavor)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.tasks_pb2.RuntimeMetadata pb2_object:
        :rtype: RuntimeMetadata
        """
        return cls(type=pb2_object.type, version=pb2_object.version, flavor=pb2_object.flavor)