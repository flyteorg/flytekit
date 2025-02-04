from flyteidl.admin import project_pb2 as _project_pb2

from flytekit.models import common as _common


class Domain(_common.FlyteIdlEntity):
    """
    Domains are fixed and unique at the global level, and provide an abstraction to isolate resources and feature configuration for different deployment environments.

    :param Text id: A globally unique identifier associated with this domain.
    :param Text name: A human-readable name for this domain.
    """

    def __init__(self, id, name):
        self._id = id
        self._name = name

    @property
    def id(self):
        """
        A globally unique identifier associated with this domain.
        :rtype: Text
        """
        return self._id

    @property
    def name(self):
        """
        A human-readable name for this domain.
        :rtype: Text
        """
        return self._name

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.project_pb2.Domain
        """
        return _project_pb2.Domain(id=self.id, name=self.name)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.project_pb2.Domain pb2_object:
        :rtype: Domain
        """
        return cls(id=pb2_object.id, name=pb2_object.name)
