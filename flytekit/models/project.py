from __future__ import absolute_import

from flyteidl.admin import project_pb2 as _project_pb2

from flytekit.models import common as _common


class Project(_common.FlyteIdlEntity):

    def __init__(self, id, name):
        """
        A project represents a logical grouping used to organize entities (tasks, workflows, executions) in the Flyte
        platform.

        :param Text id: A globally unique identifier associated with this project.
        :param Text name: A human-readable name for this project.
        """
        self._id = id
        self._name = name

    @property
    def id(self):
        """
        A globally unique identifier associated with this project
        :rtype: Text
        """
        return self._id

    @property
    def name(self):
        """
        A human-readable name for this project.
        :rtype: Text
        """
        return self._name

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.project_pb2.Project
        """
        return _project_pb2.Project(
            id=self.id,
            name=self.name
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.project_pb2.Project pb2_object:
        :rtype: Project
        """
        return cls(
            id=pb2_object.id,
            name=pb2_object.name,
        )
