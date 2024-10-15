import flyteidl_rust as flyteidl

from flytekit.models import common as _common


class Project(_common.FlyteIdlEntity):
    class ProjectState(object):
        ACTIVE = flyteidl.project.ProjectState.Active
        ARCHIVED = flyteidl.project.ProjectState.Archived
        SYSTEM_GENERATED = flyteidl.project.ProjectState.SystemGenerated

    def __init__(self, id, name, description, state=ProjectState.ACTIVE):
        """
        A project represents a logical grouping used to organize entities (tasks, workflows, executions) in the Flyte
        platform.

        :param Text id: A globally unique identifier associated with this project.
        :param Text name: A human-readable name for this project.
        :param Text name: A concise description for this project.
        """
        self._id = id
        self._name = name
        self._description = description
        self._state = state

    @classmethod
    def archived_project(cls, id):
        return cls(id, "", "", cls.ProjectState.ARCHIVED)

    @classmethod
    def active_project(cls, id):
        return cls(id, "", "", cls.ProjectState.ACTIVE)

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

    @property
    def description(self):
        """
        A concise description for this project.
        :rtype: Text
        """
        return self._description

    @property
    def state(self):
        """
        The state of this project.
        :rtype: int
        """
        return self._state

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.project_pb2.Project
        """
        return flyteidl.admin.Project(id=self.id, name=self.name, description=self.description, state=int(self._state))

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.project_pb2.Project pb2_object:
        :rtype: Project
        """
        return cls(id=pb2_object.id, name=pb2_object.name, description=pb2_object.description, state=pb2_object.state)
