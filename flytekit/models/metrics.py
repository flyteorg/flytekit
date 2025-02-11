from flyteidl.core import metrics_pb2 as _metrics_pb2

from flytekit.models import common as _common


class Span(_common.FlyteIdlEntity):
    def __init__(self, start_time, end_time, workflow_id, node_id, task_id, operation_id, spans):
        """
        A project represents a logical grouping used to organize entities (tasks, workflows, executions) in the Flyte
        platform.

        :param Text id: A globally unique identifier associated with this project.
        :param Text name: A human-readable name for this project.
        :param Text name: A concise description for this project.
        """
        self._start_time = start_time
        self._end_time = end_time
        self._workflow_id = workflow_id
        self._node_id = node_id
        self._task_id = task_id
        self._operation_id = operation_id
        self._spans = spans

    @property
    def start_time(self):
        """
        A globally unique identifier associated with this project
        :rtype: Text
        """
        return self._start_time
    
    @property
    def end_time(self):
        """
        A globally unique identifier associated with this project
        :rtype: Text
        """
        return self._end_time
    
    @property
    def workflow_id(self):
        """
        A globally unique identifier associated with this project
        :rtype: Text
        """
        return self._workflow_id
    
    @property
    def node_id(self):
        """
        A globally unique identifier associated with this project
        :rtype: Text
        """
        return self._node_id
    
    @property
    def task_id(self):
        """
        A globally unique identifier associated with this project
        :rtype: Text
        """
        return self._task_id
    

    @property
    def operation_id(self):
        """
        A globally unique identifier associated with this project
        :rtype: Text
        """
        return self._operation_id
    
    @property
    def spans(self):
        """
        A globally unique identifier associated with this project
        :rtype: Text
        """
        return self._spans

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.project_pb2.Project
        """
        return _metrics_pb2.Span(start_time=self._start_time, end_time=self._end_time, workflow_id=self._workflow_id,node_id=self._node_id, task_id=self._task_id, operation_id=self._operation_id, spans=self._spans)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.project_pb2.Project pb2_object:
        :rtype: Project
        """
        return cls(start_time=pb2_object.start_time, end_time=pb2_object.end_time, workflow_id=pb2_object.workflow_id,node_id=pb2_object.node_id, task_id=pb2_object.task_id, operation_id=pb2_object.operation_id, spans=pb2_object.spans)