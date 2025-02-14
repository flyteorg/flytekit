from flyteidl.core import metrics_pb2 as _metrics_pb2

from flytekit.models import common as _common


class Span(_common.FlyteIdlEntity):
    """
    Span represents a duration trace of Flyte execution. The id field denotes a Flyte execution entity or an operation
    which uniquely identifies the Span. The spans attribute allows this Span to be further broken down into more
    precise definitions.

    :param google.protobuf.Timestamp start_time: start_time defines the instance this span began.
    :param google.protobuf.Timestamp end_time: end_time defines the instance this span completed.
    :param flyteidl.core.WorkflowExecutionIdentifier workflow_id: workflow_id defines the instance this span completed.
    :param flyteidl.core.NodeExecutionIdentifier node_id: node_id is the id of the node execution this Span represents.
    :param flyteidl.core.TaskExecutionIdentifier task_id: task_id is the id of the task execution this Span represents.
    :param Text operations_id: operation_id is the id of a unique operation that this Span represents.
    :param Spans spans: spans defines a collection of Spans that breakdown this execution.

    """

    def __init__(
        self,
        start_time,
        end_time,
        workflow_id=None,
        node_id=None,
        task_id=None,
        operation_id=None,
        spans=None,
    ):
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
        start_time defines the instance this span began.
        :rtype: google.protobuf.Timestamp
        """
        return self._start_time

    @property
    def end_time(self):
        """
        end_time defines the instance this span completed.
        :rtype: google.protobuf.Timestamp
        """
        return self._end_time

    @property
    def workflow_id(self):
        """
        workflow_id defines the instance this span completed.
        :rtype: flyteidl.core.WorkflowExecutionIdentifier
        """
        return self._workflow_id

    @property
    def node_id(self):
        """
        node_id is the id of the node execution this Span represents.
        :rtype: flyteidl.core.NodeExecutionIdentifier
        """
        return self._node_id

    @property
    def task_id(self):
        """
        task_id is the id of the task execution this Span represents.
        :rtype: flyteidl.core.TaskExecutionIdentifier
        """
        return self._task_id

    @property
    def operation_id(self):
        """
        operation_id is the id of a unique operation that this Span represents.
        :rtype: Text
        """
        return self._operation_id

    @property
    def spans(self):
        """
        spans defines a collection of Spans that breakdown this execution.
        :rtype: spans
        """
        return self._spans

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.metrics_pb2.Span
        """
        return _metrics_pb2.Span(
            start_time=self._start_time,
            end_time=self._end_time,
            workflow_id=self._workflow_id,
            node_id=self._node_id,
            task_id=self._task_id,
            operation_id=self._operation_id,
            spans=self._spans,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.metrics_pb2.Span pb2_object:
        :rtype: Span
        """
        return cls(
            start_time=pb2_object.start_time,
            end_time=pb2_object.end_time,
            workflow_id=pb2_object.workflow_id,
            node_id=pb2_object.node_id,
            task_id=pb2_object.task_id,
            operation_id=pb2_object.operation_id,
            spans=pb2_object.spans,
        )
