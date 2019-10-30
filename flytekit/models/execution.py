from __future__ import absolute_import

import flyteidl.admin.execution_pb2 as _execution_pb2
import flyteidl.admin.node_execution_pb2 as _node_execution_pb2
import flyteidl.admin.task_execution_pb2 as _task_execution_pb2

from flytekit.models import common as _common_models
from flytekit.models.core import execution as _core_execution, identifier as _identifier
import pytz as _pytz

class ExecutionMetadata(_common_models.FlyteIdlEntity):

    class ExecutionMode(object):
        MANUAL = 0
        SCHEDULED = 1
        SYSTEM = 2

    def __init__(self, mode, principal, nesting):
        """
        :param int mode: An enum value from ExecutionMetadata.ExecutionMode which specifies how the job started.
        :param Text principal: The entity that triggered the execution
        :param int nesting: An integer representing how deeply nested the workflow is (i.e. was it triggered by a parent
            workflow)
        """
        self._mode = mode
        self._principal = principal
        self._nesting = nesting

    @property
    def mode(self):
        """
        An enum value from ExecutionMetadata.ExecutionMode which specifies how the job started.
        :rtype: int
        """
        return self._mode

    @property
    def principal(self):
        """
        The entity that triggered the execution
        :rtype: Text
        """
        return self._principal

    @property
    def nesting(self):
        """
        An integer representing how deeply nested the workflow is (i.e. was it triggered by a parent workflow)
        :rtype: int
        """
        return self._nesting

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.execution_pb2.ExecutionMetadata
        """
        return _execution_pb2.ExecutionMetadata(
            mode=self.mode,
            principal=self.principal,
            nesting=self.nesting
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.execution_pb2.ExecutionMetadata pb2_object:
        :return: ExecutionMetadata
        """
        return cls(
            mode=pb2_object.mode,
            principal=pb2_object.principal,
            nesting=pb2_object.nesting
        )


class ExecutionSpec(_common_models.FlyteIdlEntity):

    def __init__(self, launch_plan, metadata, notifications=None, disable_all=None, labels=None,
                 annotations=None):
        """
        :param flytekit.models.core.identifier.Identifier launch_plan: Launch plan unique identifier to execute
        :param ExecutionMetadata metadata: The metadata to be associated with this execution
        :param NotificationList notifications: List of notifications for this execution.
        :param bool disable_all: If true, all notifications should be disabled.
        :param flytekit.models.common.Labels labels: Labels to apply to the execution.
        :param flytekit.models.common.Annotations annotations: Annotations to apply to the execution

        """
        self._launch_plan = launch_plan
        self._metadata = metadata
        self._notifications = notifications
        self._disable_all = disable_all
        self._labels = labels or _common_models.Labels({})
        self._annotations = annotations or _common_models.Annotations({})

    @property
    def launch_plan(self):
        """
        If the values were too large, this is the URI where the values were offloaded.
        :rtype: flytekit.models.core.identifier.Identifier
        """
        return self._launch_plan

    @property
    def metadata(self):
        """
        :rtype: ExecutionMetadata
        """
        return self._metadata

    @property
    def notifications(self):
        """
        :rtype: Optional[NotificationList]
        """
        return self._notifications

    @property
    def disable_all(self):
        """
        :rtype: Optional[bool]
        """
        return self._disable_all

    @property
    def labels(self):
        """
        :rtype: flytekit.models.common.Labels
        """
        return self._labels

    @property
    def annotations(self):
        """
        :rtype: flytekit.models.common.Annotations
        """
        return self._annotations

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.execution_pb2.ExecutionSpec
        """
        return _execution_pb2.ExecutionSpec(
            launch_plan=self.launch_plan.to_flyte_idl(),
            metadata=self.metadata.to_flyte_idl(),
            notifications=self.notifications.to_flyte_idl() if self.notifications else None,
            disable_all=self.disable_all,
            labels=self.labels.to_flyte_idl(),
            annotations=self.annotations.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, p):
        """
        :param flyteidl.admin.execution_pb2.ExecutionSpec p:
        :return: ExecutionSpec
        """
        return cls(
            launch_plan=_identifier.Identifier.from_flyte_idl(p.launch_plan),
            metadata=ExecutionMetadata.from_flyte_idl(p.metadata),
            notifications=NotificationList.from_flyte_idl(p.notifications) if p.HasField("notifications") else None,
            disable_all=p.disable_all if p.HasField("disable_all") else None,
            labels=_common_models.Labels.from_flyte_idl(p.labels),
            annotations=_common_models.Annotations.from_flyte_idl(p.annotations),
        )


class LiteralMapBlob(_common_models.FlyteIdlEntity):

    def __init__(self, values=None, uri=None):
        """
        :param flytekit.models.literals.LiteralMap values:
        :param Text uri:
        """
        self._values = values
        self._uri = uri

    @property
    def values(self):
        """
        :rtype: flytekit.models.literals.LiteralMap
        """
        return self._values

    @property
    def uri(self):
        """
        :rtype: Text
        """
        return self._uri

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.execution_pb2.LiteralMapBlob
        """
        return _execution_pb2.LiteralMapBlob(
            values=self.values.to_flyte_idl() if self.values is not None else None,
            uri=self.uri
        )

    @classmethod
    def from_flyte_idl(cls, pb):
        """
        :param flyteidl.admin.execution_pb2.LiteralMapBlob pb:
        :rtype: LiteralMapBlob
        """
        values = None
        if pb.HasField("values"):
            values = LiteralMapBlob.from_flyte_idl(pb.values)
        return cls(
            values=values,
            uri=pb.uri if pb.HasField("uri") else None
        )


class Execution(_common_models.FlyteIdlEntity):

    def __init__(self, id, spec, closure):
        """
        :param flytekit.models.core.identifier.WorkflowExecutionIdentifier id:
        :param Text id:
        :param ExecutionSpec spec:
        :param ExecutionClosure closure:
        """
        self._id = id
        self._spec = spec
        self._closure = closure

    @property
    def id(self):
        """
        :rtype: flytekit.models.core.identifier.WorkflowExecutionIdentifier
        """
        return self._id

    @property
    def closure(self):
        """
        :rtype: ExecutionClosure
        """
        return self._closure

    @property
    def spec(self):
        """
        :rtype: ExecutionSpec
        """
        return self._spec

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.execution_pb2.Execution
        """
        return _execution_pb2.Execution(
            id=self.id.to_flyte_idl(),
            closure=self.closure.to_flyte_idl(),
            spec=self.spec.to_flyte_idl()
        )

    @classmethod
    def from_flyte_idl(cls, pb):
        """
        :param flyteidl.admin.execution_pb2.Execution pb:
        :rtype: Execution
        """
        return cls(
            id=_identifier.WorkflowExecutionIdentifier.from_flyte_idl(pb.id),
            closure=ExecutionClosure.from_flyte_idl(pb.closure),
            spec=ExecutionSpec.from_flyte_idl(pb.spec)
        )


class ExecutionClosure(_common_models.FlyteIdlEntity):

    def __init__(self, phase, started_at, error=None, outputs=None):
        """
        :param int phase: From the flytekit.models.core.execution.WorkflowExecutionPhase enum
        :param datetime.datetime started_at:
        :param flytekit.models.core.execution.ExecutionError error:
        :param LiteralMapBlob outputs:
        """
        self._phase = phase
        self._started_at = started_at
        self._error = error
        self._outputs = outputs

    @property
    def error(self):
        """
        :rtype: flytekit.models.core.execution.ExecutionError
        """
        return self._error

    @property
    def phase(self):
        """
        From the flytekit.models.core.execution.WorkflowExecutionPhase enum
        :rtype: int
        """
        return self._phase

    @property
    def started_at(self):
        """
        :rtype: datetime.datetime
        """
        return self._started_at

    @property
    def outputs(self):
        """
        :rtype: LiteralMapBlob
        """
        return self._outputs

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.execution_pb2.ExecutionClosure
        """
        obj = _execution_pb2.ExecutionClosure(
            phase=self.phase,
            error=self.error.to_flyte_idl() if self.error is not None else None,
            outputs=self.outputs.to_flyte_idl() if self.outputs is not None else None
        )
        obj.started_at.FromDatetime(self.started_at.astimezone(_pytz.UTC).replace(tzinfo=None))
        return obj

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.execution_pb2.ExecutionClosure pb2_object:
        :rtype: ExecutionClosure
        """
        error = None
        if pb2_object.HasField("error"):
            error = _core_execution.ExecutionError.from_flyte_idl(pb2_object.error)
        outputs = None
        if pb2_object.HasField("outputs"):
            outputs = LiteralMapBlob.from_flyte_idl(pb2_object.outputs)
        return cls(
            error=error,
            outputs=outputs,
            phase=pb2_object.phase,
            started_at=pb2_object.started_at.ToDatetime().replace(tzinfo=_pytz.UTC),
        )


class NotificationList(_common_models.FlyteIdlEntity):
    def __init__(self, notifications):
        """
        :param list[flytekit.models.common.Notification] notifications: A simple list of notifications.
        """
        self._notifications = notifications

    @property
    def notifications(self):
        """
        :rtype: list[flytekit.models.common.Notification]
        """
        return self._notifications

    def to_flyte_idl(self):
        """
        :rtype:  flyteidl.admin.execution_pb2.NotificationList
        """
        return _execution_pb2.NotificationList(
            notifications=[n.to_flyte_idl() for n in self.notifications]
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.execution_pb2.NotificationList pb2_object:
        :rtype: NotificationList
        """
        return cls(
            [_common_models.Notification.from_flyte_idl(p) for p in pb2_object.notifications]
        )


class _CommonDataResponse(_common_models.FlyteIdlEntity):
    """
    Currently, node, task, and workflow execution all have the same get data response. So we'll create this common
    superclass to reduce code duplication until things diverge in the future.
    """

    def __init__(self, inputs, outputs):
        """
        :param _common_models.UrlBlob inputs:
        :param _common_models.UrlBlob outputs:
        """
        self._inputs = inputs
        self._outputs = outputs

    @property
    def inputs(self):
        """
        :rtype: _common_models.UrlBlob
        """
        return self._inputs

    @property
    def outputs(self):
        """
        :rtype: _common_models.UrlBlob
        """
        return self._outputs


class WorkflowExecutionGetDataResponse(_CommonDataResponse):
    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param _execution_pb2.WorkflowExecutionGetDataResponse pb2_object:
        :rtype: WorkflowExecutionGetDataResponse
        """
        return cls(
            inputs=_common_models.UrlBlob.from_flyte_idl(pb2_object.inputs),
            outputs=_common_models.UrlBlob.from_flyte_idl(pb2_object.outputs),
        )

    def to_flyte_idl(self):
        """
        :rtype: _execution_pb2.WorkflowExecutionGetDataResponse
        """
        return _execution_pb2.WorkflowExecutionGetDataResponse(
            inputs=self.inputs.to_flyte_idl(),
            outputs=self.outputs.to_flyte_idl(),
        )


class TaskExecutionGetDataResponse(_CommonDataResponse):
    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param _task_execution_pb2.TaskExecutionGetDataResponse pb2_object:
        :rtype: TaskExecutionGetDataResponse
        """
        return cls(
            inputs=_common_models.UrlBlob.from_flyte_idl(pb2_object.inputs),
            outputs=_common_models.UrlBlob.from_flyte_idl(pb2_object.outputs),
        )

    def to_flyte_idl(self):
        """
        :rtype: _task_execution_pb2.TaskExecutionGetDataResponse
        """
        return _task_execution_pb2.TaskExecutionGetDataResponse(
            inputs=self.inputs.to_flyte_idl(),
            outputs=self.outputs.to_flyte_idl(),
        )


class NodeExecutionGetDataResponse(_CommonDataResponse):
    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param _node_execution_pb2.NodeExecutionGetDataResponse pb2_object:
        :rtype: NodeExecutionGetDataResponse
        """
        return cls(
            inputs=_common_models.UrlBlob.from_flyte_idl(pb2_object.inputs),
            outputs=_common_models.UrlBlob.from_flyte_idl(pb2_object.outputs),
        )

    def to_flyte_idl(self):
        """
        :rtype: _node_execution_pb2.NodeExecutionGetDataResponse
        """
        return _node_execution_pb2.NodeExecutionGetDataResponse(
            inputs=self.inputs.to_flyte_idl(),
            outputs=self.outputs.to_flyte_idl(),
        )
