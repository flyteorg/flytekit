import flyteidl.admin.node_execution_pb2 as _node_execution_pb2
import pytz as _pytz

from flytekit.models import common as _common_models
from flytekit.models.core import execution as _core_execution
from flytekit.models.core import identifier as _identifier


class NodeExecutionClosure(_common_models.FlyteIdlEntity):
    def __init__(self, phase, started_at, duration, output_uri=None, error=None):
        """
        :param int phase:
        :param datetime.datetime started_at:
        :param datetime.timedelta duration:
        :param Text output_uri:
        :param flytekit.models.core.execution.ExecutionError error:
        """
        self._phase = phase
        self._started_at = started_at
        self._duration = duration
        self._output_uri = output_uri
        self._error = error

    @property
    def phase(self):
        """
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
    def duration(self):
        """
        :rtype: datetime.timedelta
        """
        return self._duration

    @property
    def output_uri(self):
        """
        :rtype: Text
        """
        return self._output_uri

    @property
    def error(self):
        """
        :rtype: flytekit.models.core.execution.ExecutionError
        """
        return self._error

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecutionClosure
        """
        obj = _node_execution_pb2.NodeExecutionClosure(
            phase=self.phase,
            output_uri=self.output_uri,
            error=self.error.to_flyte_idl() if self.error is not None else None,
        )
        obj.started_at.FromDatetime(self.started_at.astimezone(_pytz.UTC).replace(tzinfo=None))
        obj.duration.FromTimedelta(self.duration)
        return obj

    @classmethod
    def from_flyte_idl(cls, p):
        """
        :param flyteidl.admin.node_execution_pb2.NodeExecutionClosure p:
        :rtype: NodeExecutionClosure
        """
        return cls(
            phase=p.phase,
            output_uri=p.output_uri if p.HasField("output_uri") else None,
            error=_core_execution.ExecutionError.from_flyte_idl(p.error) if p.HasField("error") else None,
            started_at=p.started_at.ToDatetime().replace(tzinfo=_pytz.UTC),
            duration=p.duration.ToTimedelta(),
        )


class NodeExecution(_common_models.FlyteIdlEntity):
    def __init__(self, id, input_uri, closure):
        """
        :param flytekit.models.core.identifier.NodeExecutionIdentifier id:
        :param Text input_uri:
        :param NodeExecutionClosure closure:
        """
        self._id = id
        self._input_uri = input_uri
        self._closure = closure

    @property
    def id(self):
        """
        :rtype: flytekit.models.core.identifier.NodeExecutionIdentifier
        """
        return self._id

    @property
    def input_uri(self):
        """
        :rtype: Text
        """
        return self._input_uri

    @property
    def closure(self):
        """
        :rtype: NodeExecutionClosure
        """
        return self._closure

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecution
        """
        return _node_execution_pb2.NodeExecution(
            id=self.id.to_flyte_idl(), input_uri=self.input_uri, closure=self.closure.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, p):
        """
        :param flyteidl.admin.node_execution_pb2.NodeExecution p:
        :rtype: NodeExecution
        """
        return cls(
            id=_identifier.NodeExecutionIdentifier.from_flyte_idl(p.id),
            input_uri=p.input_uri,
            closure=NodeExecutionClosure.from_flyte_idl(p.closure),
        )
