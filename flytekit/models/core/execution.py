import datetime
import typing

import flyteidl_rust as flyteidl

from flytekit.models import common as _common


class WorkflowExecutionPhase(object):
    """
    This class holds enum values used for setting notifications. See :py:class:`flytekit.Email`
    for sample usage.
    """

    UNDEFINED = int(flyteidl.workflow_execution.Phase.Undefined)
    QUEUED = int(flyteidl.workflow_execution.Phase.Queued)
    RUNNING = int(flyteidl.workflow_execution.Phase.Running)
    SUCCEEDING = int(flyteidl.workflow_execution.Phase.Succeeding)
    SUCCEEDED = int(flyteidl.workflow_execution.Phase.Succeeded)
    FAILING = int(flyteidl.workflow_execution.Phase.Failing)
    FAILED = int(flyteidl.workflow_execution.Phase.Failed)
    ABORTED = int(flyteidl.workflow_execution.Phase.Aborted)
    TIMED_OUT = int(flyteidl.workflow_execution.Phase.TimedOut)
    ABORTING = int(flyteidl.workflow_execution.Phase.Aborting)

    @classmethod
    def enum_to_string(cls, int_value):
        """
        :param int_value:
        :rtype: Text
        """
        for name, value in cls.__dict__.items():
            if value == int_value:
                return name
        return str(int_value)


class NodeExecutionPhase(object):
    UNDEFINED = flyteidl.node_execution.Phase.Undefined
    QUEUED = flyteidl.node_execution.Phase.Queued
    RUNNING = flyteidl.node_execution.Phase.Running
    SUCCEEDED = flyteidl.node_execution.Phase.Succeeded
    FAILING = flyteidl.node_execution.Phase.Failing
    FAILED = flyteidl.node_execution.Phase.Failed
    ABORTED = flyteidl.node_execution.Phase.Aborted
    SKIPPED = flyteidl.node_execution.Phase.Skipped
    TIMED_OUT = flyteidl.node_execution.Phase.TimedOut
    DYNAMIC_RUNNING = flyteidl.node_execution.Phase.DynamicRunning
    RECOVERED = flyteidl.node_execution.Phase.Recovered

    @classmethod
    def enum_to_string(cls, int_value):
        """
        :param int_value:
        :rtype: Text
        """
        for name, value in cls.__dict__.items():
            if value == int_value:
                return name
        return str(int_value)


class TaskExecutionPhase(object):
    UNDEFINED = flyteidl.task_execution.Phase.Undefined
    RUNNING = flyteidl.task_execution.Phase.Running
    SUCCEEDED = flyteidl.task_execution.Phase.Succeeded
    FAILED = flyteidl.task_execution.Phase.Failed
    ABORTED = flyteidl.task_execution.Phase.Aborted
    QUEUED = flyteidl.task_execution.Phase.Queued
    INITIALIZING = flyteidl.task_execution.Phase.Initializing
    WAITING_FOR_RESOURCES = flyteidl.task_execution.Phase.WaitingForResources

    @classmethod
    def enum_to_string(cls, int_value):
        """
        :param int_value:
        :rtype: Text
        """
        for name, value in cls.__dict__.items():
            if value == int_value:
                return name
        return str(int_value)


class ExecutionError(_common.FlyteIdlEntity):
    class ErrorKind(object):
        UNKNOWN = flyteidl.execution_error.ErrorKind.Unknown
        USER = flyteidl.execution_error.ErrorKind.User
        SYSTEM = flyteidl.execution_error.ErrorKind.System

    def __init__(self, code: str, message: str, error_uri: str, kind: int):
        """
        :param code:
        :param message:
        :param uri:
        :param kind:
        """
        self._code = code
        self._message = message
        self._error_uri = error_uri
        self._kind = kind

    @property
    def code(self):
        """
        :rtype: Text
        """
        return self._code

    @property
    def message(self):
        """
        :rtype: Text
        """
        return self._message

    @property
    def error_uri(self):
        """
        :rtype: Text
        """
        return self._error_uri

    @property
    def kind(self) -> int:
        """
        Enum value from ErrorKind
        """
        return self._kind

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_pb2.ExecutionError
        """
        return flyteidl.core.ExecutionError(
            code=self.code,
            message=self.message,
            error_uri=self.error_uri,
            kind=self.kind,
        )

    @classmethod
    def from_flyte_idl(cls, p):
        """
        :param flyteidl.core.execution_pb2.ExecutionError p:
        :rtype: ExecutionError
        """
        return cls(code=p.code, message=p.message, error_uri=p.error_uri, kind=p.kind)


class TaskLog(_common.FlyteIdlEntity):
    class MessageFormat(object):
        UNKNOWN = flyteidl.task_log.MessageFormat.Unknown
        CSV = flyteidl.task_log.MessageFormat.Csv
        JSON = flyteidl.task_log.MessageFormat.Json

    def __init__(
        self,
        uri: str,
        name: str,
        message_format: typing.Optional[MessageFormat] = None,
        ttl: typing.Optional[datetime.timedelta] = None,
    ):
        """
        :param Text uri:
        :param Text name:
        :param MessageFormat message_format: Enum value from TaskLog.MessageFormat
        :param datetime.timedelta ttl: The time the log will persist for.  0 represents unknown or ephemeral in nature.
        """
        self._uri = uri
        self._name = name
        self._message_format = message_format
        self._ttl = ttl

    @property
    def uri(self):
        """
        :rtype: Text
        """
        return self._uri

    @property
    def name(self):
        """
        :rtype: Text
        """
        return self._name

    @property
    def message_format(self):
        """
        Enum value from TaskLog.MessageFormat
        :rtype: MessageFormat
        """
        return self._message_format

    @property
    def ttl(self):
        """
        :rtype: datetime.timedelta
        """
        return self._ttl

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_pb2.TaskLog
        """
        p = flyteidl.core.TaskLog(uri=self.uri, name=self.name, message_format=self.message_format)
        if self.ttl is not None:
            p.ttl.FromTimedelta(self.ttl)
        return p

    @classmethod
    def from_flyte_idl(cls, p):
        """
        :param flyteidl.core.execution_pb2.TaskLog p:
        :rtype: TaskLog
        """
        return cls(
            uri=p.uri,
            name=p.name,
            message_format=p.message_format,
            ttl=p.ttl.ToTimedelta() if p.ttl else None,
        )
