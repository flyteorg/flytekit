from flyteidl.core import errors_pb2 as _errors_pb2
from google.protobuf.timestamp_pb2 import Timestamp

from flytekit.models import common as _common


class ContainerError(_common.FlyteIdlEntity):
    class Kind(object):
        NON_RECOVERABLE = _errors_pb2.ContainerError.NON_RECOVERABLE
        RECOVERABLE = _errors_pb2.ContainerError.RECOVERABLE

    def __init__(
        self,
        code: str,
        message: str,
        kind: int,
        origin: int,
        timestamp: Timestamp = Timestamp(seconds=0, nanos=0),
        worker: str = "",
    ):
        """
        :param code: A succinct code about the error
        :param message: Whatever message you want to surface about the error
        :param kind: A value from the ContainerError.Kind enum.
        :param origin: A value from ExecutionError.ErrorKind. Don't confuse this with error kind, even though
          both are called kind.
        """
        self._code = code
        self._message = message
        self._kind = kind
        self._origin = origin
        self._timestamp = timestamp
        self._worker = worker

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
    def kind(self):
        """
        :rtype: int
        """
        return self._kind

    @property
    def origin(self) -> int:
        """
        The origin of the error, an enum value from ExecutionError.ErrorKind
        """
        return self._origin

    @property
    def timestamp(self) -> Timestamp:
        """
        The timestamp of the error, as number of seconds and nanos since Epoch
        """
        return self._timestamp

    @property
    def worker(self) -> int:
        """
        The worker name where the error originated
        """
        return self._worker

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.errors_pb2.ContainerError
        """
        return _errors_pb2.ContainerError(
            code=self.code,
            message=self.message,
            kind=self.kind,
            origin=self.origin,
            timestamp=self._timestamp,
            worker=self.worker,
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.errors_pb2.ContainerError proto:
        :rtype: ContainerError
        """
        return cls(proto.code, proto.message, proto.kind, proto.origin, proto.timestamp, proto.worker)


class ErrorDocument(_common.FlyteIdlEntity):
    def __init__(self, error):
        """
        :param ContainerError error:
        """
        self._error = error

    @property
    def error(self):
        """
        :rtype: ContainerError
        """
        return self._error

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.errors_pb2.ErrorDocument
        """
        return _errors_pb2.ErrorDocument(error=self.error.to_flyte_idl())

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.errors_pb2.ErrorDocument proto:
        :rtype: ErrorDocument
        """
        return cls(ContainerError.from_flyte_idl(proto.error))
