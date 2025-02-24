from flyteidl.event import event_pb2 as _event_pb2

from flytekit.models import common as _common


class TaskExecutionMetadata(_common.FlyteIdlEntity):
    """
    :param google.protobuf.internal.containers.RepeatedCompositeFieldContainer external_resources:
    """

    def __init__(self, external_resources=None):
        self._external_resources = external_resources

    @property
    def external_resources(self):
        """
        :rtype: google.protobuf.internal.containers.RepeatedCompositeFieldContainer
        """
        return self._external_resources

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.event.TaskExecutionMetadata
        """
        return _event_pb2.TaskExecutionMetadata(
            external_resources=self._external_resources,
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.event.event_pb2.TaskExecutionMetadata proto:
        :rtype: TaskExecutionMetadata
        """
        return cls(
            external_resources=proto.external_resources,
        )
