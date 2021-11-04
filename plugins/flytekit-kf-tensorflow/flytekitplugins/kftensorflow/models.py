from flyteidl.plugins import tensorflow_pb2 as _tensorflow_task

from flytekit.models import common as _common


class TensorFlowJob(_common.FlyteIdlEntity):
    def __init__(self, workers_count, ps_replicas_count, chief_replicas_count):
        self._workers_count = workers_count
        self._ps_replicas_count = ps_replicas_count
        self._chief_replicas_count = chief_replicas_count

    @property
    def workers_count(self):
        return self._workers_count

    @property
    def ps_replicas_count(self):
        return self._ps_replicas_count

    @property
    def chief_replicas_count(self):
        return self._chief_replicas_count

    def to_flyte_idl(self):
        return _tensorflow_task.DistributedTensorflowTrainingTask(
            workers=self.workers_count, ps_replicas=self.ps_replicas_count, chief_replicas=self.chief_replicas_count
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            workers_count=pb2_object.workers,
            ps_replicas_count=pb2_object.ps_replicas,
            chief_replicas_count=pb2_object.chief_replicas,
        )
