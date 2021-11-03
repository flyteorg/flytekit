from flyteidl.plugins import pytorch_pb2 as _pytorch_task

from flytekit.models import common as _common


class PyTorchJob(_common.FlyteIdlEntity):
    def __init__(self, workers_count):
        self._workers_count = workers_count

    @property
    def workers_count(self):
        return self._workers_count

    def to_flyte_idl(self):
        return _pytorch_task.DistributedPyTorchTrainingTask(
            workers=self.workers_count,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            workers_count=pb2_object.workers,
        )
