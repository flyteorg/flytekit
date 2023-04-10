from typing import Optional

from flyteidl.plugins import pytorch_pb2 as _pytorch_task

from flytekit.models import common as _common


class ElasticConfig(_common.FlyteIdlEntity):
    def __init__(
        self,
        rdzv_backend: Optional[str] = None,
        min_replicas: Optional[int] = None,
        max_replicas: Optional[int] = None,
        nproc_per_node: Optional[int] = None,
        max_restarts: Optional[int] = None,
    ):
        self._rdzv_backend = rdzv_backend
        self._min_replicas = min_replicas
        self._max_replicas = max_replicas
        self._nproc_per_node = nproc_per_node
        self._max_restarts = max_restarts

    @property
    def rdzv_backend(self):
        return self._rdzv_backend

    @property
    def min_replicas(self):
        return self._min_replicas

    @property
    def max_replicas(self):
        return self._max_replicas

    @property
    def nproc_per_node(self):
        return self._nproc_per_node

    @property
    def max_restarts(self):
        return self._max_restarts

    def to_flyte_idl(self):

        return _pytorch_task.ElasticConfig(
            rdzv_backend=self.rdzv_backend,
            min_replicas=self.min_replicas,
            max_replicas=self.max_replicas,
            nproc_per_node=self.nproc_per_node,
            max_restarts=self.max_restarts,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            rdzv_backend=pb2_object.rdzv_backend,
            min_replicas=pb2_object.min_replicas,
            max_replicas=pb2_object.max_replicas,
            nproc_per_node=pb2_object.nproc_per_node,
            max_restarts=pb2_object.max_restarts,
        )


class PyTorchJob(_common.FlyteIdlEntity):
    def __init__(
        self,
        workers_count: Optional[int]=None,
        elastic_config: Optional[ElasticConfig]=None,
    ):
        self._workers_count = workers_count
        self._elastic_config = elastic_config

    @property
    def workers_count(self):
        return self._workers_count

    @property
    def elastic_config(self):
        return self._elastic_config

    def to_flyte_idl(self):
        return _pytorch_task.DistributedPyTorchTrainingTask(
            workers=self._workers_count,
            elastic_config=self._elastic_config.to_flyte_idl() if self._elastic_config else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            workers_count=pb2_object.workers,
            elastic_config=ElasticConfig.from_flyte_idl(pb2_object.elastic_config) if pb2_object.elastic_config else None,
        )
