from typing import Optional

from flyteidl.plugins.kubeflow import tensorflow_pb2 as tensorflow_task
from flytekitplugins.kftensorflow.models import PS, Chief, RunPolicy, Worker

from flytekit.models import common


class TensorFlowJob(common.FlyteIdlEntity):
    def __init__(self, chief: Chief, ps: PS, worker: Worker, run_policy: Optional[RunPolicy] = None):
        self._chief = chief
        self._ps = ps
        self._worker = worker
        self._run_policy = run_policy

    @property
    def worker(self):
        return self._worker

    @property
    def ps(self):
        return self._ps

    @property
    def chief(self):
        return self._chief

    @property
    def run_policy(self):
        return self._run_policy

    def to_flyte_idl(self) -> tensorflow_task.DistributedTensorflowTrainingTask:
        training_task = tensorflow_task.DistributedTensorflowTrainingTask(
            chief_replicas=self.chief.to_flyte_idl(),
            worker_replicas=self.worker.to_flyte_idl(),
            ps_replicas=self.ps.to_flyte_idl(),
            run_policy=self.run_policy.to_flyte_idl() if self.run_policy else None,
        )
        return training_task
