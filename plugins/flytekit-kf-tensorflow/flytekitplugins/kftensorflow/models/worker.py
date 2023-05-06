from enum import Enum
from typing import Optional

from flyteidl.plugins.kubeflow import tensorflow_pb2 as tensorflow_task

from flytekit.models import common, task

from .restart_policy import RestartPolicy


class Worker(common.FlyteIdlEntity):
    """
    Configuration for a worker replica group in a TFJob.

    :param replicas: Number of replicas in the group. Minimum is 1.
    :param image: Optional image to use for the pods of the group
    :param resources: Optional resources to use for the pods of the group
    :param restart_policy: Optional restart policy to use for the pods of the group
    """

    def __init__(
        self,
        replicas: int,
        image: Optional[str] = None,
        resources: Optional[task.Resources] = None,
        restart_policy: Optional[RestartPolicy] = None,
    ):
        if replicas < 1:
            raise ValueError(
                f"TFJob worker replica needs to have at least one worker, but {replicas} have been specified."
            )
        self._replicas = replicas
        self._image = image
        self._resources = resources
        self._restart_policy = restart_policy

    @property
    def image(self) -> Optional[str]:
        return self._image

    @property
    def resources(self) -> Optional[task.Resources]:
        return self._resources

    @property
    def replicas(self) -> Optional[int]:
        return self._replicas

    @property
    def restart_policy(self) -> Optional[RestartPolicy]:
        return self._restart_policy

    def to_flyte_idl(self) -> tensorflow_task.DistributedTensorflowTrainingReplicaSpec:
        return tensorflow_task.DistributedTensorflowTrainingReplicaSpec(
            replicas=self.replicas,
            image=self.image,
            resources=self.resources.to_flyte_idl() if self.resources else None,
            restart_policy=self.restart_policy.value if self.restart_policy else None,
        )
