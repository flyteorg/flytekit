from dataclasses import dataclass
from enum import Enum
from typing import Optional

from flyteidl.plugins.kubeflow import common_pb2 as kubeflow_common
from flyteidl.plugins.kubeflow.common_pb2 import CLEANPOD_POLICY_ALL, CLEANPOD_POLICY_NONE, CLEANPOD_POLICY_RUNNING

from flytekit.models import common


@dataclass
class CleanPodPolicy(Enum):
    """
    CleanPodPolicy describes how to deal with pods when the job is finished.
    """

    NONE = CLEANPOD_POLICY_NONE
    ALL = CLEANPOD_POLICY_ALL
    RUNNING = CLEANPOD_POLICY_RUNNING


class RunPolicy(common.FlyteIdlEntity):
    """
    Configuration for a dask worker group

    :param replicas: Number of workers in the group, minimum is 1
    :param image: Optional image to use for the pods of the worker group
    :param resources: Optional resources to use for the pods of the worker group
    """

    def __init__(
        self,
        clean_pod_policy: Optional[CleanPodPolicy],
        ttl_seconds_after_finished: Optional[int],
        active_deadline_seconds: Optional[int],
        backoff_limit: Optional[int],
    ):
        self._clean_pod_policy = clean_pod_policy
        self._ttl_seconds_after_finished = ttl_seconds_after_finished
        self._active_deadline_seconds = active_deadline_seconds
        self._backoff_limit = backoff_limit

    @property
    def clean_pod_policy(self) -> Optional[CleanPodPolicy]:
        return self._clean_pod_policy

    @property
    def ttl_seconds_after_finished(self) -> Optional[int]:
        return self._ttl_seconds_after_finished

    @property
    def active_deadline_seconds(self) -> Optional[int]:
        return self._active_deadline_seconds

    @property
    def backoff_limit(self) -> Optional[int]:
        return self._backoff_limit

    def to_flyte_idl(self) -> kubeflow_common.RunPolicy:
        return kubeflow_common.RunPolicy(
            clean_pod_policy=self._clean_pod_policy.value if self._clean_pod_policy else None,
            ttl_seconds_after_finished=self._ttl_seconds_after_finished,
            active_deadline_seconds=self._active_deadline_seconds,
            backoff_limit=self._backoff_limit,
        )
