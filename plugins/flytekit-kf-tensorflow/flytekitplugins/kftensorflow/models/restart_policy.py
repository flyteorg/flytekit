from dataclasses import dataclass
from enum import Enum

from flyteidl.plugins.kubeflow.common_pb2 import RESTART_POLICY_ALWAYS, RESTART_POLICY_NEVER, RESTART_POLICY_ON_FAILURE


@dataclass
class RestartPolicy(Enum):
    """
    RestartPolicy describes how the replicas should be restarted
    """

    ALWAYS = RESTART_POLICY_ALWAYS
    FAILURE = RESTART_POLICY_ON_FAILURE
    NEVER = RESTART_POLICY_NEVER
