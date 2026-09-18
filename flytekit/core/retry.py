import datetime
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, List, Optional, Union
from typing import Literal as L

from flyteidl.core import tasks_pb2

if TYPE_CHECKING:
    from kubernetes.client import V1PodSpec
from mashumaro.mixins.json import DataClassJSONMixin

from flytekit.core.constants import SHARED_MEMORY_MOUNT_NAME, SHARED_MEMORY_MOUNT_PATH
from flytekit.extras.accelerators import BaseAccelerator
from flytekit.models import task as task_models


@dataclass
class Backoff(DataClassJSONMixin):
    """
    This class is used to specify the backoff strategy for retries.

    .. code-block:: python

        Backoff(exponent=2, max=timedelta(minutes=2))  # This is a backoff strategy with an exponent of 2 and a max of 2 minutes

    """

    exponent: int = 0
    max: Optional[datetime.timedelta] = None

    def __post_init__(self):
        if self.exponent < 0:
            raise ValueError("Exponent must be a non-negative integer.")
        if self.max is not None and self.max.total_seconds() < 0:
            raise ValueError("Max must be a non-negative timedelta.")


@dataclass
class OnOOM(DataClassJSONMixin):
    """
    This class is used to specify the behavior when a task runs out of memory.

    .. code-block:: python

        OnOOM(backoff=Backoff(exponent=2, max=timedelta(minutes=2)), factor=1.0, limit="0")
    """

    factor: float = 1.2
    limit: str = "0"
    backoff: Optional[Backoff] = None

    def __post_init__(self):
        if self.factor <= 1.0:
            raise ValueError("Factor must be a non-negative float.")


@dataclass
class Retry(DataClassJSONMixin):
    """
    This class is used to specify the retry strategy for a task.

    .. code-block:: python

        Retry(attempts=3, on_oom=OnOOM(backoff=Backoff(exponent=2, max=timedelta(minutes=2)), factor=1.0, limit="0"))
    """

    attempts: int = 0
    on_oom: Optional[OnOOM] = None

    def __post_init__(self):
        if self.attempts < 0:
            raise ValueError("Attempts must be a non-negative integer.")
