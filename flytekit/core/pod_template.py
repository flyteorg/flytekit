from dataclasses import dataclass, field
from typing import Dict, Optional

from kubernetes.client.models import V1PodSpec

from flytekit.exceptions import user as _user_exceptions

PRIMARY_CONTAINER_DEFAULT_NAME = "primary"


@dataclass(init=True, repr=True, eq=True, frozen=True)
class PodTemplate(object):
    """Custom PodTemplate specification for a Task."""

    pod_spec: V1PodSpec = field(default_factory=lambda: V1PodSpec(containers=[]))
    primary_container_name: str = PRIMARY_CONTAINER_DEFAULT_NAME
    labels: Optional[Dict[str, str]] = None
    annotations: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if not self.primary_container_name:
            raise _user_exceptions.FlyteValidationException("A primary container name cannot be undefined")
