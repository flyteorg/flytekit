import hashlib
import json
from dataclasses import asdict, dataclass, is_dataclass
from typing import TYPE_CHECKING, Dict, Optional

from flytekit.exceptions import user as _user_exceptions

if TYPE_CHECKING:
    from kubernetes.client import V1PodSpec

PRIMARY_CONTAINER_DEFAULT_NAME = "primary"


def serialize_pod_template(obj):
    if hasattr(obj, "to_dict"):
        d = obj.to_dict()
        if obj.__class__.__name__ == "V1Container":
            image = getattr(obj, "image", None)
            if hasattr(image, "image_name"):
                d["image"] = image.image_name()
        return {k: serialize_pod_template(v) for k, v in d.items()}
    elif isinstance(obj, list):
        return [serialize_pod_template(o) for o in obj]
    elif isinstance(obj, dict):
        return {k: serialize_pod_template(v) for k, v in obj.items()}
    elif is_dataclass(obj):
        return serialize_pod_template(asdict(obj))
    else:
        return obj


@dataclass(init=True, repr=True, eq=True, frozen=False)
class PodTemplate(object):
    """Custom PodTemplate specification for a Task."""

    pod_spec: Optional["V1PodSpec"] = None
    primary_container_name: str = PRIMARY_CONTAINER_DEFAULT_NAME
    labels: Optional[Dict[str, str]] = None
    annotations: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.pod_spec is None:
            from kubernetes.client import V1PodSpec

            self.pod_spec = V1PodSpec(containers=[])
        if not self.primary_container_name:
            raise _user_exceptions.FlyteValidationException("A primary container name cannot be undefined")

    def version_hash(self) -> str:
        data = serialize_pod_template(self)
        canonical_json = json.dumps(data, sort_keys=True)
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
