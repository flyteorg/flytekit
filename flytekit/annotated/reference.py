from __future__ import annotations

from flytekit.annotated.launch_plan import ReferenceLaunchPlan
from flytekit.models.core import identifier as _identifier_model


def get_reference_entity(resource_type: int, project: str, domain: str, name: str, version: str,
                         inputs: Dict[str, Type], outputs: Dict[str, Type]):
    if resource_type == _identifier_model.ResourceType.TASK:
        ...
    elif resource_type == _identifier_model.ResourceType.WORKFLOW:
        ...
    elif resource_type == _identifier_model.ResourceType.LAUNCH_PLAN:
        return ReferenceLaunchPlan(project, domain, name, version, inputs, outputs)
