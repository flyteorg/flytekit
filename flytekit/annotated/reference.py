from __future__ import annotations

from typing import Dict, Type

from flytekit.annotated.launch_plan import ReferenceLaunchPlan
from flytekit.annotated.task import ReferenceTask
from flytekit.annotated.workflow import ReferenceWorkflow
from flytekit.common.exceptions.user import FlyteValidationException
from flytekit.models.core import identifier as _identifier_model


def get_reference_entity(
    resource_type: int,
    project: str,
    domain: str,
    name: str,
    version: str,
    inputs: Dict[str, Type],
    outputs: Dict[str, Type],
):
    if resource_type == _identifier_model.ResourceType.TASK:
        return ReferenceTask(project, domain, name, version, inputs, outputs)
    elif resource_type == _identifier_model.ResourceType.WORKFLOW:
        return ReferenceWorkflow(project, domain, name, version, inputs, outputs)
    elif resource_type == _identifier_model.ResourceType.LAUNCH_PLAN:
        return ReferenceLaunchPlan(project, domain, name, version, inputs, outputs)
    else:
        raise FlyteValidationException("Resource type must be one of task, workflow, or launch plan")
