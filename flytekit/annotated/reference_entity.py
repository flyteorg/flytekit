from typing import Any, Dict, Type

from flytekit.annotated.context_manager import ExecutionState, FlyteContext
from flytekit.annotated.interface import Interface
from flytekit.annotated.node import create_and_link_node
from flytekit.models.core import identifier as _identifier_model
from flytekit.annotated.context_manager import (
    BranchEvalMode,
)


class Reference(object):
    def __init__(
            self, type: int, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        self._id = _identifier_model.Identifier(_identifier_model.ResourceType.TASK, project, domain, name, version)

    @property
    def id(self) -> _identifier_model.Identifier:
        return self._id


class TaskReference(Reference):
    def __init__(
            self, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        super().__init__(_identifier_model.ResourceType.TASK, project, domain, name, version, *args, **kwargs)


class LaunchPlanReference(Reference):
    def __init__(
        self, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        super().__init__(_identifier_model.ResourceType.LAUNCH_PLAN, project, domain, name, version, *args, **kwargs)


class WorkflowReference(Reference):
    def __init__(
        self, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        super().__init__(_identifier_model.ResourceType.WORKFLOW, project, domain, name, version, *args, **kwargs)


class ReferenceEntity(object):
    def __init__(self, resource_type, project: str, domain: str, name: str, version: str, inputs: Dict[str, Type],
                 outputs: Dict[str, Type]):

        self._interface = Interface(inputs=inputs, outputs=outputs)
        if resource_type == _identifier_model.ResourceType.TASK:
            self._reference = TaskReference(project, domain, name, version)
        elif resource_type == _identifier_model.ResourceType.WORKFLOW:
            self._reference = WorkflowReference(project, domain, name, version)
        elif resource_type == _identifier_model.ResourceType.LAUNCH_PLAN:
            self._reference = LaunchPlanReference(project, domain, name, version)
        else:
            raise Exception("Must be one of task, workflow, or launch plan")

    def execute(self, **kwargs) -> Any:
        raise Exception("Remote reference entities cannot be run locally. You must mock this out.")

    def __call__(self, *args, **kwargs):
        if len(args) > 0:
            raise AssertionError("Only Keyword Arguments are supported executions")

        ctx = FlyteContext.current_context()

        # Produce a node if compiling
        if ctx.compilation_state is not None:
            return create_and_link_node(ctx, entity=self, interface=self._interface, **kwargs)
        elif (
            ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION
        ):
            if ctx.execution_state.branch_eval_mode == BranchEvalMode.BRANCH_SKIPPED:
                return

            return self.execute(**kwargs)

        raise AssertionError("Calling '()' on a reference entity only can be done in a compilation, or local "
                             "workflow execution context.")

    @property
    def interface(self) -> Interface:
        return self._interface

    @property
    def reference(self) -> Reference:
        return self._reference

    @property
    def name(self):
        return self._reference.id.name
