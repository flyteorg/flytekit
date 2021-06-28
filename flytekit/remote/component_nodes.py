import logging as _logging
from typing import Dict

from flytekit.common.exceptions import system as _system_exceptions
from flytekit.models import task as _task_model
from flytekit.models.core import workflow as _workflow_model
from flytekit.remote import identifier as _identifier


class FlyteTaskNode(_workflow_model.TaskNode):
    def __init__(self, flyte_task: "flytekit.remote.tasks.task.FlyteTask"):
        self._flyte_task = flyte_task
        super(FlyteTaskNode, self).__init__(None)

    @property
    def reference_id(self) -> _identifier.Identifier:
        """A globally unique identifier for the task."""
        return self._flyte_task.id

    @property
    def flyte_task(self) -> "flytekit.remote.tasks.task.FlyteTask":
        return self._flyte_task

    @classmethod
    def promote_from_model(
        cls,
        base_model: _workflow_model.TaskNode,
        tasks: Dict[_identifier.Identifier, _task_model.TaskTemplate],
    ) -> "FlyteTaskNode":
        """
        Takes the idl wrapper for a TaskNode and returns the hydrated Flytekit object for it by fetching it with the
        FlyteTask control plane.

        :param base_model:
        :param tasks:
        """
        from flytekit.remote.tasks import task as _task

        if base_model.reference_id in tasks:
            task = tasks[base_model.reference_id]
            _logging.info(f"Found existing task template for {task.id}, will not retrieve from Admin")
            flyte_task = _task.FlyteTask.promote_from_model(task)
            return cls(flyte_task)

        # if not found, fetch it from Admin
        _logging.debug(f"Fetching task template for {base_model.reference_id} from Admin")
        return cls(
            _task.FlyteTask.fetch(
                base_model.reference_id.project,
                base_model.reference_id.domain,
                base_model.reference_id.name,
                base_model.reference_id.version,
            )
        )


class FlyteWorkflowNode(_workflow_model.WorkflowNode):
    def __init__(
        self,
        flyte_workflow: "flytekit.remote.workflow.FlyteWorkflow" = None,
        flyte_launch_plan: "flytekit.remote.launch_plan.FlyteLaunchPlan" = None,
    ):
        if flyte_workflow and flyte_launch_plan:
            raise _system_exceptions.FlyteSystemException(
                "FlyteWorkflowNode cannot be called with both a workflow and a launchplan specified, please pick "
                f"one. workflow: {flyte_workflow} launchPlan: {flyte_launch_plan}",
            )

        self._flyte_workflow = flyte_workflow
        self._flyte_launch_plan = flyte_launch_plan
        super(FlyteWorkflowNode, self).__init__(
            launchplan_ref=self._flyte_launch_plan.id if self._flyte_launch_plan else None,
            sub_workflow_ref=self._flyte_workflow.id if self._flyte_workflow else None,
        )

    def __repr__(self) -> str:
        if self.flyte_workflow is not None:
            return f"FlyteWorkflowNode with workflow: {self.flyte_workflow}"
        return f"FlyteWorkflowNode with launch plan: {self.flyte_launch_plan}"

    @property
    def launchplan_ref(self) -> _identifier.Identifier:
        """A globally unique identifier for the launch plan, which should map to Admin."""
        return self._flyte_launch_plan.id if self._flyte_launch_plan else None

    @property
    def sub_workflow_ref(self):
        return self._flyte_workflow.id if self._flyte_workflow else None

    @property
    def flyte_launch_plan(self) -> "flytekit.remote.launch_plan.FlyteLaunchPlan":
        return self._flyte_launch_plan

    @property
    def flyte_workflow(self) -> "flytekit.remote.workflow.FlyteWorkflow":
        return self._flyte_workflow

    @classmethod
    def promote_from_model(
        cls,
        base_model: _workflow_model.WorkflowNode,
        sub_workflows: Dict[_identifier.Identifier, _workflow_model.WorkflowTemplate],
        tasks: Dict[_identifier.Identifier, _task_model.TaskTemplate],
    ) -> "FlyteWorkflowNode":
        from flytekit.remote import launch_plan as _launch_plan
        from flytekit.remote import workflow as _workflow

        fetch_args = (
            base_model.reference.project,
            base_model.reference.domain,
            base_model.reference.name,
            base_model.reference.version,
        )

        if base_model.launchplan_ref is not None:
            return cls(flyte_launch_plan=_launch_plan.FlyteLaunchPlan.fetch(*fetch_args))
        elif base_model.sub_workflow_ref is not None:
            # the workflow tempaltes for sub-workflows should have been included in the original response
            if base_model.reference in sub_workflows:
                return cls(
                    flyte_workflow=_workflow.FlyteWorkflow.promote_from_model(
                        sub_workflows[base_model.reference],
                        sub_workflows=sub_workflows,
                        tasks=tasks,
                    )
                )

            # If not found for some reason, fetch it from Admin again. The reason there is a warning here but not for
            # tasks is because sub-workflows should always be passed along. Ideally subworkflows are never even
            # registered with Admin, so fetching from Admin ideelly doesn't return anything
            _logging.warning(f"Your subworkflow with id {base_model.reference} is not included in the promote call.")
            return cls(flyte_workflow=_workflow.FlyteWorkflow.fetch(*fetch_args))

        raise _system_exceptions.FlyteSystemException(
            "Bad workflow node model, neither subworkflow nor launchplan specified."
        )
