from typing import Dict

from flytekit.exceptions import system as _system_exceptions
from flytekit.loggers import remote_logger
from flytekit.models import launch_plan as _launch_plan_model
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as id_models
from flytekit.models.core import workflow as _workflow_model


class FlyteTaskNode(_workflow_model.TaskNode):
    """
    A class encapsulating a task that a Flyte node needs to execute.
    """

    def __init__(self, flyte_task: "flytekit.remote.task.FlyteTask"):
        self._flyte_task = flyte_task
        super(FlyteTaskNode, self).__init__(None)

    @property
    def reference_id(self) -> id_models.Identifier:
        """
        A globally unique identifier for the task.
        """
        return self._flyte_task.id

    @property
    def flyte_task(self) -> "flytekit.remote.tasks.task.FlyteTask":
        return self._flyte_task

    @classmethod
    def promote_from_model(
        cls,
        base_model: _workflow_model.TaskNode,
        tasks: Dict[id_models.Identifier, _task_model.TaskTemplate],
    ) -> "FlyteTaskNode":
        """
        Takes the idl wrapper for a TaskNode and returns the hydrated Flytekit object for it by fetching it with the
        FlyteTask control plane.

        :param base_model:
        :param tasks:
        """
        from flytekit.remote.task import FlyteTask

        if base_model.reference_id in tasks:
            task = tasks[base_model.reference_id]
            remote_logger.debug(f"Found existing task template for {task.id}, will not retrieve from Admin")
            flyte_task = FlyteTask.promote_from_model(task)
            return cls(flyte_task)

        raise _system_exceptions.FlyteSystemException(f"Task template {base_model.reference_id} not found.")


class FlyteWorkflowNode(_workflow_model.WorkflowNode):
    """A class encapsulating a workflow that a Flyte node needs to execute."""

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
    def launchplan_ref(self) -> id_models.Identifier:
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
        sub_workflows: Dict[id_models.Identifier, _workflow_model.WorkflowTemplate],
        node_launch_plans: Dict[id_models.Identifier, _launch_plan_model.LaunchPlanSpec],
        tasks: Dict[id_models.Identifier, _task_model.TaskTemplate],
    ) -> "FlyteWorkflowNode":
        from flytekit.remote import launch_plan as _launch_plan
        from flytekit.remote import workflow as _workflow

        if base_model.launchplan_ref is not None:
            return cls(
                flyte_launch_plan=_launch_plan.FlyteLaunchPlan.promote_from_model(
                    base_model.launchplan_ref, node_launch_plans[base_model.launchplan_ref]
                )
            )
        elif base_model.sub_workflow_ref is not None:
            # the workflow templates for sub-workflows should have been included in the original response
            if base_model.reference in sub_workflows:
                return cls(
                    flyte_workflow=_workflow.FlyteWorkflow.promote_from_model(
                        sub_workflows[base_model.reference],
                        sub_workflows=sub_workflows,
                        node_launch_plans=node_launch_plans,
                        tasks=tasks,
                    )
                )
            raise _system_exceptions.FlyteSystemException(f"Subworkflow {base_model.reference} not found.")

        raise _system_exceptions.FlyteSystemException(
            "Bad workflow node model, neither subworkflow nor launchplan specified."
        )


class FlyteBranchNode(_workflow_model.BranchNode):
    def __init__(self, if_else: _workflow_model.IfElseBlock):
        super().__init__(if_else)

    @classmethod
    def promote_from_model(
        cls,
        base_model: _workflow_model.BranchNode,
        sub_workflows: Dict[id_models.Identifier, _workflow_model.WorkflowTemplate],
        node_launch_plans: Dict[id_models.Identifier, _launch_plan_model.LaunchPlanSpec],
        tasks: Dict[id_models.Identifier, _task_model.TaskTemplate],
    ) -> "FlyteBranchNode":

        from flytekit.remote.nodes import FlyteNode

        block = base_model.if_else

        else_node = None
        if block.else_node:
            else_node = FlyteNode.promote_from_model(block.else_node, sub_workflows, node_launch_plans, tasks)

        block.case._then_node = FlyteNode.promote_from_model(
            block.case.then_node, sub_workflows, node_launch_plans, tasks
        )

        for o in block.other:
            o._then_node = FlyteNode.promote_from_model(o.then_node, sub_workflows, node_launch_plans, tasks)

        new_if_else_block = _workflow_model.IfElseBlock(block.case, block.other, else_node, block.error)

        return cls(new_if_else_block)
