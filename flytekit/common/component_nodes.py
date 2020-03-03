from __future__ import absolute_import

import six as _six
import logging as _logging

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.models.core import workflow as _workflow_model


class SdkTaskNode(_six.with_metaclass(_sdk_bases.ExtendedSdkType, _workflow_model.TaskNode)):

    def __init__(self, sdk_task):
        """
        :param flytekit.common.tasks.task.SdkTask sdk_task:
        """
        self._sdk_task = sdk_task
        super(SdkTaskNode, self).__init__(None)

    @property
    def reference_id(self):
        """
        A globally unique identifier for the task.
        :rtype: flytekit.models.core.identifier.Identifier
        """
        return self._sdk_task.id

    @property
    def sdk_task(self):
        """
        :rtype: flytekit.common.tasks.task.SdkTask
        """
        return self._sdk_task

    @classmethod
    def promote_from_model(cls, base_model, tasks):
        """
        Takes the idl wrapper for a TaskNode and returns the hydrated Flytekit object for it by fetching it from the
        engine.

        :param flytekit.models.core.workflow.TaskNode base_model:
        :param list[flytekit.models.task.TaskTemplate] tasks:
        :rtype: SdkTaskNode
        """
        from flytekit.common.tasks import task as _task
        tasks = tasks or []
        for t in tasks:
            if t.id == base_model.reference_id:
                _logging.debug("Found existing task template for {}, will not retrieve from Admin".format(t.id))
                sdk_task = _task.SdkTask.promote_from_model(t)
                return cls(sdk_task)

        # If not found, fetch it from Admin
        _logging.debug("Fetching task template for {} from Admin".format(base_model.reference_id))
        project = base_model.reference_id.project
        domain = base_model.reference_id.domain
        name = base_model.reference_id.name
        version = base_model.reference_id.version
        sdk_task = _task.SdkTask.fetch(project, domain, name, version)
        return cls(sdk_task)


class SdkWorkflowNode(_six.with_metaclass(_sdk_bases.ExtendedSdkType, _workflow_model.WorkflowNode)):
    def __init__(self, sdk_workflow=None, sdk_launch_plan=None):
        """
        :param flytekit.common.workflow.SdkWorkflow sdk_workflow:
        :param flytekit.common.launch_plan.SdkLaunchPlan sdk_launch_plan:
        """
        self._sdk_workflow = sdk_workflow
        self._sdk_launch_plan = sdk_launch_plan
        super(SdkWorkflowNode, self).__init__()

    @property
    def launchplan_ref(self):
        """
        [Optional] A globally unique identifier for the launch plan.  Should map to Admin.
        :rtype: flytekit.models.core.identifier.Identifier
        """
        return self._sdk_launch_plan.id if self._sdk_launch_plan else None

    @property
    def sub_workflow_ref(self):
        """
        [Optional] Reference to a subworkflow, that should be defined with the compiler context.
        :rtype: flytekit.models.core.identifier.Identifier
        """
        return self._sdk_workflow.id if self._sdk_workflow else None

    @property
    def sdk_launch_plan(self):
        """
        :rtype: flytekit.common.launch_plan.SdkLaunchPlan
        """
        return self._sdk_launch_plan

    @property
    def sdk_workflow(self):
        """
        :rtype: flytekit.common.workflow.SdkWorkflow
        """
        return self._sdk_workflow

    @classmethod
    def promote_from_model(cls, base_model, sub_workflows=None, tasks=None):
        """
        :param flytekit.models.core.workflow.WorkflowNode base_model:
        :param list[flytekit.models.core.workflow.WorkflowTemplate] sub_workflows:
        :param list[flytekit.models.task.TaskTemplate] tasks:
        :rtype: SdkWorkflowNode
        """
        # put the import statement here to prevent circular dependency error
        from flytekit.common import workflow as _workflow, launch_plan as _launch_plan

        project = base_model.reference.project
        domain = base_model.reference.domain
        name = base_model.reference.name
        version = base_model.reference.version
        if base_model.launchplan_ref is not None:
            sdk_launch_plan = _launch_plan.SdkLaunchPlan.fetch(project, domain, name, version)
            return cls(sdk_launch_plan=sdk_launch_plan)
        elif base_model.sub_workflow_ref is not None:
            sub_workflows = sub_workflows or []
            # The workflow templates for sub-workflows should have been included in the original response
            for sw in sub_workflows:
                if sw.id == base_model.reference:
                    promoted =  _workflow.SdkWorkflow.promote_from_model(sw, sub_workflows=sub_workflows,
                                                                         tasks=tasks)
                    return cls(sdk_workflow=promoted)

            # If not found for some reason, fetch it from Admin again.
            # The reason there is a warning here but not for tasks is because sub-workflows should always be passed
            # along. Ideally subworkflows are never even registered with Admin, so fetching from Admin ideally doesn't
            # return anything.
            _logging.warning("Your subworkflow with id {} is not included in the promote call.".format(
                base_model.reference))
            sdk_workflow = _workflow.SdkWorkflow.fetch(project, domain, name, version)
            return cls(sdk_workflow=sdk_workflow)
        else:
            raise _system_exceptions.FlyteSystemException("Bad workflow node model, neither subworkflow nor "
                                                          "launchplan specified.")
