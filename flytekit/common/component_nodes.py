from __future__ import absolute_import

import six as _six

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
    def promote_from_model(cls, base_model):
        """
        Takes the idl wrapper for a TaskNode and returns the hydrated Flytekit object for it by fetching it from the
        engine.

        :param flytekit.models.core.workflow.TaskNode base_model:
        :rtype: SdkTaskNode
        """
        from flytekit.common.tasks import task as _task
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
    def promote_from_model(cls, base_model):
        """
        :param flytekit.models.core.workflow.WorkflowNode base_model:
        :rtype: SdkWorkflowNode
        """
        from flytekit.common import workflow as _workflow, launch_plan as _launch_plan
        project = base_model.reference.project
        domain = base_model.reference.domain
        name = base_model.reference.name
        version = base_model.reference.version
        if base_model.launchplan_ref is not None:
            sdk_launch_plan = _launch_plan.SdkLaunchPlan.fetch(project, domain, name, version)
            return cls(sdk_launch_plan=sdk_launch_plan)
        elif base_model.sub_workflow_ref is not None:
            sdk_workflow = _workflow.SdkWorkflow.fetch(project, domain, name, version)
            return cls(sdk_workflow=sdk_workflow)
        else:
            raise _system_exceptions.FlyteSystemException("Bad workflow node model, neither subworkflow nor "
                                                          "launchplan specified.")
