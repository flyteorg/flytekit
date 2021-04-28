import uuid as _uuid
from typing import Any, List

from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.control_plane import identifier as _identifier
from flytekit.control_plane import interface as _interface
from flytekit.control_plane import nodes as _nodes
from flytekit.control_plane import workflow_execution as _workflow_execution
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.models import common as _common_models
from flytekit.models import execution as _execution_models
from flytekit.models import interface as _interface_models
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models import literals as _literal_models
from flytekit.models.core import identifier as _identifier_model


class FlyteLaunchPlan(_launch_plan_models.LaunchPlanSpec):
    def __init__(self, *args, **kwargs):
        super(FlyteLaunchPlan, self).__init__(*args, **kwargs)
        # Set all the attributes we expect this class to have
        self._id = None

        # The interface is not set explicitly unless fetched in an engine context
        self._interface = None

    @classmethod
    def promote_from_model(cls, model: _launch_plan_models.LaunchPlanSpec) -> "FlyteLaunchPlan":
        return cls(
            workflow_id=_identifier.Identifier.promote_from_model(model.workflow_id),
            default_inputs=_interface_models.ParameterMap(model.default_inputs.parameters),
            fixed_inputs=model.fixed_inputs,
            entity_metadata=model.entity_metadata,
            labels=model.labels,
            annotations=model.annotations,
            auth_role=model.auth_role,
            raw_output_data_config=model.raw_output_data_config,
        )

    @_exception_scopes.system_entry_point
    def register(self, project, domain, name, version):
        # NOTE: does this need to be implemented in the control plane?
        pass

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, project: str, domain: str, name: str, version: str) -> "FlyteLaunchPlan":
        """
        This function uses the engine loader to call create a hydrated task from Admin.
        :param project:
        :param domain:
        :param name:
        :param version:
        """
        from flytekit.control_plane import workflow as _workflow

        launch_plan_id = _identifier.Identifier(
            _identifier_model.ResourceType.LAUNCH_PLAN, project, domain, name, version
        )

        lp = _flyte_engine.get_client().get_launch_plan(launch_plan_id)
        flyte_lp = cls.promote_from_model(lp.spec)
        flyte_lp._id = lp.id

        # TODO: Add a test for this, and this function as a whole
        wf_id = flyte_lp.workflow_id
        lp_wf = _workflow.FlyteWorkflow.fetch(wf_id.project, wf_id.domain, wf_id.name, wf_id.version)
        flyte_lp._interface = lp_wf.interface
        return flyte_lp

    @_exception_scopes.system_entry_point
    def serialize(self):
        """
        Serializing a launch plan should produce an object similar to what the registration step produces,
        in preparation for actual registration to Admin.

        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlan
        """
        # NOTE: does this need to be implemented in the control plane?
        pass

    @property
    def id(self) -> _identifier.Identifier:
        return self._id

    @property
    def is_scheduled(self) -> bool:
        if self.entity_metadata.schedule.cron_expression:
            return True
        elif self.entity_metadata.schedule.rate and self.entity_metadata.schedule.rate.value:
            return True
        elif self.entity_metadata.schedule.cron_schedule and self.entity_metadata.schedule.cron_schedule.schedule:
            return True
        else:
            return False

    @property
    def workflow_id(self) -> _identifier.Identifier:
        return self._workflow_id

    @property
    def interface(self) -> _interface.TypedInterface:
        """
        The interface is not technically part of the admin.LaunchPlanSpec in the IDL, however the workflow ID is, and
        from the workflow ID, fetch will fill in the interface. This is nice because then you can __call__ the=
        object and get a node.
        """
        return self._interface

    @property
    def resource_type(self) -> _identifier_model.ResourceType:
        return _identifier_model.ResourceType.LAUNCH_PLAN

    @property
    def entity_type_text(self) -> str:
        return "Launch Plan"

    @_exception_scopes.system_entry_point
    def validate(self):
        # TODO: Validate workflow is satisfied
        pass

    @_exception_scopes.system_entry_point
    def update(self, state: _launch_plan_models.LaunchPlanState):
        if not self.id:
            raise _user_exceptions.FlyteAssertion(
                "Failed to update launch plan because the launch plan's ID is not set. Please call register to fetch "
                "or register the identifier first"
            )
        return _flyte_engine.get_client().update_launch_plan(self.id, state)

    @_exception_scopes.system_entry_point
    def launch_with_literals(
        self,
        project: str,
        domain: str,
        literal_inputs: _literal_models.LiteralMap,
        name: str = None,
        notification_overrides: List[_common_models.Notification] = None,
        label_overrides: _common_models.Labels = None,
        annotation_overrides: _common_models.Annotations = None,
    ) -> _workflow_execution.FlyteWorkflowExecution:
        """
        Executes the launch plan and returns the execution identifier.  This version of execution is meant for when
        you already have a LiteralMap of inputs.

        :param project:
        :param domain:
        :param literal_inputs: Inputs to the execution.
        :param name: If specified, an execution will be created with this name.  Note: the name must
            be unique within the context of the project and domain.
        :param notification_overrides: If specified, these are the notifications that will be honored for this
            execution. An empty list signals to disable all notifications.
        :param label_overrides:
        :param annotation_overrides:
        """
        # Kubernetes requires names starting with an alphabet for some resources.
        name = name or "f" + _uuid.uuid4().hex[:19]
        disable_all = notification_overrides == []
        if disable_all:
            notification_overrides = None
        else:
            notification_overrides = _execution_models.NotificationList(notification_overrides or [])
            disable_all = None

        client = _flyte_engine.get_client()
        try:
            exec_id = client.create_execution(
                project,
                domain,
                name,
                _execution_models.ExecutionSpec(
                    self.id,
                    _execution_models.ExecutionMetadata(
                        _execution_models.ExecutionMetadata.ExecutionMode.MANUAL,
                        "sdk",  # TODO: get principle
                        0,  # TODO: Detect nesting
                    ),
                    notifications=notification_overrides,
                    disable_all=disable_all,
                    labels=label_overrides,
                    annotations=annotation_overrides,
                ),
                literal_inputs,
            )
        except _user_exceptions.FlyteEntityAlreadyExistsException:
            exec_id = _identifier.WorkflowExecutionIdentifier(project, domain, name)
        return _workflow_execution.FlyteWorkflowExecution.promote_from_model(client.get_execution(exec_id))

    @_exception_scopes.system_entry_point
    def __call__(self, *args, **input_map: Any) -> _nodes.FlyteNode:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"FlyteLaunchPlan(ID: {self.id} Interface: {self.interface} WF ID: {self.workflow_id})"
