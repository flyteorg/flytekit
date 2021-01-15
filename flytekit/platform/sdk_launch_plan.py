import datetime as _datetime
import logging as _logging
import uuid as _uuid

import six as _six
from deprecated.classic import deprecated as _deprecated

import flytekit.platform
from flytekit.common import sdk_bases as _sdk_bases, promise as _promises, workflow_execution as _workflow_execution, \
    nodes as _nodes
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions import scopes as _exception_scopes, user as _user_exceptions
from flytekit.common.mixins import launchable as _launchable_mixin, registerable as _registerable
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import auth as _auth_config, sdk as _sdk_config
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.models import launch_plan as _launch_plan_models, interface as _interface_models, \
    common as _common_models, execution as _execution_models, literals as _literal_models
from flytekit.models.core import identifier as _identifier_model, workflow as _workflow_models


class SdkLaunchPlan(
    _launchable_mixin.LaunchableEntity,
    _registerable.HasDependencies,
    _registerable.RegisterableEntity,
    _launch_plan_models.LaunchPlanSpec,
    metaclass=_sdk_bases.ExtendedSdkType,
):
    def __init__(self, *args, **kwargs):
        super(SdkLaunchPlan, self).__init__(*args, **kwargs)
        # Set all the attributes we expect this class to have
        self._id = None

        # The interface is not set explicitly unless fetched in an engine context
        self._interface = None

    @classmethod
    def promote_from_model(cls, model) -> "SdkLaunchPlan":
        """
        :param flytekit.models.launch_plan.LaunchPlanSpec model:
        :rtype: SdkLaunchPlan
        """
        return cls(
            workflow_id=_identifier.Identifier.promote_from_model(model.workflow_id),
            default_inputs=_interface_models.ParameterMap(
                {
                    k: _promises.Input.promote_from_model(v).rename_and_return_reference(k)
                    for k, v in _six.iteritems(model.default_inputs.parameters)
                }
            ),
            fixed_inputs=model.fixed_inputs,
            entity_metadata=model.entity_metadata,
            labels=model.labels,
            annotations=model.annotations,
            auth_role=model.auth_role,
            raw_output_data_config=model.raw_output_data_config,
        )

    @_exception_scopes.system_entry_point
    def register(self, project, domain, name, version):
        """
        :param Text project:
        :param Text domain:
        :param Text name:
        :param Text version:
        """
        self.validate()
        id_to_register = _identifier.Identifier(
            _identifier_model.ResourceType.LAUNCH_PLAN, project, domain, name, version
        )
        client = _flyte_engine.get_client()
        try:
            client.create_launch_plan(id_to_register, self)
        except _user_exceptions.FlyteEntityAlreadyExistsException:
            pass

        self._id = id_to_register
        self._has_registered = True
        return str(self.id)

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, project, domain, name, version=None):
        """
        This function uses the engine loader to call create a hydrated task from Admin.
        :param Text project:
        :param Text domain:
        :param Text name:
        :param Text version: [Optional] If not set, the SDK will fetch the active launch plan for the given project,
            domain, and name.
        :rtype: SdkLaunchPlan
        """
        from flytekit.common import workflow as _workflow

        launch_plan_id = _identifier.Identifier(
            _identifier_model.ResourceType.LAUNCH_PLAN, project, domain, name, version
        )

        if launch_plan_id.version:
            lp = _flyte_engine.get_client().get_launch_plan(launch_plan_id)
        else:
            named_entity_id = _common_models.NamedEntityIdentifier(
                launch_plan_id.project, launch_plan_id.domain, launch_plan_id.name
            )
            lp = _flyte_engine.get_client().get_active_launch_plan(named_entity_id)

        sdk_lp = cls.promote_from_model(lp.spec)
        sdk_lp._id = lp.id

        # TODO: Add a test for this, and this function as a whole
        wf_id = sdk_lp.workflow_id
        lp_wf = flytekit.platform.sdk_workflow.SdkWorkflow.fetch(wf_id.project, wf_id.domain, wf_id.name, wf_id.version)
        sdk_lp._interface = lp_wf.interface
        sdk_lp._has_registered = True
        return sdk_lp

    @_exception_scopes.system_entry_point
    def serialize(self):
        """
        Unlike the SdkWorkflow serialize call, nothing special needs to be done here.
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlanSpec
        """
        return self.to_flyte_idl()

    @property
    def id(self):
        """
        :rtype: flytekit.common.core.identifier.Identifier
        """
        return self._id

    @property
    def is_scheduled(self):
        """
        :rtype: bool
        """
        if self.entity_metadata.schedule.cron_expression:
            return True
        elif self.entity_metadata.schedule.rate and self.entity_metadata.schedule.rate.value:
            return True
        elif self.entity_metadata.schedule.cron_schedule and self.entity_metadata.schedule.cron_schedule.schedule:
            return True
        else:
            return False

    @property
    def auth_role(self):
        """
        :rtype: flytekit.models.common.AuthRole
        """
        fixed_auth = super(SdkLaunchPlan, self).auth_role
        if fixed_auth is not None and (
            fixed_auth.assumable_iam_role is not None or fixed_auth.kubernetes_service_account is not None
        ):
            return fixed_auth

        assumable_iam_role = _auth_config.ASSUMABLE_IAM_ROLE.get()
        kubernetes_service_account = _auth_config.KUBERNETES_SERVICE_ACCOUNT.get()

        if not (assumable_iam_role or kubernetes_service_account):
            _logging.warning(
                "Using deprecated `role` from config. Please update your config to use `assumable_iam_role` instead"
            )
            assumable_iam_role = _sdk_config.ROLE.get()
        return _common_models.AuthRole(
            assumable_iam_role=assumable_iam_role, kubernetes_service_account=kubernetes_service_account,
        )

    @property
    def workflow_id(self):
        """
        :rtype: flytekit.common.core.identifier.Identifier
        """
        return self._workflow_id

    @property
    def interface(self):
        """
        The interface is not technically part of the admin.LaunchPlanSpec in the IDL, however the workflow ID is, and
        from the workflow ID, fetch will fill in the interface. This is nice because then you can __call__ the=
        object and get a node.
        :rtype: flytekit.common.interface.TypedInterface
        """
        return self._interface

    @property
    def resource_type(self):
        """
        Integer from _identifier.ResourceType enum
        :rtype: int
        """
        return _identifier_model.ResourceType.LAUNCH_PLAN

    @property
    def entity_type_text(self):
        """
        :rtype: Text
        """
        return "Launch Plan"

    @property
    def raw_output_data_config(self):
        """
        :rtype: flytekit.models.common.RawOutputDataConfig
        """
        raw_output_data_config = super(SdkLaunchPlan, self).raw_output_data_config
        if raw_output_data_config is not None and raw_output_data_config.output_location_prefix != "":
            return raw_output_data_config

        # If it was not set explicitly then let's use the value found in the configuration.
        return _common_models.RawOutputDataConfig(_auth_config.RAW_OUTPUT_DATA_PREFIX.get())

    @_exception_scopes.system_entry_point
    def validate(self):
        # TODO: Validate workflow is satisfied
        pass

    @_exception_scopes.system_entry_point
    def update(self, state):
        """
        :param int state: Enum value from flytekit.models.launch_plan.LaunchPlanState
        """
        if not self.id:
            raise _user_exceptions.FlyteAssertion(
                "Failed to update launch plan because the launch plan's ID is not set. Please call register to fetch "
                "or register the identifier first"
            )
        return _flyte_engine.get_client().update_launch_plan(self.id, state)

    def _python_std_input_map_to_literal_map(self, inputs):
        """
        :param dict[Text,Any] inputs: A dictionary of Python standard inputs that will be type-checked and compiled
            to a LiteralMap
        :rtype: flytekit.models.literals.LiteralMap
        """
        return _type_helpers.pack_python_std_map_to_literal_map(
            inputs,
            {k: user_input.sdk_type for k, user_input in _six.iteritems(self.default_inputs.parameters) if k in inputs},
        )

    @_deprecated(reason="Use launch_with_literals instead", version="0.9.0")
    def execute_with_literals(
        self,
        project,
        domain,
        literal_inputs,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
    ):
        """
        Deprecated.
        """
        return self.launch_with_literals(
            project, domain, literal_inputs, name, notification_overrides, label_overrides, annotation_overrides,
        )

    @_exception_scopes.system_entry_point
    def launch_with_literals(
        self,
        project,
        domain,
        literal_inputs,
        name=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
    ):
        """
        Executes the launch plan and returns the execution identifier.  This version of execution is meant for when
        you already have a LiteralMap of inputs.

        :param Text project:
        :param Text domain:
        :param flytekit.models.literals.LiteralMap literal_inputs: Inputs to the execution.
        :param Text name: [Optional] If specified, an execution will be created with this name.  Note: the name must
            be unique within the context of the project and domain.
        :param list[flytekit.common.notifications.Notification] notification_overrides: [Optional] If specified, these
            are the notifications that will be honored for this execution.  An empty list signals to disable all
            notifications.
        :param flytekit.models.common.Labels label_overrides:
        :param flytekit.models.common.Annotations annotation_overrides:
        :rtype: flytekit.common.workflow_execution.SdkWorkflowExecution
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
        execution = client.get_execution(exec_id)
        return _workflow_execution.SdkWorkflowExecution.promote_from_model(execution)

    @_exception_scopes.system_entry_point
    def __call__(self, *args, **input_map):
        """
        :param list[T] args: Do not specify.  Kwargs only are supported for this function.
        :param dict[Text,T] input_map: Map of inputs.  Can be statically defined or OutputReference links.
        :rtype: flytekit.common.nodes.SdkNode
        """
        if len(args) > 0:
            raise _user_exceptions.FlyteAssertion(
                "When adding a launchplan as a node in a workflow, all inputs must be specified with kwargs only.  We "
                "detected {} positional args.".format(len(args))
            )

        # Take the default values from the launch plan
        default_inputs = {k: v.sdk_default for k, v in _six.iteritems(self.default_inputs.parameters) if not v.required}
        default_inputs.update(input_map)

        bindings, upstream_nodes = self.interface.create_bindings_for_inputs(default_inputs)

        return _nodes.SdkNode(
            id=None,
            metadata=_workflow_models.NodeMetadata("", _datetime.timedelta(), _literal_models.RetryStrategy(0)),
            bindings=sorted(bindings, key=lambda b: b.var),
            upstream_nodes=upstream_nodes,
            sdk_launch_plan=self,
        )

    def __repr__(self):
        """
        :rtype: Text
        """
        return "SdkLaunchPlan(ID: {} Interface: {} WF ID: {})".format(self.id, self.interface, self.workflow_id)