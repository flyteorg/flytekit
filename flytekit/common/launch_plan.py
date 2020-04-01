from __future__ import absolute_import

from flytekit.common import sdk_bases as _sdk_bases, promise as _promises, interface as _interface, nodes as _nodes, \
    workflow_execution as _workflow_execution
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions import scopes as _exception_scopes, user as _user_exceptions

from flytekit.common.mixins import registerable as _registerable, hash as _hash_mixin, executable as _executable_mixin
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import sdk as _sdk_config, internal as _internal_config, auth as _auth_config
from flytekit.engines import loader as _engine_loader
from flytekit.models import launch_plan as _launch_plan_models, schedule as _schedule_model, interface as \
    _interface_models, literals as _literal_models, common as _common_models
from flytekit.models.core import identifier as _identifier_model, workflow as _workflow_models
import datetime as _datetime
import logging as _logging
import six as _six
import uuid as _uuid


class SdkLaunchPlan(
    _six.with_metaclass(
        _sdk_bases.ExtendedSdkType,
        _launch_plan_models.LaunchPlanSpec,
        _executable_mixin.ExecutableEntity,
    )
):
    def __init__(self, *args, **kwargs):
        super(SdkLaunchPlan, self).__init__(*args, **kwargs)
        # Set all the attributes we expect this class to have
        self._id = None

        # The interface is not set explicitly unless fetched in an engine context
        self._interface = None

    @classmethod
    def promote_from_model(cls, model):
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
            auth=model.auth,
        )

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
        lp = _engine_loader.get_engine().fetch_launch_plan(launch_plan_id)
        sdk_lp = cls.promote_from_model(lp.spec)
        sdk_lp._id = lp.id

        # TODO: Add a test for this, and this function as a whole
        wf_id = sdk_lp.workflow_id
        lp_wf = _workflow.SdkWorkflow.fetch(wf_id.project, wf_id.domain, wf_id.name, wf_id.version)
        sdk_lp._interface = lp_wf.interface
        return sdk_lp

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
        else:
            return False

    @property
    def auth(self):
        """
        :rtype: flytekit.models.LaunchPlan.Auth
        """
        fixed_auth = super(SdkLaunchPlan, self).auth
        if fixed_auth is not None and\
                (fixed_auth.assumable_iam_role is not None or fixed_auth.kubernetes_service_account is not None):
                return fixed_auth

        assumable_iam_role = _auth_config.ASSUMABLE_IAM_ROLE.get()
        kubernetes_service_account = _auth_config.KUBERNETES_SERVICE_ACCOUNT.get()

        if not (assumable_iam_role or kubernetes_service_account):
            _logging.warning("Using deprecated `role` from config. "
                             "Please update your config to use `assumable_iam_role` instead")
            assumable_iam_role = _sdk_config.ROLE.get()
        return _launch_plan_models.Auth(assumable_iam_role=assumable_iam_role,
                                        kubernetes_service_account=kubernetes_service_account)

    @property
    def interface(self):
        """
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
        return _engine_loader.get_engine().get_launch_plan(self).update(self.id, state)

    def _python_std_input_map_to_literal_map(self, inputs):
        """
        :param dict[Text,Any] inputs: A dictionary of Python standard inputs that will be type-checked and compiled
            to a LiteralMap
        :rtype: flytekit.models.literals.LiteralMap
        """
        return _type_helpers.pack_python_std_map_to_literal_map(
            inputs,
            {
                k: user_input.sdk_type
                for k, user_input in _six.iteritems(self.default_inputs.parameters) if k in inputs
            }
        )

    @_exception_scopes.system_entry_point
    def execute_with_literals(self, project, domain, literal_inputs, name=None, notification_overrides=None,
                              label_overrides=None, annotation_overrides=None):
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
        execution = _engine_loader.get_engine().get_launch_plan(self).execute(
            project,
            domain,
            name,
            literal_inputs,
            notification_overrides=notification_overrides,
            label_overrides=label_overrides,
            annotation_overrides=annotation_overrides,
        )
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
                "detected {} positional args.".format(self, len(args))
            )

        # Take the default values from the launch plan
        default_inputs = {
            k: v.sdk_default
            for k, v in _six.iteritems(self.default_inputs.parameters) if not v.required
        }
        default_inputs.update(input_map)

        bindings, upstream_nodes = self.interface.create_bindings_for_inputs(default_inputs)

        return _nodes.SdkNode(
            id=None,
            metadata=_workflow_models.NodeMetadata("", _datetime.timedelta(), _literal_models.RetryStrategy(0)),
            bindings=sorted(bindings, key=lambda b: b.var),
            upstream_nodes=upstream_nodes,
            sdk_launch_plan=self
        )

    def __repr__(self):
        """
        :rtype: Text
        """
        return "SdkLaunchPlan(ID: {} Interface: {} WF ID: {})".format(self.id, self.interface, self.workflow_id)


# The difference between this and the SdkLaunchPlan class is that this runnable class is supposed to only be used for
# launch plans loaded alongside the current Python interpreter.
class SdkRunnableLaunchPlan(
    _hash_mixin.HashOnReferenceMixin,
    SdkLaunchPlan,
    _registerable.RegisterableEntity,
):
    def __init__(
            self,
            sdk_workflow,
            default_inputs=None,
            fixed_inputs=None,
            role=None,
            schedule=None,
            notifications=None,
            labels=None,
            annotations=None,
            auth=None,
    ):
        """
        :param flytekit.common.workflow.SdkWorkflow sdk_workflow:
        :param dict[Text,flytekit.common.promise.Input] default_inputs:
        :param dict[Text,Any] fixed_inputs: These inputs will be fixed and not need to be set when executing this
            launch plan.
        :param Text role: Deprecated. IAM role to execute this launch plan with.
        :param flytekit.models.schedule.Schedule: Schedule to apply to this workflow.
        :param list[flytekit.models.common.Notification]: List of notifications to apply to this launch plan.
        :param flytekit.models.common.Labels labels: Any custom kubernetes labels to apply to workflows executed by this
            launch plan.
        :param flytekit.models.common.Annotations annotations: Any custom kubernetes annotations to apply to workflows
            executed by this launch plan.
            Any custom kubernetes annotations to apply to workflows executed by this launch plan.
        :param flytekit.models.launch_plan.Auth auth: The auth method with which to execute the workflow.
        """
        if role and auth:
            raise ValueError("Cannot set both role and auth. Role is deprecated, use auth instead.")

        fixed_inputs = fixed_inputs or {}
        default_inputs = default_inputs or {}

        if role:
            auth = _launch_plan_models.Auth(assumable_iam_role=role)

        # The constructor for SdkLaunchPlan sets the id to None anyways so we don't bother passing in an ID. The ID
        # should be set in one of three places,
        #   1) When the object is registered (in the code above)
        #   2) By the dynamic task code after this runnable object has already been __call__'ed. The SdkNode produced
        #      maintains a link to this object and will set the ID according to the configuration variables present.
        #   3) When SdkLaunchPlan.fetch() is run
        super(SdkRunnableLaunchPlan, self).__init__(
            None,
            _launch_plan_models.LaunchPlanMetadata(
                schedule=schedule or _schedule_model.Schedule(''),
                notifications=notifications or []
            ),
            _interface_models.ParameterMap(default_inputs),
            _type_helpers.pack_python_std_map_to_literal_map(
                fixed_inputs,
                {
                    k: _type_helpers.get_sdk_type_from_literal_type(var.type)
                    for k, var in _six.iteritems(sdk_workflow.interface.inputs) if k in fixed_inputs
                }
            ),
            labels or _common_models.Labels({}),
            annotations or _common_models.Annotations({}),
            auth,
        )
        self._interface = _interface.TypedInterface(
            {k: v.var for k, v in _six.iteritems(default_inputs)},
            sdk_workflow.interface.outputs
        )
        self._upstream_entities = {sdk_workflow}
        self._sdk_workflow = sdk_workflow

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
            _identifier_model.ResourceType.LAUNCH_PLAN,
            project,
            domain,
            name,
            version
        )
        _engine_loader.get_engine().get_launch_plan(self).register(id_to_register)
        self._id = id_to_register
        return _six.text_type(self.id)

    @classmethod
    def from_flyte_idl(cls, _):
        raise _user_exceptions.FlyteAssertion(
            "An SdkRunnableLaunchPlan must be created from a reference to local Python code only."
        )

    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.launch_plan.LaunchPlanSpec model:
        :rtype: SdkRunnableLaunchPlan
        """
        raise _user_exceptions.FlyteAssertion(
            "An SdkRunnableLaunchPlan must be created from a reference to local Python code only."
        )

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, project, domain, name, version=None):
        """
        This function uses the engine loader to call create a hydrated task from Admin.
        :param Text project:
        :param Text domain:
        :param Text name:
        :param Text version:
        :rtype: SdkRunnableLaunchPlan
        """
        raise _user_exceptions.FlyteAssertion(
            "An SdkRunnableLaunchPlan must be created from a reference to local Python code only."
        )

    @property
    def interface(self):
        """
        :rtype: flytekit.common.interface.TypedInterface
        """
        return self._interface

    @property
    def upstream_entities(self):
        """
        Task, workflow, and launch plan that need to be registered in advance of this workflow.
        :rtype: set[_registerable.RegisterableEntity]
        """
        return self._upstream_entities

    @property
    def workflow_id(self):
        """
        :rtype: flytekit.common.core.identifier.Identifier
        """
        return self._sdk_workflow.id

    def __repr__(self):
        """
        :rtype: Text
        """
        return "SdkRunnableLaunchPlan(ID: {} Interface: {} WF ID: {})".format(self.id, self.interface, self.workflow_id)
