import six as _six

from flytekit.common import interface as _interface
from flytekit.common.exceptions import user as _user_exceptions, scopes as _exception_scopes
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import common as _common_models, launch_plan as _launch_plan_models, schedule as _schedule_model, \
    interface as _interface_models
from flytekit.platform.sdk_launch_plan import SdkLaunchPlan


# The difference between this and the SdkLaunchPlan class is that this runnable class is supposed to only be used for
# launch plans loaded alongside the current Python interpreter.
class SdkRunnableLaunchPlan(_hash_mixin.HashOnReferenceMixin, SdkLaunchPlan):
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
        auth_role=None,
        raw_output_data_config=None,
    ):
        """
        :param flytekit.common.local_workflow.SdkRunnableWorkflow sdk_workflow:
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
        :param flytekit.models.common.Authrole auth_role: The auth method with which to execute the workflow.
        :param flytekit.models.common.RawOutputDataConfig raw_output_data_config: Config for offloading data
        """
        if role and auth_role:
            raise ValueError("Cannot set both role and auth. Role is deprecated, use auth instead.")

        fixed_inputs = fixed_inputs or {}
        default_inputs = default_inputs or {}

        if role:
            auth_role = _common_models.AuthRole(assumable_iam_role=role)

        # The constructor for SdkLaunchPlan sets the id to None anyways so we don't bother passing in an ID. The ID
        # should be set in one of three places,
        #   1) When the object is registered (in the code above)
        #   2) By the dynamic task code after this runnable object has already been __call__'ed. The SdkNode produced
        #      maintains a link to this object and will set the ID according to the configuration variables present.
        #   3) When SdkLaunchPlan.fetch() is run
        super(SdkRunnableLaunchPlan, self).__init__(
            None,
            _launch_plan_models.LaunchPlanMetadata(
                schedule=schedule or _schedule_model.Schedule(""), notifications=notifications or [],
            ),
            _interface_models.ParameterMap(default_inputs),
            _type_helpers.pack_python_std_map_to_literal_map(
                fixed_inputs,
                {
                    k: _type_helpers.get_sdk_type_from_literal_type(var.type)
                    for k, var in _six.iteritems(sdk_workflow.interface.inputs)
                    if k in fixed_inputs
                },
            ),
            labels or _common_models.Labels({}),
            annotations or _common_models.Annotations({}),
            auth_role,
            raw_output_data_config or _common_models.RawOutputDataConfig(""),
        )
        self._interface = _interface.TypedInterface(
            {k: v.var for k, v in _six.iteritems(default_inputs)}, sdk_workflow.interface.outputs,
        )
        self._upstream_entities = {sdk_workflow}
        self._sdk_workflow = sdk_workflow

    @classmethod
    def from_flyte_idl(cls, _):
        raise _user_exceptions.FlyteAssertion(
            "An SdkRunnableLaunchPlan must be created from a reference to local Python code only."
        )

    @classmethod
    def promote_from_model(cls, model):
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