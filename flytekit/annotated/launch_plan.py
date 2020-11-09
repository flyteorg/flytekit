from __future__ import annotations

from typing import Any, Dict, List, Optional

from flytekit.annotated import workflow as _annotated_workflow
from flytekit.annotated.context_manager import FlyteContext, FlyteEntities
from flytekit.annotated.interface import Interface, transform_inputs_to_parameters
from flytekit.annotated.node import create_and_link_node
from flytekit.annotated.promise import translate_inputs_to_literals
from flytekit.common.launch_plan import SdkLaunchPlan
from flytekit.models import common as _common_models
from flytekit.models import interface as _interface_models
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models import literals as _literal_models
from flytekit.models import schedule as _schedule_model
from flytekit.models.core import identifier as _identifier_model


class LaunchPlan(object):
    # The reason we cache is simply because users may get the default launch plan twice for a single Workflow. We
    # don't want to create two defaults, could be confusing.
    CACHE = {}

    @staticmethod
    def get_default_launch_plan(ctx: FlyteContext, workflow: _annotated_workflow.Workflow) -> LaunchPlan:
        if workflow.name in LaunchPlan.CACHE:
            return LaunchPlan.CACHE[workflow.name]

        parameter_map = transform_inputs_to_parameters(ctx, workflow._native_interface)

        lp = LaunchPlan(
            name=workflow.name,
            workflow=workflow,
            parameters=parameter_map,
            fixed_inputs=_literal_models.LiteralMap(literals={}),
        )

        LaunchPlan.CACHE[workflow.name] = lp
        return lp

    @classmethod
    def create(
        cls,
        name: str,
        workflow: _annotated_workflow.Workflow,
        default_inputs: Dict[str, Any] = None,
        fixed_inputs: Dict[str, Any] = None,
    ) -> LaunchPlan:
        ctx = FlyteContext.current_context()
        default_inputs = default_inputs or {}
        fixed_inputs = fixed_inputs or {}
        # Default inputs come from two places, the original signature of the workflow function, and the default_inputs
        # argument to this function. We'll take the latter as having higher precedence.
        wf_signature_parameters = transform_inputs_to_parameters(ctx, workflow._native_interface)

        # Construct a new Interface object with just the default inputs given to get Parameters, maybe there's an
        # easier way to do this, think about it later.
        temp_inputs = {}
        for k, v in default_inputs.items():
            temp_inputs[k] = (workflow._native_interface.inputs[k], v)
        temp_interface = Interface(inputs=temp_inputs, outputs={})
        temp_signature = transform_inputs_to_parameters(ctx, temp_interface)
        wf_signature_parameters._parameters.update(temp_signature.parameters)

        # These are fixed inputs that cannot change at launch time. If the same argument is also in default inputs,
        # it'll be taken out from defaults in the LaunchPlan constructor
        fixed_literals = translate_inputs_to_literals(
            ctx,
            input_kwargs=fixed_inputs,
            interface=workflow.interface,
            native_input_types=workflow._native_interface.inputs,
        )
        fixed_lm = _literal_models.LiteralMap(literals=fixed_literals)

        lp = cls(name=name, workflow=workflow, parameters=wf_signature_parameters, fixed_inputs=fixed_lm)

        # This is just a convenience - we'll need the fixed inputs LiteralMap for when serializing the Launch Plan out
        # to protobuf, but for local execution and such, why not save the original Python native values as well so
        # we don't have to reverse it back every time.
        default_inputs.update(fixed_inputs)
        lp._saved_inputs = default_inputs

        if name in cls.CACHE:
            raise AssertionError(f"Launch plan named {name} was already created! Make sure your names are unique.")
        cls.CACHE[name] = lp
        return lp

    # TODO: Add QoS after it's done
    def __init__(
        self,
        name: str,
        workflow: _annotated_workflow.Workflow,
        parameters: _interface_models.ParameterMap,
        fixed_inputs: _literal_models.LiteralMap,
        schedule: _schedule_model.Schedule = None,
        notifications: List[_common_models.Notification] = None,
        labels: _common_models.Labels = None,
        annotations: _common_models.Annotations = None,
        raw_output_data_config: _common_models.RawOutputDataConfig = None,
    ):
        self._name = name
        self._workflow = workflow
        # Ensure fixed inputs are not in parameter map
        parameters = {
            k: v for k, v in parameters.parameters.items() if k not in fixed_inputs.literals and v.default is not None
        }
        self._parameters = _interface_models.ParameterMap(parameters=parameters)
        self._fixed_inputs = fixed_inputs
        # See create() for additional information
        self._saved_inputs = {}

        self._schedule = schedule
        self._notifications = notifications or []
        self._labels = labels
        self._annotations = annotations
        self._raw_output_data_config = raw_output_data_config

        # This will eventually hold the registerable launch plan
        self._registerable_entity: Optional[SdkLaunchPlan] = None

        FlyteEntities.entities.append(self)

    @property
    def name(self) -> str:
        return self._name

    @property
    def parameters(self) -> _interface_models.ParameterMap:
        return self._parameters

    @property
    def fixed_inputs(self) -> _literal_models.LiteralMap:
        return self._fixed_inputs

    @property
    def workflow(self) -> _annotated_workflow.Workflow:
        return self._workflow

    @property
    def saved_inputs(self) -> Dict[str, Any]:
        # See note in create()
        # Since the call-site will typically update the dict returned, and since update updates in place, let's return
        # a copy.
        # TODO: What issues will there be when we start introducing custom classes as input types?
        return self._saved_inputs.copy()

    @property
    def schedule(self) -> Optional[_schedule_model.Schedule]:
        return self._schedule

    @property
    def notifications(self) -> List[_common_models.Notification]:
        return self._notifications

    @property
    def labels(self) -> Optional[_common_models.Labels]:
        return self._labels

    @property
    def annotations(self) -> Optional[_common_models.Annotations]:
        return self._annotations

    @property
    def raw_output_data_config(self) -> Optional[_common_models.RawOutputDataConfig]:
        return self._raw_output_data_config

    def __call__(self, *args, **kwargs):
        if len(args) > 0:
            raise AssertionError("Only Keyword Arguments are supported for launch plan executions")

        ctx = FlyteContext.current_context()
        if ctx.compilation_state is not None:
            # This would literally be a copy paste of the workflow one with the one line change
            inputs = self.saved_inputs
            inputs.update(kwargs)
            return create_and_link_node(ctx, entity=self, interface=self.workflow._native_interface, **inputs)
        else:
            # Calling a launch plan should just forward the call to the workflow, nothing more. But let's add in the
            # saved inputs.
            inputs = self.saved_inputs
            inputs.update(kwargs)
            return self.workflow(*args, **inputs)

    def get_registerable_entity(self) -> SdkLaunchPlan:
        settings = FlyteContext.current_context().registration_settings
        if self._registerable_entity is not None:
            return self._registerable_entity

        if settings.iam_role:
            auth_role = _common_models.AuthRole(assumable_iam_role=settings.iam_role)
        elif settings.service_account:
            auth_role = _common_models.AuthRole(kubernetes_service_account=settings.service_account)
        else:
            auth_role = None

        sdk_workflow = self.workflow.get_registerable_entity()
        self._registerable_entity = SdkLaunchPlan(
            workflow_id=sdk_workflow.id,
            entity_metadata=_launch_plan_models.LaunchPlanMetadata(
                schedule=self.schedule, notifications=self.notifications,
            ),
            default_inputs=self.parameters,
            fixed_inputs=self.fixed_inputs,
            labels=self.labels or _common_models.Labels({}),
            annotations=self.annotations or _common_models.Annotations({}),
            auth_role=auth_role,  # TODO: Is None here okay?
            raw_output_data_config=self.raw_output_data_config or settings.raw_output_data_config,
        )

        # These two things are normally set to None in the SdkLaunchPlan constructor and filled in by
        # SdkRunnableLaunchPlan/the registration process, so we need to set them manually. The reason is because these
        # fields are not part of the underlying LaunchPlanSpec
        self._registerable_entity._interface = sdk_workflow.interface
        self._registerable_entity._id = _identifier_model.Identifier(
            resource_type=_identifier_model.ResourceType.LAUNCH_PLAN,
            project=settings.project,
            domain=settings.domain,
            name=self.name,
            version=settings.version,
        )
        return self._registerable_entity
