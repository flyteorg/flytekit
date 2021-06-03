from __future__ import annotations

import inspect
from typing import Any, Callable, Dict, List, Optional, Type

from flytekit.core import workflow as _annotated_workflow
from flytekit.core.context_manager import FlyteContext, FlyteContextManager, FlyteEntities
from flytekit.core.interface import Interface, transform_inputs_to_parameters, transform_signature_to_interface
from flytekit.core.promise import create_and_link_node, translate_inputs_to_literals
from flytekit.core.reference_entity import LaunchPlanReference, ReferenceEntity
from flytekit.models import common as _common_models
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.models import schedule as _schedule_model


class LaunchPlan(object):
    # The reason we cache is simply because users may get the default launch plan twice for a single Workflow. We
    # don't want to create two defaults, could be confusing.
    CACHE = {}

    @staticmethod
    def get_default_launch_plan(ctx: FlyteContext, workflow: _annotated_workflow.WorkflowBase) -> LaunchPlan:
        """
        Users should probably call the get_or_create function defined below instead. A default launch plan is the one
        that will just pick up whatever default values are defined in the workflow function signature (if any) and
        use the default auth information supplied during serialization, with no notifications or schedules.

        :param ctx: This is not flytekit.current_context(). This is an internal context object. Users familiar with
          flytekit should feel free to use this however.
        :param workflow: The workflow to create a launch plan for.
        """
        if workflow.name in LaunchPlan.CACHE:
            return LaunchPlan.CACHE[workflow.name]

        parameter_map = transform_inputs_to_parameters(ctx, workflow.python_interface)

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
        workflow: _annotated_workflow.WorkflowBase,
        default_inputs: Dict[str, Any] = None,
        fixed_inputs: Dict[str, Any] = None,
        schedule: _schedule_model.Schedule = None,
        notifications: List[_common_models.Notification] = None,
        labels: _common_models.Labels = None,
        annotations: _common_models.Annotations = None,
        raw_output_data_config: _common_models.RawOutputDataConfig = None,
        auth_role: _common_models.AuthRole = None,
    ) -> LaunchPlan:
        ctx = FlyteContextManager.current_context()
        default_inputs = default_inputs or {}
        fixed_inputs = fixed_inputs or {}
        # Default inputs come from two places, the original signature of the workflow function, and the default_inputs
        # argument to this function. We'll take the latter as having higher precedence.
        wf_signature_parameters = transform_inputs_to_parameters(ctx, workflow.python_interface)

        # Construct a new Interface object with just the default inputs given to get Parameters, maybe there's an
        # easier way to do this, think about it later.
        temp_inputs = {}
        for k, v in default_inputs.items():
            temp_inputs[k] = (workflow.python_interface.inputs[k], v)
        temp_interface = Interface(inputs=temp_inputs, outputs={})
        temp_signature = transform_inputs_to_parameters(ctx, temp_interface)
        wf_signature_parameters._parameters.update(temp_signature.parameters)

        # These are fixed inputs that cannot change at launch time. If the same argument is also in default inputs,
        # it'll be taken out from defaults in the LaunchPlan constructor
        fixed_literals = translate_inputs_to_literals(
            ctx,
            incoming_values=fixed_inputs,
            flyte_interface_types=workflow.interface.inputs,
            native_types=workflow.python_interface.inputs,
        )
        fixed_lm = _literal_models.LiteralMap(literals=fixed_literals)

        lp = cls(
            name=name,
            workflow=workflow,
            parameters=wf_signature_parameters,
            fixed_inputs=fixed_lm,
            schedule=schedule,
            notifications=notifications,
            labels=labels,
            annotations=annotations,
            raw_output_data_config=raw_output_data_config,
            auth_role=auth_role,
        )

        # This is just a convenience - we'll need the fixed inputs LiteralMap for when serializing the Launch Plan out
        # to protobuf, but for local execution and such, why not save the original Python native values as well so
        # we don't have to reverse it back every time.
        default_inputs.update(fixed_inputs)
        lp._saved_inputs = default_inputs

        if name in cls.CACHE:
            raise AssertionError(f"Launch plan named {name} was already created! Make sure your names are unique.")
        cls.CACHE[name] = lp
        return lp

    @classmethod
    def get_or_create(
        cls,
        workflow: _annotated_workflow.WorkflowBase,
        name: Optional[str] = None,
        default_inputs: Dict[str, Any] = None,
        fixed_inputs: Dict[str, Any] = None,
        schedule: _schedule_model.Schedule = None,
        notifications: List[_common_models.Notification] = None,
        labels: _common_models.Labels = None,
        annotations: _common_models.Annotations = None,
        raw_output_data_config: _common_models.RawOutputDataConfig = None,
        auth_role: _common_models.AuthRole = None,
    ) -> LaunchPlan:
        """
        This function offers a friendlier interface for creating launch plans. If the name for the launch plan is not
        supplied, this assumes you are looking for the default launch plan for the workflow. If it is specified, it
        will be used. If creating the default launch plan, none of the other arguments may be specified.

        The resulting launch plan is also cached and if called again with the same name, the
        cached version is returned

        :param workflow: The Workflow to create a launch plan for.
        :param name: If you supply a name, keep it mind it needs to be unique. That is, project, domain, version, and
          this name form a primary key. If you do not supply a name, this function will assume you want the default
          launch plan for the given workflow.
        :param default_inputs: Default inputs, expressed as Python values.
        :param fixed_inputs: Fixed inputs, expressed as Python values. At call time, these cannot be changed.
        :param schedule: Optional schedule to run on.
        :param notifications: Notifications to send.
        :param labels: Optional labels to attach to executions created by this launch plan.
        :param annotations: Optional annotations to attach to executions created by this launch plan.
        :param raw_output_data_config: Optional location of offloaded data for things like S3, etc.
        :param auth_role: Add an auth role if necessary.
        """
        if name is None and (
            default_inputs is not None
            or fixed_inputs is not None
            or schedule is not None
            or notifications is not None
            or labels is not None
            or annotations is not None
            or raw_output_data_config is not None
            or auth_role is not None
        ):
            raise ValueError(
                "Only named launchplans can be created that have other properties. Drop the name if you want to create a default launchplan. Default launchplans cannot have any other associations"
            )

        if name is not None and name in LaunchPlan.CACHE:
            cached_outputs = vars(LaunchPlan.CACHE[name])

            notifications = notifications or []
            default_inputs = default_inputs or {}
            fixed_inputs = fixed_inputs or {}

            default_inputs.update(fixed_inputs)
            if (
                workflow != cached_outputs["_workflow"]
                or schedule != cached_outputs["_schedule"]
                or notifications != cached_outputs["_notifications"]
                or auth_role != cached_outputs["_auth_role"]
                or default_inputs != cached_outputs["_saved_inputs"]
                or labels != cached_outputs["_labels"]
                or annotations != cached_outputs["_annotations"]
                or raw_output_data_config != cached_outputs["_raw_output_data_config"]
            ):
                return AssertionError("The cached values aren't the same as the current call arguments")

            return LaunchPlan.CACHE[name]
        elif name is None and workflow.name in LaunchPlan.CACHE:
            return LaunchPlan.CACHE[workflow.name]

        # Otherwise, handle the default launch plan case
        if name is None:
            ctx = FlyteContext.current_context()
            lp = cls.get_default_launch_plan(ctx, workflow)
        else:
            lp = cls.create(
                name,
                workflow,
                default_inputs,
                fixed_inputs,
                schedule,
                notifications,
                labels,
                annotations,
                raw_output_data_config,
                auth_role,
            )
        LaunchPlan.CACHE[name or workflow.name] = lp
        return lp

    # TODO: Add QoS after it's done
    def __init__(
        self,
        name: str,
        workflow: _annotated_workflow.WorkflowBase,
        parameters: _interface_models.ParameterMap,
        fixed_inputs: _literal_models.LiteralMap,
        schedule: _schedule_model.Schedule = None,
        notifications: List[_common_models.Notification] = None,
        labels: _common_models.Labels = None,
        annotations: _common_models.Annotations = None,
        raw_output_data_config: _common_models.RawOutputDataConfig = None,
        auth_role: _common_models.AuthRole = None,
    ):
        self._name = name
        self._workflow = workflow
        # Ensure fixed inputs are not in parameter map
        parameters = {k: v for k, v in parameters.parameters.items() if k not in fixed_inputs.literals}
        self._parameters = _interface_models.ParameterMap(parameters=parameters)
        self._fixed_inputs = fixed_inputs
        # See create() for additional information
        self._saved_inputs = {}

        self._schedule = schedule
        self._notifications = notifications or []
        self._labels = labels
        self._annotations = annotations
        self._raw_output_data_config = raw_output_data_config
        self._auth_role = auth_role

        FlyteEntities.entities.append(self)

    @property
    def python_interface(self) -> Interface:
        return self.workflow.python_interface

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
    def workflow(self) -> _annotated_workflow.PythonFunctionWorkflow:
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
            inputs = self.saved_inputs
            inputs.update(kwargs)
            return create_and_link_node(ctx, entity=self, interface=self.workflow.python_interface, **inputs)
        else:
            # Calling a launch plan should just forward the call to the workflow, nothing more. But let's add in the
            # saved inputs.
            inputs = self.saved_inputs
            inputs.update(kwargs)
            return self.workflow(*args, **inputs)


class ReferenceLaunchPlan(ReferenceEntity, LaunchPlan):
    """
    A reference launch plan serves as a pointer to a Launch Plan that already exists on your Flyte installation. This
    object will not initiate a network call to Admin, which is why the user is asked to provide the expected interface.
    If at registration time the interface provided causes an issue with compilation, an error will be returned.
    """

    def __init__(
        self, project: str, domain: str, name: str, version: str, inputs: Dict[str, Type], outputs: Dict[str, Type]
    ):
        super().__init__(LaunchPlanReference(project, domain, name, version), inputs, outputs)


def reference_launch_plan(
    project: str,
    domain: str,
    name: str,
    version: str,
) -> Callable[[Callable[..., Any]], ReferenceLaunchPlan]:
    """
    A reference launch plan is a pointer to a launch plan that already exists on your Flyte installation. This
    object will not initiate a network call to Admin, which is why the user is asked to provide the expected interface.
    If at registration time the interface provided causes an issue with compilation, an error will be returned.
    """

    def wrapper(fn) -> ReferenceLaunchPlan:
        interface = transform_signature_to_interface(inspect.signature(fn))
        return ReferenceLaunchPlan(project, domain, name, version, interface.inputs, interface.outputs)

    return wrapper
