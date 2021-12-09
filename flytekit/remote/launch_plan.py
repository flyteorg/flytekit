from typing import Optional

from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.core.interface import Interface
from flytekit.core.type_engine import TypeEngine
from flytekit.engines.flyte import engine as _flyte_engine
from flytekit.models import interface as _interface_models
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models.core import identifier as id_models
from flytekit.remote import interface as _interface


class FlyteLaunchPlan(_launch_plan_models.LaunchPlanSpec):
    """A class encapsulating a remote Flyte launch plan."""

    def __init__(self, id, *args, **kwargs):
        super(FlyteLaunchPlan, self).__init__(*args, **kwargs)
        # Set all the attributes we expect this class to have
        self._id = id

        # The interface is not set explicitly unless fetched in an engine context
        self._interface = None

        self._python_interface = None

    @classmethod
    def promote_from_model(
        cls, id: id_models.Identifier, model: _launch_plan_models.LaunchPlanSpec
    ) -> "FlyteLaunchPlan":
        lp = cls(
            id=id,
            workflow_id=model.workflow_id,
            default_inputs=_interface_models.ParameterMap(model.default_inputs.parameters),
            fixed_inputs=model.fixed_inputs,
            entity_metadata=model.entity_metadata,
            labels=model.labels,
            annotations=model.annotations,
            auth_role=model.auth_role,
            raw_output_data_config=model.raw_output_data_config,
        )

        if lp.interface is not None:
            lp.guessed_python_interface = Interface(
                inputs=TypeEngine.guess_python_types(lp.interface.inputs),
                outputs=TypeEngine.guess_python_types(lp.interface.outputs),
            )

        return lp

    @property
    def id(self) -> id_models.Identifier:
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
    def workflow_id(self) -> id_models.Identifier:
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
    def resource_type(self) -> id_models.ResourceType:
        return id_models.ResourceType.LAUNCH_PLAN

    @property
    def entity_type_text(self) -> str:
        return "Launch Plan"

    @property
    def guessed_python_interface(self) -> Optional[Interface]:
        return self._python_interface

    @guessed_python_interface.setter
    def guessed_python_interface(self, value):
        if self._python_interface is not None:
            return
        self._python_interface = value

    @_exception_scopes.system_entry_point
    def update(self, state: _launch_plan_models.LaunchPlanState):
        if not self.id:
            raise _user_exceptions.FlyteAssertion(
                "Failed to update launch plan because the launch plan's ID is not set. Please call register to fetch "
                "or register the identifier first"
            )
        return _flyte_engine.get_client().update_launch_plan(self.id, state)

    def __repr__(self) -> str:
        return f"FlyteLaunchPlan(ID: {self.id} Interface: {self.interface} WF ID: {self.workflow_id})"
