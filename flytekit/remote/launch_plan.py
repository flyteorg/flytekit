from __future__ import annotations

import textwrap
from typing import Optional

from flytekit.core import hash as hash_mixin
from flytekit.core.interface import Interface
from flytekit.models import interface as _interface_models
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models.core import identifier as id_models
from flytekit.remote import interface as _interface
from flytekit.remote.remote_callable import RemoteEntity


class FlyteLaunchPlan(hash_mixin.HashOnReferenceMixin, RemoteEntity, _launch_plan_models.LaunchPlanSpec):
    """A class encapsulating a remote Flyte launch plan."""

    def __init__(self, id, *args, **kwargs):
        super(FlyteLaunchPlan, self).__init__(*args, **kwargs)
        # Set all the attributes we expect this class to have
        self._id = id
        self._name = id.name

        # The interface is not set explicitly unless fetched in an engine context
        self._interface = None
        self._python_interface = None

    def verbose_string(self) -> str:

        """
        labels: _common.Labels,
        annotations: _common.Annotations,
        auth_role: _common.AuthRole,
        raw_output_data_config: _common.RawOutputDataConfig,
        max_parallelism=None,
        """
        header = f"""\
        Launch Plan ID:
          [{self.id.project}/{self.id.domain}]
          {self.name}@{self.id.version}
          Workflow:
          [{self.workflow_id.project}/{self.workflow_id.domain}]
          {self.workflow_id.name}@{self.workflow_id.version}        
        """
        header = textwrap.dedent(header)

        schedule = f"Schedule: {self.entity_metadata.schedule}" if self.entity_metadata.schedule else ""
        notifies = f"Notifications: {self.entity_metadata.notifications}" if self.entity_metadata.notifications else ""
        labels = f"Labels: {str(self.labels)}" if len(self.labels.values) > 0 else ""
        annotate = f"Annotations: {str(self.annotations)}" if len(self.annotations.values) > 0 else ""

        data = f"Offloaded data location: {self.raw_output_data_config.output_location_prefix or '(default)'}"
        iam = f"IAM Role: {self.auth_role.assumable_iam_role}" if self.auth_role.assumable_iam_role else ""
        svc = (
            f"Service Account: {self.auth_role.kubernetes_service_account}"
            if self.auth_role.kubernetes_service_account
            else ""
        )
        fixed = "Fixed inputs:" + (
            "\n" + self.fixed_inputs.verbose_string(indent=4) if len(self.fixed_inputs.literals) > 0 else " None"
        )
        defaults = "Default inputs:" + (
            "\n" + self.default_inputs.verbose_string(indent=4) if len(self.default_inputs.parameters) > 0 else " None"
        )

        # Filter out empty strings
        entries = filter(
            lambda x: bool(x), [header, schedule, notifies, labels, annotate, data, iam, svc, fixed, defaults]
        )
        return textwrap.dedent("\n".join(entries))

    @property
    def name(self) -> str:
        return self._name

        # If fetched when creating this object, can store it here.
        self._flyte_workflow = None

    @property
    def flyte_workflow(self) -> Optional["FlyteWorkflow"]:
        return self._flyte_workflow

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
    def interface(self) -> Optional[_interface.TypedInterface]:
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

    def __repr__(self) -> str:
        return f"FlyteLaunchPlan(ID: {self.id} Interface: {self.interface} WF ID: {self.workflow_id})"
