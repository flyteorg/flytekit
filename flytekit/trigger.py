from datetime import timedelta
from typing import Any, Dict, List, Optional, Type, Union

import isodate
from flyteidl.core import identifier_pb2 as idl
from flyteidl.core import artifact_id_pb2 as art_id
from flyteidl.core import interface_pb2

from flytekit.core.artifact import TIME_PARTITION, Artifact, ArtifactQuery, Partition, TimePartition
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.tracker import TrackedInstance
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import WorkflowBase
from flytekit.models.interface import ParameterMap, Variable


class Trigger(TrackedInstance):
    """
     Trigger(
        trigger_on=[dailyArtifact, hourlyArtifact],
        inputs={
            "today_upstream": dailyArtifact,  # this means: use the matched artifact
            "yesterday_upstream": dailyArtifact.query(time_partition=dailyArtifact.time_partition - timedelta(days=1)),
            # this means: use the matched hourly artifact
            "other_daily_upstream": hourlyArtifact.query(partitions={"region": "LAX"}),
            "region": "SEA",  # static value that will be passed as input
            "other_artifact": UnrelatedArtifact.query(time_partition=dailyArtifact.time_partition - timedelta(days=1)),
            "other_artifact_2": UnrelatedArtifact.query(time_partition=hourlyArtifact.time_partition.truncate_to_day()),
            "other_artifact_3": UnrelatedArtifact.query(region=hourlyArtifact.time_partition.truncate_to_day()),
        },
    )
    """

    def __init__(
        self, trigger_on: List[Artifact], inputs: Optional[Dict[str, Union[Any, Artifact, ArtifactQuery]]] = None
    ):
        super().__init__()
        for t in trigger_on:
            if not isinstance(t, Artifact):
                raise ValueError("Trigger must be called with a list of artifacts")
        self.triggers = trigger_on
        self.inputs = inputs or {}  # user doesn't need to specify if the launch plan doesn't take inputs
        for k, v in self.inputs.items():
            if isinstance(v, ArtifactQuery):
                if v.bindings:
                    for b in v.bindings:
                        if b not in self.triggers:
                            raise ValueError(
                                f"Binding {b} id {id(b)} must reference a triggering artifact {self.triggers}"
                            )

            if isinstance(v, Artifact):
                if v not in self.triggers:
                    raise ValueError(f"Binding {v} id {id(v)} must reference a triggering artifact {self.triggers}")

    def get_parameter_map(
        self, input_python_interface: Dict[str, Type], input_typed_interface: Dict[str, Variable]
    ) -> interface_pb2.ParameterMap:
        """
        This is the key function that enables triggers to work. When declaring a trigger, the user specifies an input
        map in the form of artifacts, artifact time partitions, and artifact queries (possibly on unrelated artifacts).
        When it comes time to create the trigger, we need to convert all of these into a parameter map (because we've
        chosen Parameter as the method by which things like artifact queries are passed around). This function does
        that, and converts constants to Literals.
        """
        ctx = FlyteContextManager.current_context()
        pm = {}
        for k, v in self.inputs.items():
            var = input_typed_interface[k].to_flyte_idl()
            if isinstance(v, Artifact):
                aq = v.embed_as_query(self.triggers)
                p = interface_pb2.Parameter(var=var, artifact_query=aq)
            elif isinstance(v, ArtifactQuery):
                p = interface_pb2.Parameter(var=var, artifact_query=v.to_flyte_idl(self.triggers))
            elif isinstance(v, TimePartition):
                expr = None
                if v.op and v.other and isinstance(v.other, timedelta):
                    expr = str(v.op) + isodate.duration_isoformat(v.other)
                aq = v.reference_artifact.embed_as_query(self.triggers, TIME_PARTITION, expr)
                p = interface_pb2.Parameter(var=var, artifact_query=aq)
            elif isinstance(v, Partition):
                # The reason is that if we bind to arbitrary partitions, we'll have to start keeping track of types
                # and if the experiment service is written in non-python, we'd have to reimplement a type engine in
                # the other language
                raise ValueError(
                    "Don't bind to non-time partitions. Time partitions are okay because of the known type."
                )
            else:
                lit = TypeEngine.to_literal(ctx, v, input_python_interface[k], input_typed_interface[k].type)
                p = interface_pb2.Parameter(var=var, default=lit.to_flyte_idl())

            pm[k] = p
        return interface_pb2.ParameterMap(parameters=pm)

    def to_flyte_idl(self) -> art_id.Trigger:
        try:
            name = f"{self.instantiated_in}.{self.lhs}"
        except Exception:  # noqa broad for now given the changing nature of the tracker implementation.
            import random
            from uuid import UUID

            name = "trigger" + UUID(int=random.getrandbits(128)).hex

        # project/domain will be empty - to be bound later at registration time.
        artifact_ids = [a.to_flyte_idl().artifact_id for a in self.triggers]

        return art_id.Trigger(
            trigger_id=idl.Identifier(
                resource_type=idl.ResourceType.LAUNCH_PLAN,
                name=name,
            ),
            triggers=artifact_ids,
        )

    def __call__(self, *args, **kwargs):
        if len(kwargs) > 0:
            raise ValueError("Trigger does not support keyword arguments")
        if len(args) != 1:
            raise ValueError("Trigger must be called with a single function")

        entity = args[0]
        # This needs to create a Launch plan in the same
        if not isinstance(entity, WorkflowBase) and not isinstance(entity, LaunchPlan):
            raise ValueError("Trigger must be called with a workflow or launch plan")

        ctx = FlyteContextManager.current_context()

        pm = self.get_parameter_map(entity.python_interface.inputs, entity.interface.inputs)

        pm_model = ParameterMap.from_flyte_idl(pm)

        if isinstance(entity, LaunchPlan):
            raise NotImplementedError("Launch plan triggers are not yet supported")

        if isinstance(entity, WorkflowBase):
            default_lp = LaunchPlan.get_default_launch_plan(ctx, entity)
            trigger_lp = default_lp.clone_with(name=default_lp.name, parameters=pm_model)
            self_idl = self.to_flyte_idl()
            trigger_lp._additional_metadata = self_idl

            return trigger_lp
