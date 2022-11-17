import math
import os as _os
import sys
import typing
from collections import OrderedDict

import click

from flytekit import LaunchPlan
from flytekit.core import context_manager as flyte_context
from flytekit.core.base_task import PythonTask
from flytekit.core.workflow import WorkflowBase
from flytekit.exceptions.user import FlyteValidationException
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models import task as task_models
from flytekit.models.admin import workflow as admin_workflow_models
from flytekit.models.admin.workflow import WorkflowSpec
from flytekit.models.core import identifier as _identifier
from flytekit.models.task import TaskSpec
from flytekit.tools.translator import FlyteControlPlaneEntity, Options, get_serializable


def _determine_text_chars(length):
    """
    This function is used to help prefix files. If there are only 10 entries, then we just need one digit (0-9) to be
    the prefix. If there are 11, then we'll need two (00-10).

    :param int length:
    :rtype: int
    """
    if length == 0:
        return 0
    return math.ceil(math.log(length, 10))


def _should_register_with_admin(entity) -> bool:
    """
    This is used in the code below. The translator.py module produces lots of objects (namely nodes and BranchNodes)
    that do not/should not be written to .pb file to send to admin. This function filters them out.
    """
    return isinstance(
        entity, (task_models.TaskSpec, _launch_plan_models.LaunchPlan, admin_workflow_models.WorkflowSpec)
    )


def _find_duplicate_tasks(tasks: typing.List[task_models.TaskSpec]) -> typing.Set[task_models.TaskSpec]:
    """
    Given a list of `TaskSpec`, this function returns a set containing the duplicated `TaskSpec` if any exists.
    """
    seen: typing.Set[_identifier.Identifier] = set()
    duplicate_tasks: typing.Set[task_models.TaskSpec] = set()
    for task in tasks:
        if task.template.id not in seen:
            seen.add(task.template.id)
        else:
            duplicate_tasks.add(task)
    return duplicate_tasks


def get_registrable_entities(
    ctx: flyte_context.FlyteContext, options: typing.Optional[Options] = None
) -> typing.List[FlyteControlPlaneEntity]:
    """
    Returns all entities that can be serialized and should be sent over to Flyte backend. This will filter any entities
    that are not known to Admin
    """
    new_api_serializable_entities = OrderedDict()
    # TODO: Clean up the copy() - it's here because we call get_default_launch_plan, which may create a LaunchPlan
    #  object, which gets added to the FlyteEntities.entities list, which we're iterating over.
    for entity in flyte_context.FlyteEntities.entities.copy():
        if isinstance(entity, PythonTask) or isinstance(entity, WorkflowBase) or isinstance(entity, LaunchPlan):
            get_serializable(new_api_serializable_entities, ctx.serialization_settings, entity, options=options)

            if isinstance(entity, WorkflowBase):
                lp = LaunchPlan.get_default_launch_plan(ctx, entity)
                get_serializable(new_api_serializable_entities, ctx.serialization_settings, lp, options)

    new_api_model_values = list(new_api_serializable_entities.values())
    entities_to_be_serialized = list(filter(_should_register_with_admin, new_api_model_values))
    serializable_tasks: typing.List[task_models.TaskSpec] = [
        entity for entity in entities_to_be_serialized if isinstance(entity, task_models.TaskSpec)
    ]
    # Detect if any of the tasks is duplicated. Duplicate tasks are defined as having the same
    # metadata identifiers (see :py:class:`flytekit.common.core.identifier.Identifier`). Duplicate
    # tasks are considered invalid at registration
    # time and usually indicate user error, so we catch this common mistake at serialization time.
    duplicate_tasks = _find_duplicate_tasks(serializable_tasks)
    if len(duplicate_tasks) > 0:
        duplicate_task_names = [task.template.id.name for task in duplicate_tasks]
        raise FlyteValidationException(
            f"Multiple definitions of the following tasks were found: {duplicate_task_names}"
        )

    return entities_to_be_serialized


def persist_registrable_entities(entities: typing.List[FlyteControlPlaneEntity], folder: str):
    """
    For protobuf serializable list of entities, writes a file with the name if the entity and
    enumeration order to the specified folder

    This function will write to the folder specified the following protobuf types ::
        flyteidl.admin.launch_plan_pb2.LaunchPlan
        flyteidl.admin.workflow_pb2.WorkflowSpec
        flyteidl.admin.task_pb2.TaskSpec

    These can be inspected by calling (in the launch plan case) ::
        flyte-cli parse-proto -f filename.pb -p flyteidl.admin.launch_plan_pb2.LaunchPlan
    """
    zero_padded_length = _determine_text_chars(len(entities))
    for i, entity in enumerate(entities):
        fname_index = str(i).zfill(zero_padded_length)
        if isinstance(entity, TaskSpec):
            name = entity.template.id.name
            fname = "{}_{}_1.pb".format(fname_index, entity.template.id.name)
        elif isinstance(entity, WorkflowSpec):
            name = entity.template.id.name
            fname = "{}_{}_2.pb".format(fname_index, entity.template.id.name)
        elif isinstance(entity, _launch_plan_models.LaunchPlan):
            name = entity.id.name
            fname = "{}_{}_3.pb".format(fname_index, entity.id.name)
        else:
            click.secho(f"Entity is incorrect formatted {entity} - type {type(entity)}", fg="red")
            sys.exit(-1)
        click.secho(f"  Packaging {name} -> {fname}", dim=True)
        fname = _os.path.join(folder, fname)
        with open(fname, "wb") as writer:
            writer.write(entity.serialize_to_string())
