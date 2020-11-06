import logging as _logging
import math as _math
import os as _os

import click

from flytekit.annotated import context_manager as flyte_context
from flytekit.annotated.launch_plan import LaunchPlan
from flytekit.annotated.task import PythonTask
from flytekit.annotated.workflow import Workflow
from flytekit.clis.sdk_in_container.constants import CTX_DOMAIN, CTX_PACKAGES, CTX_PROJECT, CTX_VERSION
from flytekit.common import utils as _utils
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions.scopes import system_entry_point
from flytekit.common.tasks import task as _sdk_task
from flytekit.common.utils import write_proto_to_file as _write_proto_to_file
from flytekit.configuration import TemporaryConfiguration
from flytekit.configuration import auth as _auth_config
from flytekit.configuration import internal as _internal_config
from flytekit.tools.module_loader import iterate_registerable_entities_in_order


@system_entry_point
def serialize_tasks_only(project, domain, pkgs, version, folder=None):
    """
    :param Text project:
    :param Text domain:
    :param list[Text] pkgs:
    :param Text version:
    :param Text folder:

    :return:
    """
    # m = module (i.e. python file)
    # k = value of dir(m), type str
    # o = object (e.g. SdkWorkflow)
    loaded_entities = []
    for m, k, o in iterate_registerable_entities_in_order(pkgs, include_entities={_sdk_task.SdkTask}):
        name = _utils.fqdn(m.__name__, k, entity_type=o.resource_type)
        _logging.debug("Found module {}\n   K: {} Instantiated in {}".format(m, k, o._instantiated_in))
        o._id = _identifier.Identifier(o.resource_type, project, domain, name, version)
        loaded_entities.append(o)

    zero_padded_length = _determine_text_chars(len(loaded_entities))
    for i, entity in enumerate(loaded_entities):
        serialized = entity.serialize()
        fname_index = str(i).zfill(zero_padded_length)
        fname = "{}_{}.pb".format(fname_index, entity._id.name)
        click.echo("  Writing {} to\n    {}".format(entity._id, fname))
        if folder:
            fname = _os.path.join(folder, fname)
        _write_proto_to_file(serialized, fname)

        identifier_fname = "{}_{}.identifier.pb".format(fname_index, entity._id.name)
        if folder:
            identifier_fname = _os.path.join(folder, identifier_fname)
        _write_proto_to_file(entity._id.to_flyte_idl(), identifier_fname)


@system_entry_point
def serialize_all(project, domain, pkgs, version, folder=None):
    """
    In order to register, we have to comply with Admin's endpoints. Those endpoints take the following objects. These
    flyteidl.admin.launch_plan_pb2.LaunchPlanSpec
    flyteidl.admin.workflow_pb2.WorkflowSpec
    flyteidl.admin.task_pb2.TaskSpec

    However, if we were to merely call .to_flyte_idl() on all the discovered entities, what we would get are:
    flyteidl.admin.launch_plan_pb2.LaunchPlanSpec
    flyteidl.core.workflow_pb2.WorkflowTemplate
    flyteidl.core.tasks_pb2.TaskTemplate

    For Workflows and Tasks therefore, there is special logic in the serialize function that translates these objects.

    :param Text project:
    :param Text domain:
    :param list[Text] pkgs:
    :param Text version:
    :param Text folder:

    :return:
    """

    # m = module (i.e. python file)
    # k = value of dir(m), type str
    # o = object (e.g. SdkWorkflow)
    env = {
        _internal_config.CONFIGURATION_PATH.env_var: _internal_config.CONFIGURATION_PATH.get(),
        _internal_config.IMAGE.env_var: _internal_config.IMAGE.get(),
    }

    registration_settings = flyte_context.RegistrationSettings(
        project=project,
        domain=domain,
        version=version,
        image=_internal_config.IMAGE.get(),
        env=env,
        iam_role=_auth_config.ASSUMABLE_IAM_ROLE.get(),
        service_account=_auth_config.KUBERNETES_SERVICE_ACCOUNT.get(),
        raw_output_data_config=_auth_config.RAW_OUTPUT_DATA_PREFIX.get(),
    )
    with flyte_context.FlyteContext.current_context().new_registration_settings(
        registration_settings=registration_settings
    ) as ctx:
        loaded_entities = []
        for m, k, o in iterate_registerable_entities_in_order(pkgs):
            name = _utils.fqdn(m.__name__, k, entity_type=o.resource_type)
            _logging.debug("Found module {}\n   K: {} Instantiated in {}".format(m, k, o._instantiated_in))
            o._id = _identifier.Identifier(o.resource_type, project, domain, name, version)
            loaded_entities.append(o)

        click.echo(f"Found {len(flyte_context.FlyteEntities.entities)} tasks/workflows")

        # TODO: Clean up the copy() - it's here because we call get_default_launch_plan, which may create a LaunchPlan
        #  object, which gets added to the FlyteEntities.entities list, which we're iterating over.
        for entity in flyte_context.FlyteEntities.entities.copy():
            # TODO: Add a reachable check. Since these entities are always added by the constructor, weird things can
            #  happen. If someone creates a workflow inside a workflow, we don't actually want the inner workflow to be
            #  registered. Or do we? Certainly, we don't want inner tasks to be registered because we don't know how
            #  to reach them, but perhaps workflows should be okay to take into account generated workflows.
            #  Also a user may import dir_b.workflows from dir_a.workflows but workflow packages might only
            #  specify dir_a

            if isinstance(entity, PythonTask) or isinstance(entity, Workflow) or isinstance(entity, LaunchPlan):
                serializable = entity.get_registerable_entity()
                loaded_entities.append(serializable)

                if isinstance(entity, Workflow):
                    lp = LaunchPlan.get_default_launch_plan(ctx, entity)
                    launch_plan = lp.get_registerable_entity()
                    loaded_entities.append(launch_plan)

        zero_padded_length = _determine_text_chars(len(loaded_entities))
        for i, entity in enumerate(loaded_entities):
            if entity.has_registered:
                _logging.info(f"Skipping entity {entity.id} because already registered")
                continue
            serialized = entity.serialize()
            fname_index = str(i).zfill(zero_padded_length)
            fname = "{}_{}.pb".format(fname_index, entity.id.name)
            click.echo(f"  Writing type: {entity.id.resource_type}, {entity.id.name} to\n    {fname}")
            if folder:
                fname = _os.path.join(folder, fname)
            _write_proto_to_file(serialized, fname)

            # Not everything serialized will necessarily have an identifier field in it, even though some do (like the
            # TaskTemplate). To be more rigorous, we write an explicit identifier file that reflects the choices (like
            # project/domain, etc.) made for this serialize call. We should not allow users to specify a different project
            # for instance come registration time, to avoid mismatches between potential internal ids like the TaskTemplate
            # and the registered entity.
            identifier_fname = "{}_{}.identifier.pb".format(fname_index, entity._id.name)
            if folder:
                identifier_fname = _os.path.join(folder, identifier_fname)
            _write_proto_to_file(entity._id.to_flyte_idl(), identifier_fname)


def _determine_text_chars(length):
    """
    This function is used to help prefix files. If there are only 10 entries, then we just need one digit (0-9) to be
    the prefix. If there are 11, then we'll need two (00-10).

    :param int length:
    :rtype: int
    """
    if length == 0:
        return 0
    return _math.ceil(_math.log(length, 10))


@click.group("serialize")
@click.pass_context
def serialize(ctx):
    """
    This command produces protobufs for tasks and templates.
    For tasks, one pb file is produced for each task, representing one TaskTemplate object.
    For workflows, one pb file is produced for each workflow, representing a WorkflowClosure object.  The closure
        object contains the WorkflowTemplate, along with the relevant tasks for that workflow.  In lieu of Admin,
        this serialization step will set the URN of the tasks to the fully qualified name of the task function.
    """
    click.echo("Serializing Flyte elements with image {}".format(_internal_config.IMAGE.get()))


@click.command("tasks")
@click.option(
    "-v",
    "--version",
    type=str,
    help="Version to serialize tasks with. This is normally parsed from the" "image, but you can override here.",
)
@click.option("-f", "--folder", type=click.Path(exists=True))
@click.pass_context
def tasks(ctx, version=None, folder=None):
    project = ctx.obj[CTX_PROJECT]
    domain = ctx.obj[CTX_DOMAIN]
    pkgs = ctx.obj[CTX_PACKAGES]

    if folder:
        click.echo(f"Writing output to {folder}")

    version = (
        version or ctx.obj[CTX_VERSION] or _internal_config.look_up_version_from_image_tag(_internal_config.IMAGE.get())
    )

    internal_settings = {
        "project": project,
        "domain": domain,
        "version": version,
    }
    # Populate internal settings for project/domain/version from the environment so that the file names are resolved
    # with the correct strings. The file itself doesn't need to change though.
    with TemporaryConfiguration(_internal_config.CONFIGURATION_PATH.get(), internal_settings):
        _logging.debug(
            "Serializing with settings\n"
            "\n  Project: {}"
            "\n  Domain: {}"
            "\n  Version: {}"
            "\n\nover the following packages {}".format(project, domain, version, pkgs)
        )
        serialize_tasks_only(project, domain, pkgs, version, folder)


@click.command("workflows")
@click.option(
    "-v",
    "--version",
    type=str,
    help="Version to serialize tasks with. This is normally parsed from the" "image, but you can override here.",
)
# For now let's just assume that the directory needs to exist. If you're docker run -v'ing, docker will create the
# directory for you so it shouldn't be a problem.
@click.option("-f", "--folder", type=click.Path(exists=True))
@click.pass_context
def workflows(ctx, version=None, folder=None):
    _logging.getLogger().setLevel(_logging.DEBUG)

    if folder:
        click.echo(f"Writing output to {folder}")

    project = ctx.obj[CTX_PROJECT]
    domain = ctx.obj[CTX_DOMAIN]
    pkgs = ctx.obj[CTX_PACKAGES]

    version = (
        version or ctx.obj[CTX_VERSION] or _internal_config.look_up_version_from_image_tag(_internal_config.IMAGE.get())
    )

    internal_settings = {
        "project": project,
        "domain": domain,
        "version": version,
    }
    # Populate internal settings for project/domain/version from the environment so that the file names are resolved
    # with the correct strings. The file itself doesn't need to change though.
    with TemporaryConfiguration(_internal_config.CONFIGURATION_PATH.get(), internal_settings):
        _logging.debug(
            "Serializing with settings\n"
            "\n  Project: {}"
            "\n  Domain: {}"
            "\n  Version: {}"
            "\n\nover the following packages {}".format(project, domain, version, pkgs)
        )
        serialize_all(project, domain, pkgs, version, folder)


serialize.add_command(tasks)
serialize.add_command(workflows)
