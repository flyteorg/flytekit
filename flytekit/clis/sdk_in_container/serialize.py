import logging as _logging
import math as _math
import os as _os
import tarfile as _tarfile
from enum import Enum as _Enum
from typing import List

import click

import flytekit as _flytekit
from flytekit.annotated import context_manager as flyte_context
from flytekit.annotated.base_task import PythonTask
from flytekit.annotated.context_manager import InstanceVar
from flytekit.annotated.launch_plan import LaunchPlan
from flytekit.annotated.workflow import Workflow
from flytekit.clis.sdk_in_container.constants import CTX_PACKAGES
from flytekit.common import utils as _utils
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions.scopes import system_entry_point
from flytekit.common.tasks import task as _sdk_task
from flytekit.common.translator import get_serializable
from flytekit.common.utils import write_proto_to_file as _write_proto_to_file
from flytekit.configuration import internal as _internal_config
from flytekit.tools.fast_registration import compute_digest as _compute_digest
from flytekit.tools.fast_registration import filter_tar_file_fn as _filter_tar_file_fn
from flytekit.tools.module_loader import iterate_registerable_entities_in_order

# Identifier fields use placeholders for registration-time substitution.
# Additional fields, such as auth and the raw output data prefix have more complex structures
# and can be optional so they are not serialized with placeholders.
_PROJECT_PLACEHOLDER = "{{ registration.project }}"
_DOMAIN_PLACEHOLDER = "{{ registration.domain }}"
_VERSION_PLACEHOLDER = "{{ registration.version }}"


# During out of container serialize the absolute path of the flytekit virtualenv at serialization time won't match the
# in-container value at execution time. The following default value is used to provide the in-container virtualenv path
# but can be optionally overridden at serialization time based on the installation of your flytekit virtualenv.
_DEFAULT_FLYTEKIT_VIRTUALENV_ROOT = "/opt/venv/"
_DEFAULT_FLYTEKIT_RELATIVE_ENTRYPOINT_LOC = "bin/entrypoint.py"

CTX_IMAGE = "image"
CTX_LOCAL_SRC_ROOT = "local_source_root"
CTX_CONFIG_FILE_LOC = "config_file_loc"
CTX_FLYTEKIT_VIRTUALENV_ROOT = "flytekit_virtualenv_root"


class SerializationMode(_Enum):
    DEFAULT = 0
    FAST = 1


@system_entry_point
def serialize_tasks_only(pkgs, folder=None):
    """
    :param list[Text] pkgs:
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
        o._id = _identifier.Identifier(
            o.resource_type, _PROJECT_PLACEHOLDER, _DOMAIN_PLACEHOLDER, name, _VERSION_PLACEHOLDER
        )
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
def serialize_all(
    pkgs: List[str] = None,
    local_source_root: str = None,
    folder: str = None,
    mode: SerializationMode = None,
    image: str = None,
    config_path: str = None,
    flytekit_virtualenv_root: str = None,
):
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

    :param list[Text] pkgs:
    :param Text folder:

    :return:
    """

    # m = module (i.e. python file)
    # k = value of dir(m), type str
    # o = object (e.g. SdkWorkflow)
    env = {
        _internal_config.CONFIGURATION_PATH.env_var: config_path
        if config_path
        else _internal_config.CONFIGURATION_PATH.get(),
        _internal_config.IMAGE.env_var: image,
    }

    serialization_settings = flyte_context.SerializationSettings(
        project=_PROJECT_PLACEHOLDER,
        domain=_DOMAIN_PLACEHOLDER,
        version=_VERSION_PLACEHOLDER,
        image_config=flyte_context.get_image_config(img_name=image),
        env=env,
        flytekit_virtualenv_root=flytekit_virtualenv_root,
        entrypoint_settings=flyte_context.EntrypointSettings(
            path=_os.path.join(flytekit_virtualenv_root, _DEFAULT_FLYTEKIT_RELATIVE_ENTRYPOINT_LOC)
        ),
    )
    with flyte_context.FlyteContext.current_context().new_serialization_settings(
        serialization_settings=serialization_settings
    ) as ctx:
        loaded_entities = []
        for m, k, o in iterate_registerable_entities_in_order(pkgs, local_source_root=local_source_root):
            name = _utils.fqdn(m.__name__, k, entity_type=o.resource_type)
            _logging.debug("Found module {}\n   K: {} Instantiated in {}".format(m, k, o._instantiated_in))
            o._id = _identifier.Identifier(
                o.resource_type, _PROJECT_PLACEHOLDER, _DOMAIN_PLACEHOLDER, name, _VERSION_PLACEHOLDER
            )
            loaded_entities.append(o)
            ctx.serialization_settings.add_instance_var(InstanceVar(module=m, name=k, o=o))

        click.echo(f"Found {len(flyte_context.FlyteEntities.entities)} tasks/workflows")

        mode = mode if mode else SerializationMode.DEFAULT
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
                if isinstance(entity, PythonTask):
                    if mode == SerializationMode.DEFAULT:
                        serializable = get_serializable(ctx.serialization_settings, entity)
                    elif mode == SerializationMode.FAST:
                        serializable = get_serializable(ctx.serialization_settings, entity, fast=True)
                    else:
                        raise AssertionError(f"Unrecognized serialization mode: {mode}")
                else:
                    serializable = get_serializable(ctx.serialization_settings, entity)
                loaded_entities.append(serializable)

                if isinstance(entity, Workflow):
                    lp = LaunchPlan.get_default_launch_plan(ctx, entity)
                    launch_plan = get_serializable(ctx.serialization_settings, lp)
                    loaded_entities.append(launch_plan)

        zero_padded_length = _determine_text_chars(len(loaded_entities))
        for i, entity in enumerate(loaded_entities):
            if entity.has_registered:
                _logging.info(f"Skipping entity {entity.id} because already registered")
                continue
            serialized = entity.serialize()
            fname_index = str(i).zfill(zero_padded_length)
            fname = "{}_{}.pb".format(fname_index, entity.id.name)
            click.echo(f"  Writing type: {entity.id.resource_type_name()}, {entity.id.name} to\n    {fname}")
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
@click.option("--image", help="Text tag: e.g. somedocker.com/myimage:someversion123", required=False)
@click.option(
    "--local-source-root",
    required=False,
    help="Root dir for python code containing workflow definitions to operate on when not the current working directory"
    "Required for running `pyflyte serialize` in out of container mode",
)
@click.option(
    "--in-container-config-path",
    required=False,
    help="This is where the configuration for your task lives inside the container. "
    "The reason it needs to be a separate option is because this pyflyte utility cannot know where the Dockerfile "
    "writes the config file to. Required for running `pyflyte serialize` in out of container mode",
)
@click.option(
    "--in-container-virtualenv-root",
    required=False,
    help="This is the root of the flytekit virtual env in your container. "
    "The reason it needs to be a separate option is because this pyflyte utility cannot know where flytekit is "
    "installed inside your container. Required for running `pyflyte serialize` in out of container mode when "
    "your container installs the flytekit virtualenv outside of the default `/opt/venv`",
)
@click.pass_context
def serialize(ctx, image, local_source_root, in_container_config_path, in_container_virtualenv_root):
    """
    This command produces protobufs for tasks and templates.
    For tasks, one pb file is produced for each task, representing one TaskTemplate object.
    For workflows, one pb file is produced for each workflow, representing a WorkflowClosure object.  The closure
        object contains the WorkflowTemplate, along with the relevant tasks for that workflow.  In lieu of Admin,
        this serialization step will set the URN of the tasks to the fully qualified name of the task function.
    """
    if not image:
        image = _internal_config.IMAGE.get()
    if not image:
        raise click.UsageError("Could not find image from config, please specify a value for ``--image``")
    ctx.obj[CTX_IMAGE] = image
    ctx.obj[CTX_LOCAL_SRC_ROOT] = local_source_root
    click.echo("Serializing Flyte elements with image {}".format(image))

    if bool(local_source_root) != bool(in_container_config_path):
        raise click.UsageError(
            "When running out of container serialization you must specify --local-source-root AND --in-container-config-path"
        )
    ctx.obj[CTX_CONFIG_FILE_LOC] = in_container_config_path
    if local_source_root is not None and in_container_config_path is not None:
        # We're in the process of an out of container serialize call.
        # Set the entrypoint path to the in container default unless a user-specified option exists.
        ctx.obj[CTX_FLYTEKIT_VIRTUALENV_ROOT] = (
            in_container_virtualenv_root
            if in_container_virtualenv_root is not None
            else _DEFAULT_FLYTEKIT_VIRTUALENV_ROOT
        )
    else:
        # For in container serialize we make sure to never accept an override the entrypoint path and determine it here
        # instead.
        entrypoint_path = _os.path.abspath(_flytekit.__file__)
        if entrypoint_path.endswith(".pyc"):
            entrypoint_path = entrypoint_path[:-1]

        ctx.obj[CTX_FLYTEKIT_VIRTUALENV_ROOT] = entrypoint_path


@click.command("tasks")
@click.option("-f", "--folder", type=click.Path(exists=True))
@click.pass_context
def tasks(ctx, folder=None):
    pkgs = ctx.obj[CTX_PACKAGES]

    if folder:
        click.echo(f"Writing output to {folder}")

    serialize_tasks_only(pkgs, folder)


@click.command("workflows")
# For now let's just assume that the directory needs to exist. If you're docker run -v'ing, docker will create the
# directory for you so it shouldn't be a problem.
@click.option("-f", "--folder", type=click.Path(exists=True))
@click.pass_context
def workflows(ctx, folder=None):
    _logging.getLogger().setLevel(_logging.DEBUG)

    if folder:
        click.echo(f"Writing output to {folder}")

    pkgs = ctx.obj[CTX_PACKAGES]
    dir = ctx.obj[CTX_LOCAL_SRC_ROOT]
    serialize_all(
        pkgs,
        dir,
        folder,
        SerializationMode.DEFAULT,
        image=ctx.obj[CTX_IMAGE],
        config_path=ctx.obj[CTX_CONFIG_FILE_LOC],
        flytekit_virtualenv_root=ctx.obj[CTX_FLYTEKIT_VIRTUALENV_ROOT],
    )


@click.group("fast")
@click.pass_context
def fast(ctx):
    pass


@click.command("workflows")
@click.option("-f", "--folder", type=click.Path(exists=True))
@click.pass_context
def fast_workflows(ctx, folder=None):
    _logging.getLogger().setLevel(_logging.DEBUG)

    if folder:
        click.echo(f"Writing output to {folder}")

    pkgs = ctx.obj[CTX_PACKAGES]
    dir = ctx.obj[CTX_LOCAL_SRC_ROOT]
    serialize_all(
        pkgs,
        dir,
        folder,
        SerializationMode.FAST,
        image=ctx.obj[CTX_IMAGE],
        config_path=ctx.obj[CTX_CONFIG_FILE_LOC],
        flytekit_virtualenv_root=ctx.obj[CTX_FLYTEKIT_VIRTUALENV_ROOT],
    )

    source_dir = ctx.obj[CTX_LOCAL_SRC_ROOT]
    digest = _compute_digest(source_dir)
    folder = folder if folder else ""
    archive_fname = _os.path.join(folder, f"{digest}.tar.gz")
    click.echo(f"Writing compressed archive to {archive_fname}")
    # Write using gzip
    with _tarfile.open(archive_fname, "w:gz") as tar:
        tar.add(source_dir, arcname="", filter=_filter_tar_file_fn)


fast.add_command(fast_workflows)

serialize.add_command(tasks)
serialize.add_command(workflows)
serialize.add_command(fast)
