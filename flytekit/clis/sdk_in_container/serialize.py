import logging as _logging
import math as _math
import os as _os
import sys
import tarfile as _tarfile
import typing
from collections import OrderedDict
from enum import Enum as _Enum

import click
from flyteidl.admin.launch_plan_pb2 import LaunchPlan as _idl_admin_LaunchPlan
from flyteidl.admin.task_pb2 import TaskSpec as _idl_admin_TaskSpec
from flyteidl.admin.workflow_pb2 import WorkflowSpec as _idl_admin_WorkflowSpec

import flytekit as _flytekit
from flytekit.clis.sdk_in_container.constants import CTX_PACKAGES
from flytekit.common import utils as _utils
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions.scopes import system_entry_point
from flytekit.common.exceptions.user import FlyteValidationException
from flytekit.common.tasks import task as _sdk_task
from flytekit.common.translator import get_serializable
from flytekit.common.utils import write_proto_to_file as _write_proto_to_file
from flytekit.configuration import internal as _internal_config
from flytekit.core import context_manager as flyte_context
from flytekit.core.base_task import PythonTask
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.workflow import WorkflowBase
from flytekit.models import launch_plan as _launch_plan_models
from flytekit.models import task as task_models
from flytekit.models.admin import workflow as admin_workflow_models
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
CTX_PYTHON_INTERPRETER = "python_interpreter"


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


def get_registrable_entities(ctx: flyte_context.FlyteContext) -> typing.List:
    """
    Returns all entities that can be serialized and should be sent over to Flyte backend. This will filter any entities
    that are not known to Admin
    """
    new_api_serializable_entities = OrderedDict()
    # TODO: Clean up the copy() - it's here because we call get_default_launch_plan, which may create a LaunchPlan
    #  object, which gets added to the FlyteEntities.entities list, which we're iterating over.
    for entity in flyte_context.FlyteEntities.entities.copy():
        if isinstance(entity, PythonTask) or isinstance(entity, WorkflowBase) or isinstance(entity, LaunchPlan):
            get_serializable(new_api_serializable_entities, ctx.serialization_settings, entity)

            if isinstance(entity, WorkflowBase):
                lp = LaunchPlan.get_default_launch_plan(ctx, entity)
                get_serializable(new_api_serializable_entities, ctx.serialization_settings, lp)

    new_api_model_values = list(new_api_serializable_entities.values())
    entities_to_be_serialized = list(filter(_should_register_with_admin, new_api_model_values))
    serializable_tasks: typing.List[task_models.TaskSpec] = [
        entity for entity in entities_to_be_serialized if isinstance(entity, task_models.TaskSpec)
    ]
    # Detect if any of the tasks is duplicated. Duplicate tasks are defined as having the same metadata identifiers
    # (see :py:class:`flytekit.common.core.identifier.Identifier`). Duplicate tasks are considered invalid at registration
    # time and usually indicate user error, so we catch this common mistake at serialization time.
    duplicate_tasks = _find_duplicate_tasks(serializable_tasks)
    if len(duplicate_tasks) > 0:
        duplicate_task_names = [task.template.id.name for task in duplicate_tasks]
        raise FlyteValidationException(
            f"Multiple definitions of the following tasks were found: {duplicate_task_names}"
        )

    return [v.to_flyte_idl() for v in entities_to_be_serialized]


def persist_registrable_entities(entities: typing.List, folder: str):
    """
    For protobuf serializable list of entities, writes a file with the name if the entity and
    enumeration order to the specified folder
    """
    zero_padded_length = _determine_text_chars(len(entities))
    for i, entity in enumerate(entities):
        name = ""
        fname_index = str(i).zfill(zero_padded_length)
        if isinstance(entity, _idl_admin_TaskSpec):
            name = entity.template.id.name
            fname = "{}_{}_1.pb".format(fname_index, entity.template.id.name)
        elif isinstance(entity, _idl_admin_WorkflowSpec):
            name = entity.template.id.name
            fname = "{}_{}_2.pb".format(fname_index, entity.template.id.name)
        elif isinstance(entity, _idl_admin_LaunchPlan):
            name = entity.id.name
            fname = "{}_{}_3.pb".format(fname_index, entity.id.name)
        else:
            click.secho(f"Entity is incorrect formatted {entity} - type {type(entity)}", fg="red")
            sys.exit(-1)
        click.secho(f"  Packaging {name} -> {fname}", dim=True)
        fname = _os.path.join(folder, fname)
        with open(fname, "wb") as writer:
            writer.write(entity.SerializeToString())


@system_entry_point
def serialize_all(
    pkgs: typing.List[str] = None,
    local_source_root: str = None,
    folder: str = None,
    mode: SerializationMode = None,
    image: str = None,
    config_path: str = None,
    flytekit_virtualenv_root: str = None,
    python_interpreter: str = None,
):
    """
    This function will write to the folder specified the following protobuf types ::
        flyteidl.admin.launch_plan_pb2.LaunchPlan
        flyteidl.admin.workflow_pb2.WorkflowSpec
        flyteidl.admin.task_pb2.TaskSpec

    These can be inspected by calling (in the launch plan case) ::
        flyte-cli parse-proto -f filename.pb -p flyteidl.admin.launch_plan_pb2.LaunchPlan

    See :py:class:`flytekit.models.core.identifier.ResourceType` to match the trailing index in the file name with the
    entity type.
    :param pkgs: Dot-delimited Python packages/subpackages to look into for serialization.
    :param local_source_root: Where to start looking for the code.
    :param folder: Where to write the output protobuf files
    :param mode: Regular vs fast
    :param image: The fully qualified and versioned default image to use
    :param config_path: Path to the config file, if any, to be used during serialization
    :param flytekit_virtualenv_root: The full path of the virtual env in the container.
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

    if not (mode == SerializationMode.DEFAULT or mode == SerializationMode.FAST):
        raise AssertionError(f"Unrecognized serialization mode: {mode}")
    fast_serialization_settings = flyte_context.FastSerializationSettings(
        enabled=mode == SerializationMode.FAST,
        # TODO: if we want to move the destination dir as a serialization argument, we should initialize it here
    )
    serialization_settings = flyte_context.SerializationSettings(
        project=_PROJECT_PLACEHOLDER,
        domain=_DOMAIN_PLACEHOLDER,
        version=_VERSION_PLACEHOLDER,
        image_config=flyte_context.get_image_config(img_name=image),
        env=env,
        flytekit_virtualenv_root=flytekit_virtualenv_root,
        python_interpreter=python_interpreter,
        entrypoint_settings=flyte_context.EntrypointSettings(
            path=_os.path.join(flytekit_virtualenv_root, _DEFAULT_FLYTEKIT_RELATIVE_ENTRYPOINT_LOC)
        ),
        fast_serialization_settings=fast_serialization_settings,
    )
    ctx = flyte_context.FlyteContextManager.current_context().with_serialization_settings(serialization_settings)
    with flyte_context.FlyteContextManager.with_context(ctx) as ctx:
        old_style_entities = []
        # This first for loop is for legacy API entities - SdkTask, SdkWorkflow, etc. The _get_entity_to_module
        # function that this iterate calls only works on legacy objects
        for m, k, o in iterate_registerable_entities_in_order(pkgs, local_source_root=local_source_root):
            name = _utils.fqdn(m.__name__, k, entity_type=o.resource_type)
            _logging.debug("Found module {}\n   K: {} Instantiated in {}".format(m, k, o._instantiated_in))
            o._id = _identifier.Identifier(
                o.resource_type, _PROJECT_PLACEHOLDER, _DOMAIN_PLACEHOLDER, name, _VERSION_PLACEHOLDER
            )
            old_style_entities.append(o)

        serialized_old_style_entities = []
        for entity in old_style_entities:
            if entity.has_registered:
                _logging.info(f"Skipping entity {entity.id} because already registered")
                continue
            serialized_old_style_entities.append(entity.serialize())

        click.echo(f"Found {len(flyte_context.FlyteEntities.entities)} tasks/workflows")

        new_api_model_values = get_registrable_entities(ctx)

        loaded_entities = serialized_old_style_entities + new_api_model_values
        if folder is None:
            folder = "."
        persist_registrable_entities(loaded_entities, folder)

        click.secho(f"Successfully serialized {len(loaded_entities)} flyte objects", fg="green")


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
    "Optional when running `pyflyte serialize` in out of container mode and your code lies outside of your working directory",
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
    ctx.obj[CTX_IMAGE] = image

    if local_source_root is None:
        local_source_root = _os.getcwd()
    ctx.obj[CTX_LOCAL_SRC_ROOT] = local_source_root
    click.echo("Serializing Flyte elements with image {}".format(image))

    ctx.obj[CTX_CONFIG_FILE_LOC] = in_container_config_path
    if in_container_config_path is not None:
        # We're in the process of an out of container serialize call.
        # Set the entrypoint path to the in container default unless a user-specified option exists.
        ctx.obj[CTX_FLYTEKIT_VIRTUALENV_ROOT] = (
            in_container_virtualenv_root
            if in_container_virtualenv_root is not None
            else _DEFAULT_FLYTEKIT_VIRTUALENV_ROOT
        )

        # append python3
        ctx.obj[CTX_PYTHON_INTERPRETER] = ctx.obj[CTX_FLYTEKIT_VIRTUALENV_ROOT] + "/bin/python3"
    else:
        # For in container serialize we make sure to never accept an override the entrypoint path and determine it here
        # instead.
        entrypoint_path = _os.path.abspath(_flytekit.__file__)
        if entrypoint_path.endswith(".pyc"):
            entrypoint_path = entrypoint_path[:-1]

        ctx.obj[CTX_FLYTEKIT_VIRTUALENV_ROOT] = _os.path.dirname(entrypoint_path)
        ctx.obj[CTX_PYTHON_INTERPRETER] = sys.executable


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

    source_dir = ctx.obj[CTX_LOCAL_SRC_ROOT]
    digest = _compute_digest(source_dir)
    folder = folder if folder else ""
    archive_fname = _os.path.join(folder, f"{digest}.tar.gz")
    click.echo(f"Writing compressed archive to {archive_fname}")
    # Write using gzip
    with _tarfile.open(archive_fname, "w:gz") as tar:
        tar.add(source_dir, arcname="", filter=_filter_tar_file_fn)

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


fast.add_command(fast_workflows)

serialize.add_command(tasks)
serialize.add_command(workflows)
serialize.add_command(fast)
