import os
import sys
import tarfile
import tempfile
import typing
from collections import OrderedDict

import click
from flyteidl.admin import launch_plan_pb2, task_pb2, workflow_pb2

from flytekit import LaunchPlan
from flytekit.clis.sdk_in_container import constants, serialize
from flytekit.clis.sdk_in_container.serialize import SerializationMode
from flytekit.common import translator
from flytekit.configuration import internal
from flytekit.core import context_manager
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import Image, ImageConfig, look_up_image_info
from flytekit.core.workflow import WorkflowBase
from flytekit.tools import module_loader

_DEFAULT_IMAGE_NAME = "default"
_DEFAULT_RUNTIME_PYTHON_INTERPRETER = "/opt/venv/bin/python3"


def validate_image(ctx: typing.Any, param: str, values: tuple) -> ImageConfig:
    """
    Validates the image to match the standard format. Also validates that only one default image
    is provided. a default image, is one that is specified as
      default=img or just img. All other images should be provided with a name, in the format
      name=img
    """
    default_image = None
    images = []
    for v in values:
        if "=" in v:
            vals = v.split("=")
            if len(vals) > 2:
                raise click.BadParameter(f"Bad value received [{v}] for param {param}, more than 1 `=` is not allowed")
            name = vals[0]
            tag = vals[1]
            img = look_up_image_info(name, tag, False)
        else:
            img = look_up_image_info(_DEFAULT_IMAGE_NAME, v, False)

        if default_image and img.name == _DEFAULT_IMAGE_NAME:
            raise click.BadParameter(
                f"Only one default image can be specified. Received multiple {default_image}&{img}"
            )
        if img.name == _DEFAULT_IMAGE_NAME:
            default_image = img
        else:
            images.append(img)

    return ImageConfig(default_image, images)


def get_registrable_entities(ctx: context_manager.FlyteContext, mode: SerializationMode) -> typing.List:
    """
    Returns all entities that can be serialized and should be sent over to Flyte backend. This will filter any local entities
    """
    new_api_serializable_entities = OrderedDict()
    # TODO: Clean up the copy() - it's here because we call get_default_launch_plan, which may create a LaunchPlan
    #  object, which gets added to the FlyteEntities.entities list, which we're iterating over.
    for entity in context_manager.FlyteEntities.entities.copy():
        if isinstance(entity, PythonTask) or isinstance(entity, WorkflowBase) or isinstance(entity, LaunchPlan):
            if isinstance(entity, PythonTask):
                if mode == SerializationMode.DEFAULT:
                    translator.get_serializable(new_api_serializable_entities, ctx.serialization_settings, entity)
                elif mode == SerializationMode.FAST:
                    translator.get_serializable(
                        new_api_serializable_entities, ctx.serialization_settings, entity, fast=True
                    )
                else:
                    raise AssertionError(f"Unrecognized serialization mode: {mode}")
            else:
                translator.get_serializable(new_api_serializable_entities, ctx.serialization_settings, entity)

            if isinstance(entity, WorkflowBase):
                lp = LaunchPlan.get_default_launch_plan(ctx, entity)
                translator.get_serializable(new_api_serializable_entities, ctx.serialization_settings, lp)

    new_api_model_values = list(new_api_serializable_entities.values())
    entities_to_be_serialized = list(filter(serialize._should_register_with_admin, new_api_model_values))
    return [v.to_flyte_idl() for v in entities_to_be_serialized]


def persist_registrable_entities(entities: typing.List, folder: str):
    """
    For protobuf serializable list of entities, writes a file with the name if the entity and
    enumeration order to the specified folder
    """
    zero_padded_length = serialize._determine_text_chars(len(entities))
    for i, entity in enumerate(entities):
        name = ""
        fname_index = str(i).zfill(zero_padded_length)
        if isinstance(entity, task_pb2.TaskSpec):
            name = entity.template.id.name
            fname = "{}_{}_1.pb".format(fname_index, entity.template.id.name)
        elif isinstance(entity, workflow_pb2.WorkflowSpec):
            name = entity.template.id.name
            fname = "{}_{}_2.pb".format(fname_index, entity.template.id.name)
        elif isinstance(entity, launch_plan_pb2.LaunchPlan):
            name = entity.id.name
            fname = "{}_{}_3.pb".format(fname_index, entity.id.name)
        else:
            click.secho(f"Entity is incorrect formatted {entity} - type {type(entity)}", fg="red")
            sys.exit(-1)
        click.secho(f"  Packaging {name} -> {fname}", dim=True)
        fname = os.path.join(folder, fname)
        with open(fname, "wb") as writer:
            writer.write(entity.SerializeToString())


@click.command("package")
@click.option(
    "-i",
    "--image",
    "image_config",
    required=False,
    multiple=True,
    type=click.UNPROCESSED,
    callback=validate_image,
    help="A fully qualified tag for an docker image, e.g. somedocker.com/myimage:someversion123. This is a "
    "multi-option and can be of the form --image xyz.io/docker:latest"
    " --image my_image=xyz.io/docker2:latest. Note, the `name=image_uri`. The name is optional, if not"
    "provided the image will be used as the default image. All the names have to be unique, and thus"
    "there can only be one --image option with no-name.",
)
@click.option(
    "-s",
    "--source",
    required=False,
    type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True, allow_dash=True),
    default=".",
    help="filesystem path and name of the package.",
)
@click.option(
    "-o",
    "--output",
    required=False,
    type=click.Path(dir_okay=False, writable=True, resolve_path=True, allow_dash=True),
    default="flyte-package.tgz",
    help="filesystem path to the source of the python package (from where the pkgs will start).",
)
@click.option(
    "--fast",
    is_flag=True,
    default=False,
    required=False,
    help="This flag enables fast packaging, that allows `no container build` deploys of flyte workflows and tasks."
    "Note this needs additional configuration, refer to the docs.",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    required=False,
    help="This flag enables overriding existing output files. If not specified, package will exit with an error,"
    " in case an output file already exists.",
)
@click.option(
    "-p",
    "--python-interpreter",
    default=_DEFAULT_RUNTIME_PYTHON_INTERPRETER,
    required=False,
    help="Use this to override the default location of the in-container python interpreter that will be used by "
    "Flyte to load your program. This is usually where you install flytekit within the container.",
)
@click.pass_context
def package(ctx, image_config, source, output, force, fast, python_interpreter):
    """
    This command produces a Flyte backend registrable package of all entities in Flyte.
    For tasks, one pb file is produced for each task, representing one TaskTemplate object.
    For workflows, one pb file is produced for each workflow, representing a WorkflowClosure object.  The closure
        object contains the WorkflowTemplate, along with the relevant tasks for that workflow.
        This serialization step will set the URN of the tasks to the fully qualified name of the task function.
    """
    if os.path.exists(output) and not force:
        raise click.BadParameter(click.style(f"Output file {output} already exists, specify -f to override.", fg="red"))

    env_binary_path = os.path.dirname(python_interpreter)
    venv_root = os.path.dirname(env_binary_path)
    serialization_settings = context_manager.SerializationSettings(
        project=serialize._PROJECT_PLACEHOLDER,
        domain=serialize._DOMAIN_PLACEHOLDER,
        version=serialize._VERSION_PLACEHOLDER,
        image_config=image_config,
        env={internal.IMAGE.env_var: image_config.default_image.full},  # TODO this env variable should be deprecated
        flytekit_virtualenv_root=venv_root,
        python_interpreter=python_interpreter,
        entrypoint_settings=context_manager.EntrypointSettings(
            path=os.path.join(venv_root, serialize._DEFAULT_FLYTEKIT_RELATIVE_ENTRYPOINT_LOC)
        ),
    )

    pkgs = ctx.obj[constants.CTX_PACKAGES]
    if not pkgs:
        click.secho("No packages to scan for flyte entities. Aborting!", fg="red")
        sys.exit(-1)

    ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(serialization_settings)
    with context_manager.FlyteContextManager.with_context(ctx) as ctx:
        # Decide serialization mode. Fast mode allows, patching existing containers
        mode = SerializationMode.FAST if fast else SerializationMode.DEFAULT

        # Scan all modules. the act of loading populates the global singleton that contains all objects
        with module_loader.add_sys_path(source):
            click.secho(f"Loading packages {pkgs} under source root {source}", fg="yellow")
            module_loader.just_load_modules(pkgs=pkgs)

        registrable_entities = get_registrable_entities(ctx, mode)

        if registrable_entities:
            with tempfile.TemporaryDirectory() as output_tmpdir:
                persist_registrable_entities(registrable_entities, output_tmpdir)

                with tarfile.open(output, "w:gz") as tar:
                    tar.add(output_tmpdir, arcname=".")

            click.secho(f"Successfully packaged {len(registrable_entities)} flyte objects into {output}", fg="green")
        else:
            click.secho(f"No flyte objects found in packages {pkgs}", fg="yellow")
