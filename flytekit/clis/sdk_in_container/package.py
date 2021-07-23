import os
import sys
import tarfile
import tempfile
import typing

import click

from flytekit.clis.sdk_in_container import constants, serialize
from flytekit.configuration import internal
from flytekit.core import context_manager
from flytekit.core.context_manager import ImageConfig, look_up_image_info
from flytekit.tools import fast_registration, module_loader

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
            splits = v.split("=", maxsplit=1)
            img = look_up_image_info(name=splits[0], tag=splits[1], optional_tag=False)
        else:
            img = look_up_image_info(_DEFAULT_IMAGE_NAME, v, False)

        if default_image and img.name == _DEFAULT_IMAGE_NAME:
            raise click.BadParameter(
                f"Only one default image can be specified. Received multiple {default_image} & {img} for {param}"
            )
        if img.name == _DEFAULT_IMAGE_NAME:
            default_image = img
        else:
            images.append(img)

    return ImageConfig(default_image, images)


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
    help="local filesystem path to the root of the package.",
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
@click.option(
    "-d",
    "--in-container-source-path",
    required=False,
    type=str,
    default="/root",
    help="Filesystem path to where the code is copied into within the Dockerfile. look for `COPY . /root` like command.",
)
@click.pass_context
def package(ctx, image_config, source, output, force, fast, in_container_source_path, python_interpreter):
    """
    This command produces a Flyte backend registrable package of all entities in Flyte.
    For tasks, one pb file is produced for each task, representing one TaskTemplate object.
    For workflows, one pb file is produced for each workflow, representing a WorkflowClosure object.  The closure
        object contains the WorkflowTemplate, along with the relevant tasks for that workflow.
        This serialization step will set the name of the tasks to the fully qualified name of the task function.
    """
    if os.path.exists(output) and not force:
        raise click.BadParameter(click.style(f"Output file {output} already exists, specify -f to override.", fg="red"))

    env_binary_path = os.path.dirname(python_interpreter)
    venv_root = os.path.dirname(env_binary_path)
    serialization_settings = context_manager.SerializationSettings(
        project=serialize._PROJECT_PLACEHOLDER,
        domain=serialize._DOMAIN_PLACEHOLDER,
        version=serialize._VERSION_PLACEHOLDER,
        fast_serialization_settings=context_manager.FastSerializationSettings(
            enabled=fast,
            destination_dir=in_container_source_path,
        ),
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
        # Scan all modules. the act of loading populates the global singleton that contains all objects
        with module_loader.add_sys_path(source):
            click.secho(f"Loading packages {pkgs} under source root {source}", fg="yellow")
            module_loader.just_load_modules(pkgs=pkgs)

        registrable_entities = serialize.get_registrable_entities(ctx)

        if registrable_entities:
            with tempfile.TemporaryDirectory() as output_tmpdir:
                serialize.persist_registrable_entities(registrable_entities, output_tmpdir)

                # If Fast serialization is enabled, then an archive is also created and packaged
                if fast:
                    digest = fast_registration.compute_digest(source)
                    archive_fname = os.path.join(output_tmpdir, f"{digest}.tar.gz")
                    click.secho(f"Fast mode enabled: compressed archive {archive_fname}", dim=True)
                    # Write using gzip
                    with tarfile.open(archive_fname, "w:gz") as tar:
                        tar.add(source, arcname="", filter=fast_registration.filter_tar_file_fn)

                with tarfile.open(output, "w:gz") as tar:
                    tar.add(output_tmpdir, arcname="")

            click.secho(f"Successfully packaged {len(registrable_entities)} flyte objects into {output}", fg="green")
        else:
            click.secho(f"No flyte objects found in packages {pkgs}", fg="yellow")
