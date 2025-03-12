import os
import sys
import typing

import rich_click as click

from flytekit.clis.helpers import display_help_with_error
from flytekit.clis.sdk_in_container import constants
from flytekit.clis.sdk_in_container.helpers import parse_copy
from flytekit.configuration import (
    DEFAULT_RUNTIME_PYTHON_INTERPRETER,
    FastSerializationSettings,
    ImageConfig,
    SerializationSettings,
)
from flytekit.constants import CopyFileDetection
from flytekit.interaction.click_types import key_value_callback
from flytekit.tools.fast_registration import FastPackageOptions
from flytekit.tools.repo import NoSerializableEntitiesError, serialize_and_package


@click.command("package")
@click.option(
    "-i",
    "--image",
    "image_config",
    required=False,
    multiple=True,
    type=click.UNPROCESSED,
    callback=ImageConfig.validate_image,
    help="A fully qualified tag for an docker image, for example ``somedocker.com/myimage:someversion123``. This is a "
    "multi-option and can be of the form ``--image xyz.io/docker:latest"
    " --image my_image=xyz.io/docker2:latest``. Note, the ``name=image_uri``. The name is optional, if not "
    "provided the image will be used as the default image. All the names have to be unique, and thus "
    "there can only be one ``--image`` option with no name.",
)
@click.option(
    "-s",
    "--source",
    required=False,
    type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True, allow_dash=True),
    default=".",
    help="Local filesystem path to the root of the package.",
)
@click.option(
    "-o",
    "--output",
    required=False,
    type=click.Path(dir_okay=False, writable=True, resolve_path=True, allow_dash=True),
    default="flyte-package.tgz",
    help="Filesystem path to the source of the Python package (from where the pkgs will start).",
)
@click.option(
    "--fast",
    is_flag=True,
    default=False,
    required=False,
    help="[Deprecated, see --copy] This flag enables fast packaging, that allows `no container build`"
    " deploys of flyte workflows and tasks. You should specify --copy all/auto instead"
    " Note this needs additional configuration, refer to the docs.",
)
@click.option(
    "--copy",
    required=False,
    type=click.Choice(["all", "auto", "none"], case_sensitive=False),
    default="none",
    show_default=True,
    callback=parse_copy,
    help="Specify whether local files should be copied and uploaded so task containers have up-to-date code"
    " 'all' will behave as the current 'fast' flag, copying all files, 'auto' copies only loaded Python modules",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    default=False,
    required=False,
    help="This flag enables overriding existing output files. If not specified, package will exit with an error,"
    " when an output file already exists.",
)
@click.option(
    "-p",
    "--python-interpreter",
    default=DEFAULT_RUNTIME_PYTHON_INTERPRETER,
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
    help="Filesystem path to where the code is copied into within the Dockerfile. look for ``COPY . /root`` like command.",
)
@click.option(
    "--deref-symlinks",
    default=False,
    is_flag=True,
    help="Enables symlink dereferencing when packaging files in fast registration",
)
@click.option(
    "--env",
    "--envvars",
    required=False,
    multiple=True,
    type=str,
    callback=key_value_callback,
    help="Environment variables to set in the container, of the format `ENV_NAME=ENV_VALUE`",
)
@click.pass_context
def package(
    ctx,
    image_config,
    source,
    output,
    force,
    copy: typing.Optional[CopyFileDetection],
    fast,
    in_container_source_path,
    python_interpreter,
    deref_symlinks,
    env,
):
    """
    This command produces a Flyte backend registrable package of all entities in Flyte.
    For tasks, one pb file is produced for each task, representing one TaskTemplate object.
    For workflows, one pb file is produced for each workflow, representing a WorkflowClosure object. The closure
    object contains the WorkflowTemplate, along with the relevant tasks for that workflow.
    This serialization step will set the name of the tasks to the fully qualified name of the task function.
    """
    # Ensure that the two flags are consistent
    if fast:
        if "--copy" in sys.argv:
            raise click.BadParameter(
                click.style(
                    "Cannot use both --fast and --copy flags together. Please move to --copy",
                    fg="red",
                )
            )
        click.secho("The --fast flag is deprecated, please use --copy all instead", fg="yellow")
        copy = CopyFileDetection.ALL

    if os.path.exists(output) and not force:
        raise click.BadParameter(
            click.style(
                f"Output file {output} already exists, specify -f to override.",
                fg="red",
            )
        )

    serialization_settings = SerializationSettings(
        image_config=image_config,
        fast_serialization_settings=FastSerializationSettings(
            enabled=copy != CopyFileDetection.NO_COPY,
            destination_dir=in_container_source_path,
        ),
        python_interpreter=python_interpreter,
        env=env,
    )

    pkgs = ctx.obj[constants.CTX_PACKAGES]
    if not pkgs:
        display_help_with_error(ctx, "No packages to scan for flyte entities. Aborting!")

    try:
        # verbosity greater than 0 means to print the files
        show_files = ctx.obj[constants.CTX_VERBOSE] > 0

        fast_options = FastPackageOptions([], copy_style=copy, show_files=show_files)
        serialize_and_package(pkgs, serialization_settings, source, output, deref_symlinks, fast_options=fast_options)
    except NoSerializableEntitiesError:
        click.secho(f"No flyte objects found in packages {pkgs}", fg="yellow")
