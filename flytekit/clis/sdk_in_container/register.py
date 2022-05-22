import os

import click
from flytekit.clis.sdk_in_container import constants
from flytekit.configuration import (
    FastSerializationSettings,
    ImageConfig,
    SerializationSettings,
)
from flytekit.configuration.default_images import DefaultImages
from flytekit.tools.module_loader import load_packages_and_modules


@click.command("register")
@click.option(
    "-i",
    "--image",
    "image_config",
    required=False,
    multiple=True,
    type=click.UNPROCESSED,
    callback=ImageConfig.validate_image,
    default=[DefaultImages.default_image()],
    help="A fully qualified tag for an docker image, e.g. somedocker.com/myimage:someversion123. This is a "
    "multi-option and can be of the form --image xyz.io/docker:latest "
    "--image my_image=xyz.io/docker2:latest. Note, the `name=image_uri`. The name is optional, if not "
    "provided the image will be used as the default image. All the names have to be unique, and thus "
    "there can only be one --image option with no name.",
)
@click.option(
    "-s",
    "--source",
    required=False,
    type=click.Path(exists=True, file_okay=False, readable=True, resolve_path=True, allow_dash=True),
    default=".",
    help="Optional directory to add to Python package search path, defaults to current directory",
)
@click.option(
    "-o",
    "--output",
    required=False,
    type=click.Path(dir_okay=False, writable=True, resolve_path=True, allow_dash=True),
    default="flyte-package.tgz",
    help="Where to write the output zip file containing the protobuf definitions",
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
    "-d",
    "--destination-dir",
    required=False,
    type=str,
    default="/root",
    help="Directory inside the image where the tar file containing the code will be copied to",
)
@click.argument("package-or-module", type=click.Path(exists=True, readable=True, resolve_path=True), nargs=-1)
# todo: add all other runtime options since this also does registration
@click.pass_context
def register(ctx, image_config, source, output, force, destination_dir, package_or_module):
    """
    This command produces a Flyte backend registrable package of all entities in Flyte.
    For tasks, one pb file is produced for each task, representing one TaskTemplate object.
    For workflows, one pb file is produced for each workflow, representing a WorkflowClosure object.  The closure
        object contains the WorkflowTemplate, along with the relevant tasks for that workflow.
        This serialization step will set the name of the tasks to the fully qualified name of the task function.
    """
    if os.path.exists(output) and not force:
        raise click.BadParameter(click.style(f"Output file {output} already exists, specify -f to override.", fg="red"))

    # Rely on default Python interpreter for now
    serialization_settings = SerializationSettings(
        image_config=image_config,
        fast_serialization_settings=FastSerializationSettings(
            enabled=True,
            destination_dir=destination_dir,
        ),
    )

    pkgs = ctx.obj[constants.CTX_PACKAGES]
    if not pkgs:
        print("No pkgs")

    print(f"Image config {image_config}")
    print(f"Source {source}")
    print(f"Destination dir {destination_dir}")
    print(f"Package arg {package_or_module} {len(package_or_module)}")
    print(__package__)
    print(os.getcwd())

    # import pkgutil
    # pkgutil.walk_packages()

    """
    """
    # Load all the entities
    load_packages_and_modules(list(package_or_module))

    # Todo: Filter out entities not in the given list (in case it imports some other crazy thing)

    # Create a zip file containing all the entries.

    #
