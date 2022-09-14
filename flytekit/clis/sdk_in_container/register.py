import os
import pathlib
import typing

import click

from flytekit.clis.helpers import display_help_with_error
from flytekit.clis.sdk_in_container import constants
from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context
from flytekit.configuration import FastSerializationSettings, ImageConfig, SerializationSettings
from flytekit.configuration.default_images import DefaultImages
from flytekit.loggers import cli_logger
from flytekit.tools.fast_registration import fast_package
from flytekit.tools.repo import find_common_root, load_packages_and_modules
from flytekit.tools.repo import register as repo_register
from flytekit.tools.translator import Options

_register_help = """
This command is similar to package but instead of producing a zip file, all your Flyte entities are compiled,
and then sent to the backend specified by your config file. Think of this as combining the pyflyte package
and the flytectl register step in one command. This is why you see switches you'd normally use with flytectl
like service account here.

Note: This command runs "fast" register by default. Future work to come to add a non-fast version.
This means that a zip is created from the detected root of the packages given, and uploaded. Just like with
pyflyte run, tasks registered from this command will download and unzip that code package before running.

Note: This command only works on regular Python packages, not namespace packages. When determining
      the root of your project, it finds the first folder that does not have an __init__.py file.
"""


@click.command("register", help=_register_help)
@click.option(
    "-p",
    "--project",
    required=False,
    type=str,
    default="flytesnacks",
    help="Project to register and run this workflow in",
)
@click.option(
    "-d",
    "--domain",
    required=False,
    type=str,
    default="development",
    help="Domain to register and run this workflow in",
)
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
    "-o",
    "--output",
    required=False,
    type=click.Path(dir_okay=True, file_okay=False, writable=True, resolve_path=True),
    default=None,
    help="Directory to write the output zip file containing the protobuf definitions",
)
@click.option(
    "-d",
    "--destination-dir",
    required=False,
    type=str,
    default="/root",
    help="Directory inside the image where the tar file containing the code will be copied to",
)
@click.option(
    "--service-account",
    required=False,
    type=str,
    default="",
    help="Service account used when creating launch plans",
)
@click.option(
    "--raw-data-prefix",
    required=False,
    type=str,
    default="",
    help="Raw output data prefix when creating launch plans, where offloaded data will be stored",
)
@click.option(
    "-v",
    "--version",
    required=False,
    type=str,
    help="Version the package or module is registered with",
)
@click.option(
    "--deref-symlinks",
    default=False,
    is_flag=True,
    help="Enables symlink dereferencing when packaging files in fast registration",
)
@click.argument("package-or-module", type=click.Path(exists=True, readable=True, resolve_path=True), nargs=-1)
@click.pass_context
def register(
    ctx: click.Context,
    project: str,
    domain: str,
    image_config: ImageConfig,
    output: str,
    destination_dir: str,
    service_account: str,
    raw_data_prefix: str,
    version: typing.Optional[str],
    deref_symlinks: bool,
    package_or_module: typing.Tuple[str],
):
    """
    see help
    """
    pkgs = ctx.obj[constants.CTX_PACKAGES]
    if not pkgs:
        cli_logger.debug("No pkgs")
    if pkgs:
        raise ValueError("Unimplemented, just specify pkgs like folder/files as args at the end of the command")

    if len(package_or_module) == 0:
        display_help_with_error(
            ctx,
            "Missing argument 'PACKAGE_OR_MODULE...', at least one PACKAGE_OR_MODULE is required but multiple can be passed",
        )

    cli_logger.debug(
        f"Running pyflyte register from {os.getcwd()} "
        f"with images {image_config} "
        f"and image destinationfolder {destination_dir} "
        f"on {len(package_or_module)} package(s) {package_or_module}"
    )

    # Create and save FlyteRemote,
    remote = get_and_save_remote_with_click_context(ctx, project, domain)

    # Todo: add switch for non-fast - skip the zipping and uploading and no fastserializationsettings
    # Create a zip file containing all the entries.
    detected_root = find_common_root(package_or_module)
    cli_logger.debug(f"Using {detected_root} as root folder for project")
    zip_file = fast_package(detected_root, output, deref_symlinks)

    # Upload zip file to Admin using FlyteRemote.
    md5_bytes, native_url = remote._upload_file(pathlib.Path(zip_file))
    cli_logger.debug(f"Uploaded zip {zip_file} to {native_url}")

    # Create serialization settings
    # Todo: Rely on default Python interpreter for now, this will break custom Spark containers
    serialization_settings = SerializationSettings(
        project=project,
        domain=domain,
        image_config=image_config,
        fast_serialization_settings=FastSerializationSettings(
            enabled=True,
            destination_dir=destination_dir,
            distribution_location=native_url,
        ),
    )

    options = Options.default_from(k8s_service_account=service_account, raw_data_prefix=raw_data_prefix)

    # Load all the entities
    registerable_entities = load_packages_and_modules(
        serialization_settings, detected_root, list(package_or_module), options
    )
    if len(registerable_entities) == 0:
        display_help_with_error(ctx, "No Flyte entities were detected. Aborting!")
    cli_logger.info(f"Found and serialized {len(registerable_entities)} entities")

    if not version:
        version = remote._version_from_hash(md5_bytes, serialization_settings, service_account, raw_data_prefix)  # noqa
        cli_logger.info(f"Computed version is {version}")

    # Register using repo code
    repo_register(registerable_entities, project, domain, version, remote.client)
