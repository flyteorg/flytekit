import os
import sys
import typing

import rich_click as click

from flytekit.clis.helpers import display_help_with_error
from flytekit.clis.sdk_in_container import constants
from flytekit.clis.sdk_in_container.helpers import (
    get_and_save_remote_with_click_context,
    parse_copy,
    patch_image_config,
)
from flytekit.clis.sdk_in_container.utils import domain_option_dec, project_option_dec
from flytekit.configuration import ImageConfig
from flytekit.configuration.default_images import DefaultImages
from flytekit.constants import CopyFileDetection
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.interaction.click_types import key_value_callback, resource_callback
from flytekit.loggers import logger
from flytekit.tools import repo

_register_help = """
This command is similar to ``package`` but instead of producing a zip file, all your Flyte entities are compiled,
and then sent to the backend specified by your config file. Think of this as combining the ``pyflyte package``
and the ``flytectl register`` steps in one command. This is why you see switches you'd normally use with flytectl
like service account here.

Note: This command runs "fast" register by default.
This means that a zip is created from the detected root of the packages given and uploaded. Just like with
``pyflyte run``, tasks registered from this command will download and unzip that code package before running.

Note: This command only works on regular Python packages, not namespace packages. When determining
the root of your project, it finds the first folder that does not have a ``__init__.py`` file.
"""


@click.command("register", help=_register_help)
@project_option_dec
@domain_option_dec
@click.option(
    "-i",
    "--image",
    "image_config",
    required=False,
    multiple=True,
    type=click.UNPROCESSED,
    callback=ImageConfig.validate_image,
    default=[DefaultImages.default_image()],
    help="A fully qualified tag for an docker image, for example ``somedocker.com/myimage:someversion123``. This is a "
    "multi-option and can be of the form ``--image xyz.io/docker:latest"
    " --image my_image=xyz.io/docker2:latest``. Note, the ``name=image_uri``. The name is optional, if not "
    "provided the image will be used as the default image. All the names have to be unique, and thus "
    "there can only be one ``--image`` option with no name.",
)
@click.option(
    "-o",
    "--output",
    required=False,
    type=click.Path(dir_okay=True, file_okay=False, writable=True, resolve_path=True),
    default=None,
    help="Directory to write the output tar file containing the protobuf definitions",
)
@click.option(
    "-D",
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
@click.option(
    "--non-fast",
    default=False,
    is_flag=True,
    help="[Deprecated, see --copy] Skip zipping and uploading the package. You should specify --copy none instead",
)
@click.option(
    "--copy",
    required=False,
    type=click.Choice(["all", "auto", "none"], case_sensitive=False),
    default="all",
    show_default=True,
    callback=parse_copy,
    help="Specify how and whether to use fast register"
    " 'all' is the current behavior copying all files from root, 'auto' copies only loaded Python modules"
    " 'none' means no files are copied, i.e. don't use fast register",
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    help="Execute registration in dry-run mode. Skips actual registration to remote",
)
@click.option(
    "--activate-launchplans",
    "--activate-launchplan",
    default=False,
    is_flag=True,
    help="Activate newly registered Launchplans. This operation deactivates previous versions of Launchplans.",
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
@click.option(
    "--resource-requests",
    required=False,
    type=str,
    callback=resource_callback,
    help="Override default task resource requests for tasks that have no statically defined resource requests in their task decorator. "
    "Example usage: --resource-requests 'cpu=1,mem=2Gi,gpu=1'",
)
@click.option(
    "--resource-limits",
    required=False,
    type=str,
    callback=resource_callback,
    help="Override default task resource limits for tasks that have no statically defined resource limits in their task decorator. "
    "Example usage: --resource-limits 'cpu=1,mem=2Gi,gpu=1'",
)
@click.option(
    "--skip-errors",
    "--skip-error",
    default=False,
    is_flag=True,
    help="Skip errors during registration. This is useful when registering multiple packages and you want to skip "
    "errors for some packages.",
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
    non_fast: bool,
    copy: typing.Optional[CopyFileDetection],
    package_or_module: typing.Tuple[str],
    dry_run: bool,
    activate_launchplans: bool,
    env: typing.Optional[typing.Dict[str, str]],
    resource_requests: typing.Optional[Resources],
    resource_limits: typing.Optional[Resources],
    skip_errors: bool,
):
    """
    see help
    """
    # Set the relevant copy option if non_fast is set, this enables the individual file listing behavior
    # that the copy flag uses.
    if non_fast:
        click.secho("The --non-fast flag is deprecated, please use --copy none instead", fg="yellow")
        if "--copy" in sys.argv:
            raise click.BadParameter(
                click.style(
                    "Cannot use both --non-fast and --copy flags together. Please move to --copy.",
                    fg="red",
                )
            )
        copy = CopyFileDetection.NO_COPY
    if copy == CopyFileDetection.NO_COPY and not version:
        raise ValueError("Version is a required parameter in case --copy none is specified.")

    show_files = ctx.obj[constants.CTX_VERBOSE] > 0

    pkgs = ctx.obj[constants.CTX_PACKAGES]
    if not pkgs:
        logger.debug("No pkgs")
    if pkgs:
        raise ValueError("Unimplemented, just specify pkgs like folder/files as args at the end of the command")

    if len(package_or_module) == 0:
        display_help_with_error(
            ctx,
            "Missing argument 'PACKAGE_OR_MODULE...', at least one PACKAGE_OR_MODULE is required but multiple can be passed",
        )

    # Use extra images in the config file if that file exists
    config_file = ctx.obj.get(constants.CTX_CONFIG_FILE)
    if config_file:
        image_config = patch_image_config(config_file, image_config)

    click.secho(
        f"Running pyflyte register from {os.getcwd()} "
        f"with images {image_config} "
        f"and image destination folder {destination_dir} "
        f"on {len(package_or_module)} package(s) {package_or_module}",
        dim=True,
    )

    # Create and save FlyteRemote,
    remote = get_and_save_remote_with_click_context(ctx, project, domain, data_upload_location="flyte://data")
    click.secho(f"Registering against {remote.config.platform.endpoint}")
    repo.register(
        project,
        domain,
        image_config,
        output,
        destination_dir,
        service_account,
        raw_data_prefix,
        version,
        deref_symlinks,
        copy_style=copy,
        package_or_module=package_or_module,
        remote=remote,
        env=env,
        default_resources=ResourceSpec(
            requests=resource_requests or Resources(), limits=resource_limits or Resources()
        ),
        dry_run=dry_run,
        activate_launchplans=activate_launchplans,
        skip_errors=skip_errors,
        show_files=show_files,
        verbosity=ctx.obj[constants.CTX_VERBOSE],
    )
