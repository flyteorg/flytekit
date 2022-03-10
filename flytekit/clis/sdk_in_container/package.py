import os
import sys
import tarfile
import tempfile

import click

from flytekit.clis.sdk_in_container import constants
from flytekit.configuration import (
    DEFAULT_RUNTIME_PYTHON_INTERPRETER,
    FastSerializationSettings,
    ImageConfig,
    SerializationSettings,
)
from flytekit.core import context_manager
from flytekit.tools import fast_registration, module_loader, serialize_helpers


@click.command("package")
@click.option(
    "-i",
    "--image",
    "image_config",
    required=False,
    multiple=True,
    type=click.UNPROCESSED,
    callback=ImageConfig.validate_image,
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

    serialization_settings = SerializationSettings(
        image_config=image_config,
        fast_serialization_settings=FastSerializationSettings(
            enabled=fast,
            destination_dir=in_container_source_path,
        ),
        python_interpreter=python_interpreter,
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

        registrable_entities = serialize_helpers.get_registrable_entities(ctx)

        if registrable_entities:
            with tempfile.TemporaryDirectory() as output_tmpdir:
                serialize_helpers.persist_registrable_entities(registrable_entities, output_tmpdir)

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
