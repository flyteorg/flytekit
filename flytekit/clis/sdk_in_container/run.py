import importlib
import os
import tarfile
import tempfile
from pathlib import Path

import click
from flyteidl.core import identifier_pb2

from flytekit import configuration
from flytekit.clients import friendly
from flytekit.clis.flyte_cli.main import _extract_and_register
from flytekit.clis.sdk_in_container.utils import produce_fast_register_task_closure
from flytekit.configuration import FastSerializationSettings, ImageConfig, SerializationSettings
from flytekit.core import context_manager
from flytekit.exceptions.user import FlyteValidationException
from flytekit.tools import fast_registration, module_loader, serialize_helpers


@click.command("run")
@click.argument(
    "module_and_workflow",
)
@click.option(
    "image",
    "-i",
    "--image",
    required=False,
    type=str,
    # TODO: fix default images push gh workflow
    default="ghcr.io/flyteorg/flytekit:py39-latest",
    # default="pyflyte-fast-execute:latest",
    help="Image used to register and run.",
)
@click.option(
    "help",
    "-h",
    "--help",
    required=False,
    is_flag=True,
    help="Shows inputs to workflow and potentially the workflow docstring",
)
@click.pass_context
def run(
    click_ctx,
    module_and_workflow,
    image,
    help=None,
):
    """
    TODO description (remember to document workflow inputs)
    """
    # TODO is it `--help`? Figure out a way to parse the inputs to the workflow

    split_input = module_and_workflow.split(":")
    if len(split_input) != 2:
        raise FlyteValidationException(f"Input {module_and_workflow} must be in format '<module>:<worfklow>'")

    module, workflow_name = split_input
    pkg, source = _find_full_package_name_and_project_root()
    destination_dir = "/root"

    image_config = ImageConfig.validate_single_image(image)
    serialization_settings = SerializationSettings(
        image_config=image_config,
        fast_serialization_settings=FastSerializationSettings(
            enabled=True,
            destination_dir=destination_dir,
        ),
    )
    flyte_ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(
        serialization_settings
    )
    with context_manager.FlyteContextManager.with_context(flyte_ctx) as flyte_ctx:
        with module_loader.add_sys_path(source):
            importlib.import_module(f"{pkg}.{module}")

    registrable_entities = serialize_helpers.get_registrable_entities(flyte_ctx)

    if not registrable_entities:
        click.secho(f"No flyte objects found in package {pkg}", fg="yellow")
        return

    # Open a temp directory and dump the contents of the digest.
    tmp_dir = tempfile.TemporaryDirectory()
    source_path = os.fspath(source)
    digest = fast_registration.compute_digest(source_path)
    archive_fname = os.path.join(tmp_dir.name, f"{digest}.tar.gz")
    with tarfile.open(archive_fname, "w:gz") as tar:
        tar.add(source, arcname="", filter=fast_registration.filter_tar_file_fn)
    serialize_helpers.persist_registrable_entities(registrable_entities, tmp_dir.name)

    pb_files = []
    for f in os.listdir(tmp_dir.name):
        if f.endswith(".pb"):
            pb_files.append(os.path.join(tmp_dir.name, f))

    version = str(os.getpid())  # TODO: find a better source of entropy

    # TODO Get signed url from flyteadmin
    full_remote_path = f"s3://my-s3-bucket/fast-register-test/{digest}.tar.gz"
    flyte_ctx = context_manager.FlyteContextManager.current_context()
    flyte_ctx.file_access.put_data(archive_fname, full_remote_path)

    patches = {
        identifier_pb2.TASK: produce_fast_register_task_closure(full_remote_path, destination_dir),
        # TODO: handle launch plans
        # _identifier_pb2.LAUNCH_PLAN: _get_patch_launch_plan_fn(
        #     assumable_iam_role, kubernetes_service_account, output_location_prefix
        # ),
    }

    # TODO: get this from the config file
    config_obj = configuration.PlatformConfig.auto()
    client = friendly.SynchronousFlyteClient(config_obj)
    _extract_and_register(client, "flytesnacks", "development", version, pb_files, patches)

    # TODO: Schedule execution using flyteremote
    # TODO: Dump flyteremote snippet to access the run


def _find_full_package_name_and_project_root() -> (str, Path):
    """
    Traverse from current working directory until it can no longer find __init__.py files
    """
    # N.B.: this path traversal is *extremely* naive. In other words it will not handle cycles
    # and probably will be
    dirs = []
    path = Path(os.getcwd())
    while os.path.exists(os.path.join(path, "__init__.py")):
        dirs.insert(0, path.parts[-1])
        path = path.parent

    full_pkg = ".".join(dirs)
    source = path

    return full_pkg, source
