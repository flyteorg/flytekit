import importlib
import os

import click
import pandas as pd
from flyteidl.service.dataproxy_pb2 import CreateUploadLocationResponse

from flytekit.clients import friendly
from flytekit.configuration import Config, FastSerializationSettings, ImageConfig, PlatformConfig, SerializationSettings
from flytekit.core import context_manager
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import WorkflowBase
from flytekit.exceptions.user import FlyteValidationException
from flytekit.models.literals import StructuredDataset
from flytekit.remote.remote import FlyteRemote
from flytekit.tools import module_loader, script_mode


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument(
    "file_and_workflow",
)
@click.option(
    "--remote",
    required=False,
    is_flag=True,
    default=False,
)
@click.option(
    "-p",
    "--project",
    required=False,
    type=str,
    default="flytesnacks",
)
@click.option(
    "-d",
    "--domain",
    required=False,
    type=str,
    default="development",
)
@click.option(
    "--destination-dir",
    "destination_dir",
    required=False,
    type=str,
    default="/root",
    help="Directory inside the image where the tar file containing the code will be copied to",
)
@click.option(
    "-i",
    "--image",
    "image_config",
    required=False,
    multiple=True,
    type=click.UNPROCESSED,
    callback=ImageConfig.validate_image,
    default=["ghcr.io/flyteorg/flytekit:py3.9-latest"],
    help="Image used to register and run.",
)
@click.pass_context
def run(
    click_ctx,
    file_and_workflow,
    remote,
    project,
    domain,
    destination_dir,
    image_config,
):
    """
    Register command, a.k.a. script mode. It allows for a a single script to be registered and run from the command line
    or any interactive environment (e.g. Jupyter notebooks).
    """
    split_input = file_and_workflow.split(":")
    if len(split_input) != 2:
        raise FlyteValidationException(f"Input {file_and_workflow} must be in format '<file.py>:<worfklow>'")

    filename, workflow_name = split_input
    module = os.path.splitext(filename)[0].replace(os.path.sep, ".")

    # Load code naively, i.e. without taking into account the fully qualified package name
    wf_entity = _load_naive_entity(module, workflow_name)

    if remote:
        config_obj = PlatformConfig.auto()
        client = friendly.SynchronousFlyteClient(config_obj)
        version = script_mode.hash_script_file(filename)
        upload_location: CreateUploadLocationResponse = client.create_upload_location(
            project=project, domain=domain, suffix=f"scriptmode-{version}.tar.gz"
        )
        serialization_settings = SerializationSettings(
            image_config=image_config,
            fast_serialization_settings=FastSerializationSettings(
                enabled=True,
                destination_dir=destination_dir,
                distribution_location=upload_location.native_url,
            ),
        )

        breakpoint()
        remote = FlyteRemote(Config.auto(), default_project=project, default_domain=domain)
        wf = remote.register_workflow_script_mode(
            wf_entity,
            serialization_settings=serialization_settings,
            version=version,
            presigned_url=upload_location.signed_url,
        )

        click.secho(
            f"Go to flyteconsole and check the version {wf.id.version} of workflow {wf.id.name} in project {wf.id.project} and domain {wf.id.domain}"
        )
    else:
        click.secho(wf())


def _load_naive_entity(module_name: str, workflow_name: str) -> WorkflowBase:
    """
    Load the workflow of a the script file.
    N.B.: it assumes that the file is self-contained, in other words, there are no relative imports.
    """
    flyte_ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(
        SerializationSettings(None)
    )
    with context_manager.FlyteContextManager.with_context(flyte_ctx):
        with module_loader.add_sys_path(os.getcwd()):
            importlib.import_module(module_name)
    return module_loader.load_object_from_module(f"{module_name}.{workflow_name}")


def _parse_workflow_inputs(click_ctx, wf_entity):
    type_hints = {k: TypeEngine.guess_python_type(v.type) for k, v in wf_entity.interface.inputs.items()}
    args = {}
    for i in range(0, len(click_ctx.args), 2):
        argument = click_ctx.args[i][2:]
        value = click_ctx.args[i + 1]

        if type_hints[argument] == str:
            value = value
        elif type_hints[argument] == int:
            value = int(value)
        if type_hints[argument] == type(StructuredDataset):
            # Assume it's a pandas dataframe?
            value = pd.read_parquet(value)
        else:
            raise ValueError(f"Unsupported type for argument {argument}")

        args[argument] = value
    return args
