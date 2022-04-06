import importlib
import os

import click
from flyteidl.service.dataproxy_pb2 import CreateUploadLocationResponse

from flytekit.clients import friendly
from flytekit.configuration import Config, FastSerializationSettings, ImageConfig, PlatformConfig, SerializationSettings
from flytekit.core import context_manager
from flytekit.core.workflow import WorkflowBase
from flytekit.exceptions.user import FlyteValidationException
from flytekit.remote.executions import FlyteWorkflowExecution
from flytekit.remote.remote import FlyteRemote
from flytekit.tools import module_loader, script_mode


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.argument(
    "file_and_workflow",
)
@click.option(
    "project",
    "-p",
    "--project",
    required=False,
    type=str,
    default="flytesnacks",
)
@click.option(
    "domain",
    "-d",
    "--domain",
    required=False,
    type=str,
    default="development",
)
@click.option(
    "image",
    "-i",
    "--image",
    required=False,
    type=str,
    # TODO: fix default images push gh workflow
    default="ghcr.io/flyteorg/flytekit:py39-latest",
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
    file_and_workflow,
    project,
    domain,
    image,
    help=None,
):
    """
    Run command, a.k.a. script mode. It allows for a a single script to be registered and run from the command line
    or any interactive environment (e.g. Jupyter notebooks).
    """
    split_input = file_and_workflow.split(":")
    if len(split_input) != 2:
        raise FlyteValidationException(f"Input {file_and_workflow} must be in format '<file.py>:<worfklow>'")

    destination_dir = "/root"

    filename, workflow_name = split_input
    module = os.path.splitext(filename)[0]

    # Load code naively, i.e. without taking into account the
    wf_entity = load_naive_entity(module, workflow_name)

    if help:
        # TODO Write a custom help message containing the types of inputs, if any, and an example of how to specify
        # arguments.
        click.secho(f"Inputs for this workflow are: \n{wf_entity.interface.inputs}")
        return

    config_obj = PlatformConfig.auto()
    client = friendly.SynchronousFlyteClient(config_obj)
    # TODO: the data proxy creates a fixed path. We should add a notion of randomness in the suffix. Haytham is fixing it.
    upload_location: CreateUploadLocationResponse = client.create_upload_location(
        project=project, domain=domain, suffix="scriptmode.tar.gz"
    )
    version = script_mode.hash_script_file(filename)
    image_config = ImageConfig.validate_single_image(image)
    serialization_settings = SerializationSettings(
        image_config=image_config,
        fast_serialization_settings=FastSerializationSettings(
            enabled=True,
            destination_dir=destination_dir,
            distribution_location=upload_location.native_url,
        ),
    )

    remote = FlyteRemote(Config.auto(), default_project=project, default_domain=domain)
    wf = remote.register_workflow(wf_entity, serialization_settings=serialization_settings, version=version)

    # TODO: replace this with signed_url after the fix for the dataproxy in the sandbox is merged
    full_remote_path = upload_location.native_url
    script_mode.fast_register_single_script(version, wf_entity, full_remote_path)

    inputs = parse_inputs(click_ctx, wf_entity)
    execution = remote.execute(wf, inputs=inputs, project=project, domain=domain, wait=True)
    dump_flyte_remote_snippet(execution, project, domain)


def parse_inputs(click_ctx, wf_entity):
    """
    TODO: parse extraneous inputs from the click context and cast them to the appropriate types.
    """
    # This only handles string parameters at the moment
    return {click_ctx.args[i][2:]: click_ctx.args[i + 1] for i in range(0, len(click_ctx.args), 2)}


def load_naive_entity(module_name: str, workflow_name: str) -> WorkflowBase:
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


def dump_flyte_remote_snippet(execution: FlyteWorkflowExecution, project: str, domain: str):
    click.secho(
        f"""
In order to have programmatic access to the execution, use the following snippet:

from flytekit.configuration import Config
from flytekit.remote import FlyteRemote
remote = FlyteRemote(Config.auto(), default_project="{project}", default_domain="{domain}")
exec = remote.fetch_workflow_execution(name="{execution.id.name}")
remote.sync(exec)
print(exec.outputs)
    """
    )
