import functools
import importlib
import json
import os
from dataclasses import is_dataclass
from datetime import datetime
from typing import Callable, Optional, cast

import click
import pandas as pd
from dataclasses_json import DataClassJsonMixin

from flytekit.configuration import Config, ImageConfig, SerializationSettings
from flytekit.configuration.default_images import DefaultImages
from flytekit.core import context_manager
from flytekit.core.workflow import WorkflowBase
from flytekit.exceptions.user import FlyteValidationException
from flytekit.models import literals
from flytekit.models.types import StructuredDatasetType
from flytekit.remote.executions import FlyteWorkflowExecution
from flytekit.remote.remote import FlyteRemote
from flytekit.tools import module_loader, script_mode
from flytekit.tools.translator import Options
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.types.schema import FlyteSchema
from flytekit.types.structured.structured_dataset import (
    StructuredDataset,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)


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
    "is_remote",
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
    # Notice how the default value assumes that the working directory is set appropriately in the docker file.
    default=".",
    help="Directory inside the image where the tar file containing the code will be copied to.",
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
    help="Image used to register and run.",
)
@click.option(
    "--service-account",
    "service_account",
    required=False,
    type=str,
    default="",
)
@click.option(
    "--wait-execution",
    "wait_execution",
    required=False,
    is_flag=True,
    default=False,
    help="Whether wait for the execution to finish",
)
@click.option(
    "--dump-snippet",
    "dump_snippet",
    required=False,
    is_flag=True,
    default=False,
    help="Whether dump a code snippet instructing how to load the workflow execution using flyteremote",
)
@click.pass_context
def run(
    click_ctx,
    file_and_workflow,
    is_remote,
    project,
    domain,
    destination_dir,
    image_config,
    service_account,
    wait_execution,
    dump_snippet,
):
    """
    Run command, a.k.a. script mode. It allows for a a single script to be registered and run from the command line
    or any interactive environment (e.g. Jupyter notebooks).
    """
    split_input = file_and_workflow.split(":")
    if len(split_input) != 2:
        raise FlyteValidationException(f"Input {file_and_workflow} must be in format '<file.py>:<worfklow>'")

    filename, workflow_name = split_input
    module = os.path.splitext(filename)[0].replace(os.path.sep, ".")

    # Load code naively, i.e. without taking into account the fully qualified package name
    wf_entity = _load_naive_entity(module, workflow_name)

    if is_remote:
        remote = FlyteRemote(Config.auto(), default_project=project, default_domain=domain)
        get_upload_url_fn = functools.partial(remote.client.get_upload_signed_url, project=project, domain=domain)
        inputs = _parse_workflow_inputs(
            click_ctx,
            wf_entity,
            get_upload_url_fn,
            is_remote=True,
        )

        # TODO: leave a comment explaining why we need to register twice
        StructuredDatasetTransformerEngine.register(
            PandasToParquetDataProxyEncodingHandler(get_upload_url_fn), default_for_type=True
        )
        StructuredDatasetTransformerEngine.register(
            PandasToParquetDataProxyEncodingHandler(get_upload_url_fn, kind=StructuredDataset), default_for_type=True
        )

        wf = remote.register_script(
            wf_entity,
            project=project,
            domain=domain,
            image_config=image_config,
            destination_dir=destination_dir,
        )

        options = None
        if service_account:
            # options are only passed for the execution. This is to prevent errors when registering a duplicate workflow
            # It is assumed that the users expectations is to override the service account only for the execution
            options = Options.default_from(k8s_service_account=service_account)

        execution = remote.execute(
            wf,
            inputs=inputs,
            project=project,
            domain=domain,
            wait=wait_execution,
            options=options,
            type_hints=wf_entity.python_interface.inputs,
        )

        console_url = remote.generate_console_url(execution)
        click.secho(f"Go to {console_url} to see execution in the console.")

        if dump_snippet:
            _dump_flyte_remote_snippet(execution, project, domain)
    else:
        inputs = _parse_workflow_inputs(
            click_ctx,
            wf_entity,
            is_remote=False,
        )
        output = wf_entity(**inputs)
        click.echo(output)


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


def _parse_workflow_inputs(click_ctx, wf_entity, create_upload_location_fn: Optional[Callable] = None, is_remote=False):
    args = {}
    for i in range(0, len(click_ctx.args), 2):
        argument = click_ctx.args[i][2:].replace("-", "_")
        value = click_ctx.args[i + 1]

        if argument not in wf_entity.interface.inputs:
            raise FlyteValidationException(
                click.style(f"argument '{argument}' is not listed as a parameter of the workflow", fg="red")
            )

        python_type = wf_entity.python_interface.inputs[argument]

        if python_type == str:
            value = value
        elif python_type == int:
            value = int(value)
        elif python_type == float:
            value = float(value)
        elif python_type == bool:
            true_values = {"TRUE", "True", "true", "1"}
            bool_values = true_values.union({"FALSE", "False", "false", "0"})
            if value not in bool_values:
                raise ValueError(click.style(f"bool type expected one of {bool_values}, found '{value}'", fg="red"))
            value = value in true_values
        elif python_type == datetime:
            value = datetime.fromtimestamp(int(value)) if value.isnumeric() else datetime.fromisoformat(value)
        elif getattr(python_type, "__origin__", None) in {list, dict}:
            value = json.loads(value)
        elif issubclass(python_type, (FlyteFile, FlyteSchema, StructuredDataset, FlyteDirectory)):
            raise NotImplementedError(
                click.style(
                    "Flyte[File, Schema, Directory] & StructuredDataSet is not yet implemented in pyflyte run", fg="red"
                )
            )
        elif is_dataclass(python_type):
            dataclass_type = python_type
            value = cast(DataClassJsonMixin, dataclass_type).from_json(value)
        elif python_type == pd.DataFrame:
            # the PandasToParquetDataProxyEncodingHandler handles converting the input value into a dataframe
            # in the remote case
            if not is_remote:
                value = pd.read_parquet(value)
        else:
            raise ValueError(f"Unsupported type '{python_type}' for argument {argument}")

        args[argument] = value
    return args


def _dump_flyte_remote_snippet(execution: FlyteWorkflowExecution, project: str, domain: str):
    click.secho(
        f"""
In order to have programmatic access to the execution, use the following snippet:

from flytekit.configuration import Config
from flytekit.remote import FlyteRemote
remote = FlyteRemote(Config.auto(), default_project="{project}", default_domain="{domain}")
exec = remote.fetch_execution(name="{execution.id.name}")
remote.sync(exec)
print(exec.outputs)
    """
    )


PARQUET = "parquet"


class PandasToParquetDataProxyEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, create_upload_fn, kind=pd.DataFrame):
        super().__init__(kind, "remote", PARQUET)
        self._create_upload_fn = create_upload_fn

    def encode(
        self,
        ctx: context_manager.FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        local_path = structured_dataset.dataframe

        filename = "00000.parquet"
        md5, _ = script_mode.hash_file(local_path)
        df_remote_location = self._create_upload_fn(filename=filename, content_md5=md5)
        flyte_ctx = context_manager.FlyteContextManager.current_context()
        flyte_ctx.file_access.put_data(local_path, df_remote_location.signed_url)

        structured_dataset_type.format = PARQUET
        return literals.StructuredDataset(
            uri=df_remote_location.native_url[: -len(filename)],
            metadata=literals.StructuredDatasetMetadata(structured_dataset_type),
        )
