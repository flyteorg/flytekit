import functools
import importlib
import inspect
import json
import os
from dataclasses import is_dataclass
from datetime import datetime
from typing import cast

import click
import pandas as pd
import typing
from dataclasses_json import DataClassJsonMixin

from flytekit.configuration import Config, ImageConfig, SerializationSettings
from flytekit.configuration.default_images import DefaultImages
from flytekit.core import context_manager
from flytekit.core.workflow import WorkflowBase, PythonFunctionWorkflow
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

REMOTE_KEY = "remote"


class JsonParamType(click.ParamType):
    name = "json_param"

    def convert(self, value, param, ctx) -> typing.Union[typing.Dict, typing.List]:
        if value is None:
            return None
        if isinstance(value, list) or isinstance(value, dict):
            return value
        return json.loads(value)


class DataframeType(click.ParamType):
    name = "dataframe"

    def convert(self, value, param, ctx):
        if not ctx.obj[REMOTE_KEY]:
            return pd.read_parquet(value)


class DataclassType(click.ParamType):
    name = "dataclass"

    def __init__(self, dataclass_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataclass_type = dataclass_type

    def convert(self, value, param, ctx):
        return cast(DataClassJsonMixin, self._dataclass_type).from_json(value)


def get_param_type_override(input_type: typing.Any) -> typing.Optional[click.ParamType]:
    """
    This handles converting workflow input types to supported click parameters with callbacks to initialize
    the input values correctly
    """
    if input_type is datetime:
        return click.DateTime()
    if is_dataclass(input_type):
        return DataclassType(input_type)
    if input_type == pd.DataFrame:
        return DataframeType()
    if inspect.isclass(input_type):
        if issubclass(input_type, (FlyteFile, FlyteSchema, StructuredDataset, FlyteDirectory)):
            raise NotImplementedError(
                click.style(
                    "Flyte[File, Schema, Directory] & StructuredDataSet is not yet implemented in pyflyte run", fg="red"
                )
            )

    origin_type = typing.get_origin(input_type)
    if origin_type in [dict, list]:
        return JsonParamType()

    # Filter through the union of types to see if any of them has a registered callback
    if origin_type is typing.Union:
        types = input_type.__args__
        for t in types:
            param = get_param_type_override(t)
            if param is not None:
                return param

    return None


def set_is_remote(ctx: click.Context, param: str, value: str):
    ctx.obj[REMOTE_KEY] = bool(value)


def get_workflow_command_base_params() -> typing.List[click.Option]:
    """
    Return the set of base parameters added to every pyflyte run workflow subcommand.
    """
    return [
        click.Option(
            param_decls=["--remote"],
            required=False,
            is_flag=True,
            default=False,
            is_eager=True,
            callback=set_is_remote,
        ),
        click.Option(
            param_decls=["-p", "--project"],
            required=False,
            type=str,
            default="flytesnacks",
        ),
        click.Option(
            param_decls=["-d", "--domain"],
            required=False,
            type=str,
            default="development",
        ),
        click.Option(
            param_decls=["--destination-dir", "destination_dir"],
            required=False,
            type=str,
            default="/root",
            help="Directory inside the image where the tar file containing the code will be copied to",
        ),
        click.Option(
            param_decls=["-i", "--image", "image_config"],
            required=False,
            multiple=True,
            type=click.UNPROCESSED,
            callback=ImageConfig.validate_image,
            default=[DefaultImages.default_image()],
            help="Image used to register and run.",
        ),
        click.Option(
            param_decls=["--service-account", "service_account"],
            required=False,
            type=str,
            default="",
        ),
        click.Option(
            param_decls=["--wait-execution", "wait_execution"],
            required=False,
            is_flag=True,
            default=False,
            help="Whether wait for the execution to finish",
        ),
        click.Option(
            param_decls=["--dump-snippet", "dump_snippet"],
            required=False,
            is_flag=True,
            default=False,
            help="Whether dump a code snippet instructing how to load the workflow execution using flyteremote",
        ),
    ]


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
exec = remote.fetch_execution(name="{execution.id.name}")
remote.sync(exec)
print(exec.outputs)
    """
    )


def get_workflows_in_file(filename: str) -> typing.List[str]:
    flyte_ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(
        SerializationSettings(None)
    )
    module_name = os.path.splitext(filename)[0].replace(os.path.sep, ".")
    with context_manager.FlyteContextManager.with_context(flyte_ctx):
        with module_loader.add_sys_path(os.getcwd()):
            importlib.import_module(module_name)

    workflows = []
    module = importlib.import_module(module_name)
    for k in dir(module):
        o = module.__dict__[k]
        if isinstance(o, PythonFunctionWorkflow):
            module_name_prefix = f"{module_name}."
            workflows.append(f"{o.name.lstrip(module_name_prefix)}")

    return workflows


def run(ctx: click.Context, filename: str, workflow_name: str, *args, **kwargs):
    def _run(*args, **kwargs):
        project, domain = kwargs.get("project"), kwargs.get("domain")
        module_name = os.path.splitext(filename)[0].replace(os.path.sep, ".")
        wf_entity = load_naive_entity(module_name, workflow_name)
        inputs = {}
        for input_name, _ in wf_entity.python_interface.inputs.items():
            inputs[input_name] = kwargs.get(input_name)

        print(f"is remote? {ctx.obj[REMOTE_KEY]}")
        if not ctx.obj[REMOTE_KEY]:
            output = wf_entity(**inputs)
            click.echo(output)
            return

        remote = FlyteRemote(Config.auto(), default_project=project, default_domain=domain)
        get_upload_url_fn = functools.partial(remote.client.get_upload_signed_url, project=project, domain=domain)

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
            image_config=kwargs.get("image_config", None),
            destination_dir=kwargs.get("destination_dir"),
        )

        options = None
        service_account = kwargs.get("service_account")
        if service_account is not None:
            # options are only passed for the execution. This is to prevent errors when registering a duplicate workflow
            # It is assumed that the users expectations is to override the service account only for the execution
            options = Options.default_from(k8s_service_account=service_account)

        execution = remote.execute(
            wf,
            inputs=inputs,
            project=project,
            domain=domain,
            wait=kwargs.get("wait_execution"),
            options=options,
            type_hints=wf_entity.python_interface.inputs,
        )

        console_url = remote.generate_console_url(execution)
        click.secho(f"Go to {console_url} to see execution in the console.")

        if kwargs.get("dump_snippet"):
            dump_flyte_remote_snippet(execution, project, domain)

    return _run


class WorkflowCommand(click.MultiCommand):
    def __init__(self, filename: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = filename

    def list_commands(self, ctx):
        workflows = get_workflows_in_file(self._filename)
        return workflows

    def get_command(self, ctx, workflow):
        module = os.path.splitext(self._filename)[0].replace(os.path.sep, ".")
        wf_entity = load_naive_entity(module, workflow)

        params = get_workflow_command_base_params()
        for input_name, input_type in wf_entity.python_interface.inputs.items():
            # add remote flag
            param_type = get_param_type_override(input_type)
            if param_type is None:
                param_type = input_type
            _, default_value = wf_entity.python_interface.inputs_with_defaults.get(input_name)
            params.append(
                click.Option(
                    param_decls=[f"--{input_name}"],
                    type=param_type,
                    is_flag=input_type == bool,
                    default=default_value,
                    show_default=True,
                    # required=default_value is None,
                )
            )
        cmd = click.Command(name=workflow, params=params, callback=run(ctx, self._filename, workflow))
        return cmd


class RunCommand(click.MultiCommand):
    def list_commands(self, ctx):
        rv = []
        return rv

    def get_command(self, ctx, filename):
        return WorkflowCommand(filename, name=filename, help="foo")


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


run_command = RunCommand(
    name="run",
    help="Run_old command, a.k.a. script mode. It allows for a a single script to be "
         + "registered and run from the command line or any interactive environment "
         + "(e.g. Jupyter notebooks).",
)
