import functools
import importlib
import inspect
import json
import os
import pathlib
import typing
from dataclasses import is_dataclass
from datetime import datetime
from typing import cast

import click
import pandas as pd
from dataclasses_json import DataClassJsonMixin

from flytekit import Literal, Scalar
from flytekit.configuration import Config, ImageConfig, SerializationSettings
from flytekit.configuration.default_images import DefaultImages
from flytekit.core import context_manager
from flytekit.core.context_manager import FlyteContextManager, FlyteContext
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import PythonFunctionWorkflow, WorkflowBase
from flytekit.models import literals
from flytekit.models.interface import Variable
from flytekit.models.types import StructuredDatasetType, LiteralType, SimpleType
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

REMOTE_FLAG_KEY = "remote"
RUN_LEVEL_PARAMS_KEY = "run_level_params"
FLYTE_REMOTE_INSTANCE_KEY = "flyte_remote"
DATA_PROXY_CALLBACK_KEY = "data_proxy"


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


class JsonParamType(click.ParamType):
    name = "json_param"

    def convert(self, value, param, ctx) -> typing.Optional[typing.Union[typing.Dict, typing.List]]:
        if value is None:
            return None
        if isinstance(value, list) or isinstance(value, dict):
            return value
        return json.loads(value)


class StructuredDatasetParamType(click.ParamType):
    name = "structured_dataset"

    def __init__(self, ctx: click.Context, flyte_ctx: FlyteContext, literal_type: LiteralType, python_type: typing.Type,
                 get_upload_url_fn: typing.Callable):
        self._remote = ctx.obj[REMOTE_FLAG_KEY]
        self._literal_type = literal_type
        self._python_type = python_type
        self._create_upload_fn = get_upload_url_fn
        self._flyte_ctx = flyte_ctx

    def convert(self, value, param, ctx) -> typing.Union[Literal, typing.Any]:
        uri = value
        local_path = pathlib.Path(uri)
        if local_path.is_file():
            local_file_name = str(local_path.resolve())
        elif local_path.is_dir():
            files = list(local_path.iterdir())
            if len(files) != 1:
                raise ValueError(
                    f"The data proxy encoder can only operate on folders containing one file currently, {len(files)} found in {local_path.name.resolve()}"
                )
            local_file_name = str(files[0].resolve())
        else:
            raise ValueError(f"Unknown path type {local_path}")

        if self._remote:
            md5, _ = script_mode.hash_file(local_file_name)
            remote_filename = "00000.parquet"
            df_remote_location = self._create_upload_fn(filename=remote_filename, content_md5=md5)
            self._flyte_ctx.file_access.put_data(local_file_name, df_remote_location.signed_url)
            uri = df_remote_location.native_url[: -len(remote_filename)]

        lit = Literal(
            scalar=Scalar(
                structured_dataset=literals.StructuredDataset(
                    uri=uri,
                    metadata=literals.StructuredDatasetMetadata(
                        structured_dataset_type=self._literal_type.structured_dataset_type),
                ),
            ),
        )

        if not self._remote:
            return TypeEngine.to_python_value(self._flyte_ctx, lit, self._python_type)

        return lit


class DataclassType(click.ParamType):
    name = "dataclass"

    def __init__(self, dataclass_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataclass_type = dataclass_type

    def convert(self, value, param, ctx):
        return cast(DataClassJsonMixin, self._dataclass_type).from_json(value)


def get_param_type_override(ctx: click.Context, input_type: typing.Any) -> typing.Optional[click.ParamType]:
    """
    This handles converting workflow input types to supported click parameters with callbacks to initialize
    the input values to their expected types.
    """
    if input_type is datetime:
        return click.DateTime()
    # This needs to be above the dataclass check since StructuredDataset is also a dataclass
    if issubclass(input_type, StructuredDataset):
        return StructuredDatasetParamType(ctx, input_type)
    if is_dataclass(input_type):
        return DataclassType(input_type)
    if issubclass(input_type, pd.DataFrame):
        return DataframeType(input_type)
    if inspect.isclass(input_type):
        if issubclass(input_type, (FlyteFile, FlyteSchema, FlyteDirectory)):
            raise NotImplementedError(
                click.style("Flyte[File, Schema, Directory] are not yet implemented in pyflyte run", fg="red")
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



def to_click_type(ctx: click.Context, flyte_ctx: FlyteContext, literal_type: LiteralType, python_type: typing.Type,
                  get_upload_url_fn: typing.Callable) -> typing.Tuple[
    click.ParamType, typing.Optional[typing.Callable[[click.Context, click.Parameter, typing.Any], typing.Any]]]:
    if literal_type.structured_dataset_type:
        return StructuredDatasetParamType(ctx, flyte_ctx, literal_type=literal_type, python_type=python_type,
                                          get_upload_url_fn=get_upload_url_fn), None
    if literal_type.enum_type:
        return click.Choice(literal_type.enum_type.values),

    if literal_type.simple:
        if literal_type.simple


def to_click_option(ctx: click.Context, flyte_ctx: FlyteContext, input_name: str, literal_var: Variable,
                    input_val: typing.Tuple[typing.Type, typing.Any],
                    get_upload_url_fn: typing.Callable) -> click.Option:
    """
    This handles converting workflow input types to supported click parameters with callbacks to initialize
    the input values to their expected types.
    """
    input_type = input_val[0]
    if literal_var.type.structured_dataset_type:
        click_type = StructuredDatasetParamType(ctx, flyte_ctx, literal_type=literal_var.type, python_type=input_type,
                                                get_upload_url_fn=get_upload_url_fn)
    elif input_type is datetime:
        click_type = click.DateTime()
    elif is_dataclass(input_type):
        click_type = DataclassType(input_type)
    elif inspect.isclass(input_type):
        if issubclass(input_type, (FlyteFile, FlyteSchema, FlyteDirectory)):
            raise NotImplementedError(
                click.style("Flyte[File, Schema, Directory] are not yet implemented in pyflyte run", fg="red")
            )

    else:
        origin_type = typing.get_origin(input_type)
        if origin_type in [dict, list]:
            click_type = JsonParamType()
        # Filter through the union of types to see if any of them has a registered callback
        if origin_type is typing.Union:
            types = input_type.__args__
            for t in types:
                param = get_param_type_override(t)
                if param is not None:
                    return param

    default_val = input_val[1]
    return click.Option(
        param_decls=[f"--{input_name}"],
        type=click_type,
        is_flag=input_type == bool,
        default=default_val,
        show_default=True,
        required=default_val is None,
        help=literal_var.description,
    )


def set_is_remote(ctx: click.Context, param: str, value: str):
    ctx.obj[REMOTE_FLAG_KEY] = bool(value)


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
            expose_value=False,  # since we're handling in the callback, no need to expose this in params
            is_eager=True,
            callback=set_is_remote,
            help="Whether to register and run the workflow on a Flyte deployment",
        ),
        click.Option(
            param_decls=["-p", "--project"],
            required=False,
            type=str,
            default="flytesnacks",
            help="Project to register and run this workflow in",
        ),
        click.Option(
            param_decls=["-d", "--domain"],
            required=False,
            type=str,
            default="development",
            help="Domain to register and run this workflow in",
        ),
        click.Option(
            param_decls=["--name"],
            required=False,
            type=str,
            help="Name to assign to this execution",
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
            help="Service account used when executing this workflow",
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
    """
    Returns a list of flyte workflow names in a file.
    """
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
            wf_name_only = remove_prefix(o.name, module_name_prefix)
            workflows.append(wf_name_only)

    return workflows


def run_command(ctx: click.Context, filename: str, workflow_name: str, *args, **kwargs):
    """
    Returns a function that is used to implement WorkflowCommand and execute a flyte workflow.
    """

    def _run(*args, **kwargs):
        run_level_params = ctx.obj[RUN_LEVEL_PARAMS_KEY]
        project, domain = run_level_params.get("project"), run_level_params.get("domain")
        module_name = os.path.splitext(filename)[0].replace(os.path.sep, ".")
        wf_entity = load_naive_entity(module_name, workflow_name)
        inputs = {}
        for input_name, _ in wf_entity.python_interface.inputs.items():
            inputs[input_name] = kwargs.get(input_name)

        if not ctx.obj[REMOTE_FLAG_KEY]:
            output = wf_entity(**inputs)
            click.echo(output)
            return

        remote = ctx.obj[FLYTE_REMOTE_INSTANCE_KEY]

        # StructuredDatasetTransformerEngine.register(
        #     PandasToParquetDataProxyEncodingHandler(get_upload_url_fn), default_for_type=True
        # )

        wf = remote.register_script(
            wf_entity,
            project=project,
            domain=domain,
            image_config=run_level_params.get("image_config", None),
            destination_dir=run_level_params.get("destination_dir"),
        )

        options = None
        service_account = run_level_params.get("service_account")
        if service_account:
            # options are only passed for the execution. This is to prevent errors when registering a duplicate workflow
            # It is assumed that the users expectations is to override the service account only for the execution
            options = Options.default_from(k8s_service_account=service_account)

        execution = remote.execute(
            wf,
            inputs=inputs,
            project=project,
            domain=domain,
            name=run_level_params.get("name"),
            wait=run_level_params.get("wait_execution"),
            options=options,
            type_hints=wf_entity.python_interface.inputs,
        )

        console_url = remote.generate_console_url(execution)
        click.secho(f"Go to {console_url} to see execution in the console.")

        if run_level_params.get("dump_snippet"):
            dump_flyte_remote_snippet(execution, project, domain)

    return _run


class WorkflowCommand(click.MultiCommand):
    """
    click multicommand at the python file layer, subcommands should be all the workflows in the file.
    """

    def __init__(self, filename: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = filename

    def list_commands(self, ctx):
        workflows = get_workflows_in_file(self._filename)
        return workflows

    def get_command(self, ctx, workflow):
        module = os.path.splitext(self._filename)[0].replace(os.path.sep, ".")
        wf_entity = load_naive_entity(module, workflow)

        # If this is a remote execution, which we should know at this point, then create the remote object
        p = ctx.obj[RUN_LEVEL_PARAMS_KEY].get("project")
        d = ctx.obj[RUN_LEVEL_PARAMS_KEY].get("domain")
        r = FlyteRemote(Config.auto(), default_project=p, default_domain=d)
        ctx.obj[FLYTE_REMOTE_INSTANCE_KEY] = r
        get_upload_url_fn = functools.partial(r.client.get_upload_signed_url, project=p, domain=d)

        flyte_ctx = context_manager.FlyteContextManager.current_context()

        # Add options for each of the workflow inputs
        params = []
        for input_name, input_type_val in wf_entity.python_interface.inputs_with_defaults.items():
            literal_var = wf_entity.interface.inputs.get(input_name)
            params.append(to_click_option(ctx, flyte_ctx, input_name, literal_var, input_type_val, get_upload_url_fn))
        cmd = click.Command(
            name=workflow,
            params=params,
            callback=run_command(ctx, self._filename, workflow),
            help=f"Run {module}.{workflow} in script mode",
        )
        return cmd


class RunCommand(click.MultiCommand):
    """
    A click command group for registering and executing flyte workflows in a file.
    """

    def __init__(self, *args, **kwargs):
        params = get_workflow_command_base_params()
        super().__init__(*args, params=params, **kwargs)

    def list_commands(self, ctx):
        rv = []
        return rv

    def get_command(self, ctx, filename):
        ctx.obj[RUN_LEVEL_PARAMS_KEY] = ctx.params
        return WorkflowCommand(filename, name=filename, help="Run a workflow in a file using script mode")


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
        local_path = pathlib.Path(structured_dataset.uri)
        if local_path.is_file():
            local_file_name = str(local_path.resolve())
        elif local_path.is_dir():
            files = list(local_path.iterdir())
            if len(files) != 1:
                raise ValueError(
                    f"The data proxy encoder can only operate on folders containing one file currently, {len(files)} found in {local_path.name.resolve()}"
                )
            local_file_name = str(files[0].resolve())
        else:
            raise ValueError(f"Unknown path type {local_path}")
        md5, _ = script_mode.hash_file(local_file_name)
        remote_filename = "00000.parquet"
        df_remote_location = self._create_upload_fn(filename=remote_filename, content_md5=md5)
        flyte_ctx = context_manager.FlyteContextManager.current_context()
        flyte_ctx.file_access.put_data(local_file_name, df_remote_location.signed_url)

        structured_dataset_type.format = PARQUET
        return literals.StructuredDataset(
            uri=df_remote_location.native_url[: -len(remote_filename)],
            metadata=literals.StructuredDatasetMetadata(structured_dataset_type),
        )


run = RunCommand(
    name="run",
    help="Run_old command, a.k.a. script mode. It allows for a a single script to be "
         + "registered and run from the command line (e.g. Jupyter notebooks).",
)
