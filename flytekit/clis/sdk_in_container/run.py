import datetime
import functools
import importlib
import json
import logging
import os
import pathlib
import typing
from dataclasses import dataclass
from typing import cast

import cloudpickle
import rich_click as click
import yaml
from dataclasses_json import DataClassJsonMixin
from pytimeparse import parse
from typing_extensions import get_args

from flytekit import BlobType, Literal, Scalar
from flytekit.clis.sdk_in_container.constants import (
    CTX_CONFIG_FILE,
    CTX_COPY_ALL,
    CTX_DOMAIN,
    CTX_MODULE,
    CTX_PROJECT,
    CTX_PROJECT_ROOT,
)
from flytekit.clis.sdk_in_container.helpers import (
    FLYTE_REMOTE_INSTANCE_KEY,
    get_and_save_remote_with_click_context,
    patch_image_config,
)
from flytekit.configuration import ImageConfig
from flytekit.configuration.default_images import DefaultImages
from flytekit.core import context_manager
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.type_engine import TypeEngine
from flytekit.core.workflow import PythonFunctionWorkflow, WorkflowBase
from flytekit.models import literals
from flytekit.models.interface import Variable
from flytekit.models.literals import Blob, BlobMetadata, LiteralCollection, LiteralMap, Primitive, Union
from flytekit.models.types import LiteralType, SimpleType
from flytekit.remote.executions import FlyteWorkflowExecution
from flytekit.tools import module_loader, script_mode
from flytekit.tools.script_mode import _find_project_root
from flytekit.tools.translator import Options
from flytekit.types.pickle.pickle import FlytePickleTransformer

REMOTE_FLAG_KEY = "remote"
RUN_LEVEL_PARAMS_KEY = "run_level_params"
DATA_PROXY_CALLBACK_KEY = "data_proxy"


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


@dataclass
class Directory(object):
    dir_path: str
    local_file: typing.Optional[pathlib.Path] = None
    local: bool = True


class DirParamType(click.ParamType):
    name = "directory path"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if FileAccessProvider.is_remote(value):
            return Directory(dir_path=value, local=False)
        p = pathlib.Path(value)
        if p.exists() and p.is_dir():
            files = list(p.iterdir())
            if len(files) != 1:
                raise ValueError(
                    f"Currently only directories containing one file are supported, found [{len(files)}] files found in {p.resolve()}"
                )
            return Directory(dir_path=str(p), local_file=files[0].resolve())
        raise click.BadParameter(f"parameter should be a valid directory path, {value}")


@dataclass
class FileParam(object):
    filepath: str
    local: bool = True


class FileParamType(click.ParamType):
    name = "file path"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if FileAccessProvider.is_remote(value):
            return FileParam(filepath=value, local=False)
        p = pathlib.Path(value)
        if p.exists() and p.is_file():
            return FileParam(filepath=str(p.resolve()))
        raise click.BadParameter(f"parameter should be a valid file path, {value}")


class PickleParamType(click.ParamType):
    name = "pickle"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:

        uri = FlyteContextManager.current_context().file_access.get_random_local_path()
        with open(uri, "w+b") as outfile:
            cloudpickle.dump(value, outfile)
        return FileParam(filepath=str(pathlib.Path(uri).resolve()))


class DateTimeType(click.DateTime):

    _NOW_FMT = "now"
    _ADDITONAL_FORMATS = [_NOW_FMT]

    def __init__(self):
        super().__init__()
        self.formats.extend(self._ADDITONAL_FORMATS)

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if value in self._ADDITONAL_FORMATS:
            if value == self._NOW_FMT:
                return datetime.datetime.now()
        return super().convert(value, param, ctx)


class DurationParamType(click.ParamType):
    name = "[1:24 | :22 | 1 minute | 10 days | ...]"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if value is None:
            raise click.BadParameter("None value cannot be converted to a Duration type.")
        return datetime.timedelta(seconds=parse(value))


class JsonParamType(click.ParamType):
    name = "json object OR json/yaml file path"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if value is None:
            raise click.BadParameter("None value cannot be converted to a Json type.")
        if type(value) == dict or type(value) == list:
            return value
        try:
            return json.loads(value)
        except Exception:  # noqa
            try:
                # We failed to load the json, so we'll try to load it as a file
                if os.path.exists(value):
                    # if the value is a yaml file, we'll try to load it as yaml
                    if value.endswith(".yaml") or value.endswith(".yml"):
                        with open(value, "r") as f:
                            return yaml.safe_load(f)
                    with open(value, "r") as f:
                        return json.load(f)
                raise
            except json.JSONDecodeError as e:
                raise click.BadParameter(f"parameter {param} should be a valid json object, {value}, error: {e}")


@dataclass
class DefaultConverter(object):
    click_type: click.ParamType
    primitive_type: typing.Optional[str] = None
    scalar_type: typing.Optional[str] = None

    def convert(self, value: typing.Any, python_type_hint: typing.Optional[typing.Type] = None) -> Scalar:
        if self.primitive_type:
            return Scalar(primitive=Primitive(**{self.primitive_type: value}))
        if self.scalar_type:
            return Scalar(**{self.scalar_type: value})

        raise NotImplementedError("Not implemented yet!")


class FlyteLiteralConverter(object):
    name = "literal_type"

    SIMPLE_TYPE_CONVERTER: typing.Dict[SimpleType, DefaultConverter] = {
        SimpleType.FLOAT: DefaultConverter(click.FLOAT, primitive_type="float_value"),
        SimpleType.INTEGER: DefaultConverter(click.INT, primitive_type="integer"),
        SimpleType.STRING: DefaultConverter(click.STRING, primitive_type="string_value"),
        SimpleType.BOOLEAN: DefaultConverter(click.BOOL, primitive_type="boolean"),
        SimpleType.DURATION: DefaultConverter(DurationParamType(), primitive_type="duration"),
        SimpleType.DATETIME: DefaultConverter(click.DateTime(), primitive_type="datetime"),
    }

    def __init__(
        self,
        ctx: click.Context,
        flyte_ctx: FlyteContext,
        literal_type: LiteralType,
        python_type: typing.Type,
        get_upload_url_fn: typing.Callable,
    ):
        self._remote = ctx.obj[REMOTE_FLAG_KEY]
        self._literal_type = literal_type
        self._python_type = python_type
        self._create_upload_fn = get_upload_url_fn
        self._flyte_ctx = flyte_ctx
        self._click_type = click.UNPROCESSED

        if self._literal_type.simple:
            if self._literal_type.simple == SimpleType.STRUCT:
                self._click_type = JsonParamType()
                self._click_type.name = f"JSON object {self._python_type.__name__}"
            elif self._literal_type.simple not in self.SIMPLE_TYPE_CONVERTER:
                raise NotImplementedError(f"Type {self._literal_type.simple} is not supported in pyflyte run")
            else:
                self._converter = self.SIMPLE_TYPE_CONVERTER[self._literal_type.simple]
                self._click_type = self._converter.click_type

        if self._literal_type.enum_type:
            self._converter = self.SIMPLE_TYPE_CONVERTER[SimpleType.STRING]
            self._click_type = click.Choice(self._literal_type.enum_type.values)

        if self._literal_type.structured_dataset_type:
            self._click_type = DirParamType()

        if self._literal_type.collection_type or self._literal_type.map_value_type:
            self._click_type = JsonParamType()
            if self._literal_type.collection_type:
                self._click_type.name = "json list"
            else:
                self._click_type.name = "json dictionary"

        if self._literal_type.blob:
            if self._literal_type.blob.dimensionality == BlobType.BlobDimensionality.SINGLE:
                if self._literal_type.blob.format == FlytePickleTransformer.PYTHON_PICKLE_FORMAT:
                    self._click_type = PickleParamType()
                else:
                    self._click_type = FileParamType()
            else:
                self._click_type = DirParamType()

    @property
    def click_type(self) -> click.ParamType:
        return self._click_type

    def is_bool(self) -> bool:
        if self._literal_type.simple:
            return self._literal_type.simple == SimpleType.BOOLEAN
        return False

    def get_uri_for_dir(
        self, ctx: typing.Optional[click.Context], value: Directory, remote_filename: typing.Optional[str] = None
    ):
        uri = value.dir_path

        if self._remote and value.local:
            md5, _ = script_mode.hash_file(value.local_file)
            if not remote_filename:
                remote_filename = value.local_file.name
            remote = ctx.obj[FLYTE_REMOTE_INSTANCE_KEY]
            _, native_url = remote.upload_file(value.local_file)
            uri = native_url[: -len(remote_filename)]

        return uri

    def convert_to_structured_dataset(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: Directory
    ) -> Literal:

        uri = self.get_uri_for_dir(ctx, value, "00000.parquet")

        lit = Literal(
            scalar=Scalar(
                structured_dataset=literals.StructuredDataset(
                    uri=uri,
                    metadata=literals.StructuredDatasetMetadata(
                        structured_dataset_type=self._literal_type.structured_dataset_type
                    ),
                ),
            ),
        )

        return lit

    def convert_to_blob(
        self,
        ctx: typing.Optional[click.Context],
        param: typing.Optional[click.Parameter],
        value: typing.Union[Directory, FileParam],
    ) -> Literal:
        if isinstance(value, Directory):
            uri = self.get_uri_for_dir(ctx, value)
        else:
            uri = value.filepath
            if self._remote and value.local:
                fp = pathlib.Path(value.filepath)
                remote = ctx.obj[FLYTE_REMOTE_INSTANCE_KEY]
                _, uri = remote.upload_file(fp)

        lit = Literal(
            scalar=Scalar(
                blob=Blob(
                    metadata=BlobMetadata(type=self._literal_type.blob),
                    uri=uri,
                ),
            ),
        )

        return lit

    def convert_to_union(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: typing.Any
    ) -> Literal:
        lt = self._literal_type
        for i in range(len(self._literal_type.union_type.variants)):
            variant = self._literal_type.union_type.variants[i]
            python_type = get_args(self._python_type)[i]
            converter = FlyteLiteralConverter(
                ctx,
                self._flyte_ctx,
                variant,
                python_type,
                self._create_upload_fn,
            )
            try:
                # Here we use click converter to convert the input in command line to native python type,
                # and then use flyte converter to convert it to literal.
                python_val = converter._click_type.convert(value, param, ctx)
                literal = converter.convert_to_literal(ctx, param, python_val)
                return Literal(scalar=Scalar(union=Union(literal, variant)))
            except (Exception or AttributeError) as e:
                logging.debug(f"Failed to convert python type {python_type} to literal type {variant}", e)
        raise ValueError(f"Failed to convert python type {self._python_type} to literal type {lt}")

    def convert_to_list(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: list
    ) -> Literal:
        """
        Convert a python list into a Flyte Literal
        """
        if not value:
            raise click.BadParameter("Expected non-empty list")
        if not isinstance(value, list):
            raise click.BadParameter(f"Expected json list '[...]', parsed value is {type(value)}")
        converter = FlyteLiteralConverter(
            ctx,
            self._flyte_ctx,
            self._literal_type.collection_type,
            type(value[0]),
            self._create_upload_fn,
        )
        lt = Literal(collection=LiteralCollection([]))
        for v in value:
            click_val = converter._click_type.convert(v, param, ctx)
            lt.collection.literals.append(converter.convert_to_literal(ctx, param, click_val))
        return lt

    def convert_to_map(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: dict
    ) -> Literal:
        """
        Convert a python dict into a Flyte Literal.
        It is assumed that the click parameter type is a JsonParamType. The map is also assumed to be univariate.
        """
        if not value:
            raise click.BadParameter("Expected non-empty dict")
        if not isinstance(value, dict):
            raise click.BadParameter(f"Expected json dict '{{...}}', parsed value is {type(value)}")
        converter = FlyteLiteralConverter(
            ctx,
            self._flyte_ctx,
            self._literal_type.map_value_type,
            type(value[list(value.keys())[0]]),
            self._create_upload_fn,
        )
        lt = Literal(map=LiteralMap({}))
        for k, v in value.items():
            click_val = converter._click_type.convert(v, param, ctx)
            lt.map.literals[k] = converter.convert_to_literal(ctx, param, click_val)
        return lt

    def convert_to_struct(
        self,
        ctx: typing.Optional[click.Context],
        param: typing.Optional[click.Parameter],
        value: typing.Union[dict, typing.Any],
    ) -> Literal:
        """
        Convert the loaded json object to a Flyte Literal struct type.
        """
        if type(value) != self._python_type:
            o = cast(DataClassJsonMixin, self._python_type).from_json(json.dumps(value))
        else:
            o = value
        return TypeEngine.to_literal(self._flyte_ctx, o, self._python_type, self._literal_type)

    def convert_to_literal(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: typing.Any
    ) -> Literal:
        if self._literal_type.structured_dataset_type:
            return self.convert_to_structured_dataset(ctx, param, value)

        if self._literal_type.blob:
            return self.convert_to_blob(ctx, param, value)

        if self._literal_type.collection_type:
            return self.convert_to_list(ctx, param, value)

        if self._literal_type.map_value_type:
            return self.convert_to_map(ctx, param, value)

        if self._literal_type.union_type:
            return self.convert_to_union(ctx, param, value)

        if self._literal_type.simple or self._literal_type.enum_type:
            if self._literal_type.simple and self._literal_type.simple == SimpleType.STRUCT:
                return self.convert_to_struct(ctx, param, value)
            return Literal(scalar=self._converter.convert(value, self._python_type))

        if self._literal_type.schema:
            raise DeprecationWarning("Schema Types are not supported in pyflyte run. Use StructuredDataset instead.")

        raise NotImplementedError(
            f"CLI parsing is not available for Python Type:`{self._python_type}`, LiteralType:`{self._literal_type}`."
        )

    def convert(self, ctx, param, value) -> typing.Union[Literal, typing.Any]:
        try:
            lit = self.convert_to_literal(ctx, param, value)
            if not self._remote:
                return TypeEngine.to_python_value(self._flyte_ctx, lit, self._python_type)
            return lit
        except click.BadParameter:
            raise
        except Exception as e:
            raise click.BadParameter(f"Failed to convert param {param}, {value} to {self._python_type}") from e


def to_click_option(
    ctx: click.Context,
    flyte_ctx: FlyteContext,
    input_name: str,
    literal_var: Variable,
    python_type: typing.Type,
    default_val: typing.Any,
    get_upload_url_fn: typing.Callable,
) -> click.Option:
    """
    This handles converting workflow input types to supported click parameters with callbacks to initialize
    the input values to their expected types.
    """
    literal_converter = FlyteLiteralConverter(
        ctx, flyte_ctx, literal_type=literal_var.type, python_type=python_type, get_upload_url_fn=get_upload_url_fn
    )

    if literal_converter.is_bool() and not default_val:
        default_val = False

    if literal_var.type.simple == SimpleType.STRUCT:
        if default_val:
            if type(default_val) == dict or type(default_val) == list:
                default_val = json.dumps(default_val)
            else:
                default_val = cast(DataClassJsonMixin, default_val).to_json()

    return click.Option(
        param_decls=[f"--{input_name}"],
        type=literal_converter.click_type,
        is_flag=literal_converter.is_bool(),
        default=default_val,
        show_default=True,
        required=default_val is None,
        help=literal_var.description,
        callback=literal_converter.convert,
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
            param_decls=["--copy-all", "copy_all"],
            required=False,
            is_flag=True,
            default=False,
            help="Copy all files in the source root directory to the destination directory",
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
            help="Whether to wait for the execution to finish",
        ),
        click.Option(
            param_decls=["--dump-snippet", "dump_snippet"],
            required=False,
            is_flag=True,
            default=False,
            help="Whether to dump a code snippet instructing how to load the workflow execution using flyteremote",
        ),
        click.Option(
            param_decls=["--overwrite-cache", "overwrite_cache"],
            required=False,
            is_flag=True,
            default=False,
            help="Whether to overwrite the cache if it already exists",
        ),
        click.Option(
            param_decls=["--envs", "envs"],
            required=False,
            type=JsonParamType(),
            help="Environment variables to set in the container",
        ),
    ]


def load_naive_entity(module_name: str, entity_name: str, project_root: str) -> typing.Union[WorkflowBase, PythonTask]:
    """
    Load the workflow of a script file.
    N.B.: it assumes that the file is self-contained, in other words, there are no relative imports.
    """
    flyte_ctx_builder = context_manager.FlyteContextManager.current_context().new_builder()
    with context_manager.FlyteContextManager.with_context(flyte_ctx_builder):
        with module_loader.add_sys_path(project_root):
            importlib.import_module(module_name)
    return module_loader.load_object_from_module(f"{module_name}.{entity_name}")


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


class Entities(typing.NamedTuple):
    """
    NamedTuple to group all entities in a file
    """

    workflows: typing.List[str]
    tasks: typing.List[str]

    def all(self) -> typing.List[str]:
        e = []
        e.extend(self.workflows)
        e.extend(self.tasks)
        return e


def get_entities_in_file(filename: str) -> Entities:
    """
    Returns a list of flyte workflow names and list of Flyte tasks in a file.
    """
    flyte_ctx = context_manager.FlyteContextManager.current_context().new_builder()
    module_name = os.path.splitext(os.path.relpath(filename))[0].replace(os.path.sep, ".")
    with context_manager.FlyteContextManager.with_context(flyte_ctx):
        with module_loader.add_sys_path(os.getcwd()):
            importlib.import_module(module_name)

    workflows = []
    tasks = []
    module = importlib.import_module(module_name)
    for name in dir(module):
        o = module.__dict__[name]
        if isinstance(o, WorkflowBase):
            workflows.append(name)
        elif isinstance(o, PythonTask):
            tasks.append(name)

    return Entities(workflows, tasks)


def run_command(ctx: click.Context, entity: typing.Union[PythonFunctionWorkflow, PythonTask]):
    """
    Returns a function that is used to implement WorkflowCommand and execute a flyte workflow.
    """

    def _run(*args, **kwargs):
        # By the time we get to this function, all the loading has already happened

        run_level_params = ctx.obj[RUN_LEVEL_PARAMS_KEY]
        project, domain = run_level_params.get("project"), run_level_params.get("domain")
        inputs = {}
        for input_name, _ in entity.python_interface.inputs.items():
            inputs[input_name] = kwargs.get(input_name)

        if not ctx.obj[REMOTE_FLAG_KEY]:
            output = entity(**inputs)
            click.echo(output)
            return

        remote = ctx.obj[FLYTE_REMOTE_INSTANCE_KEY]
        config_file = ctx.obj.get(CTX_CONFIG_FILE)

        image_config = run_level_params.get("image_config")
        image_config = patch_image_config(config_file, image_config)

        remote_entity = remote.register_script(
            entity,
            project=project,
            domain=domain,
            image_config=image_config,
            destination_dir=run_level_params.get("destination_dir"),
            source_path=ctx.obj[RUN_LEVEL_PARAMS_KEY].get(CTX_PROJECT_ROOT),
            module_name=ctx.obj[RUN_LEVEL_PARAMS_KEY].get(CTX_MODULE),
            copy_all=ctx.obj[RUN_LEVEL_PARAMS_KEY].get(CTX_COPY_ALL),
        )

        options = None
        service_account = run_level_params.get("service_account")
        if service_account:
            # options are only passed for the execution. This is to prevent errors when registering a duplicate workflow
            # It is assumed that the users expectations is to override the service account only for the execution
            options = Options.default_from(k8s_service_account=service_account)

        execution = remote.execute(
            remote_entity,
            inputs=inputs,
            project=project,
            domain=domain,
            name=run_level_params.get("name"),
            wait=run_level_params.get("wait_execution"),
            options=options,
            type_hints=entity.python_interface.inputs,
            overwrite_cache=run_level_params.get("overwrite_cache"),
            envs=run_level_params.get("envs"),
        )

        console_url = remote.generate_console_url(execution)
        click.secho(f"Go to {console_url} to see execution in the console.")

        if run_level_params.get("dump_snippet"):
            dump_flyte_remote_snippet(execution, project, domain)

    return _run


class WorkflowCommand(click.RichGroup):
    """
    click multicommand at the python file layer, subcommands should be all the workflows in the file.
    """

    def __init__(self, filename: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = pathlib.Path(filename).resolve()

    def list_commands(self, ctx):
        entities = get_entities_in_file(self._filename)
        return entities.all()

    def get_command(self, ctx, exe_entity):
        """
        This command uses the filename with which this command was created, and the string name of the entity passed
          after the Python filename on the command line, to load the Python object, and then return the Command that
          click should run.
        :param ctx: The click Context object.
        :param exe_entity: string of the flyte entity provided by the user. Should be the name of a workflow, or task
          function.
        :return:
        """

        rel_path = os.path.relpath(self._filename)
        if rel_path.startswith(".."):
            raise ValueError(
                f"You must call pyflyte from the same or parent dir, {self._filename} not under {os.getcwd()}"
            )

        project_root = _find_project_root(self._filename)

        # Find the relative path for the filename relative to the root of the project.
        # N.B.: by construction project_root will necessarily be an ancestor of the filename passed in as
        # a parameter.
        rel_path = self._filename.relative_to(project_root)
        module = os.path.splitext(rel_path)[0].replace(os.path.sep, ".")

        ctx.obj[RUN_LEVEL_PARAMS_KEY][CTX_PROJECT_ROOT] = project_root
        ctx.obj[RUN_LEVEL_PARAMS_KEY][CTX_MODULE] = module

        entity = load_naive_entity(module, exe_entity, project_root)

        # If this is a remote execution, which we should know at this point, then create the remote object
        p = ctx.obj[RUN_LEVEL_PARAMS_KEY].get(CTX_PROJECT)
        d = ctx.obj[RUN_LEVEL_PARAMS_KEY].get(CTX_DOMAIN)
        r = get_and_save_remote_with_click_context(ctx, p, d)
        get_upload_url_fn = functools.partial(r.client.get_upload_signed_url, project=p, domain=d)

        flyte_ctx = context_manager.FlyteContextManager.current_context()

        # Add options for each of the workflow inputs
        params = []
        for input_name, input_type_val in entity.python_interface.inputs_with_defaults.items():
            literal_var = entity.interface.inputs.get(input_name)
            python_type, default_val = input_type_val
            params.append(
                to_click_option(ctx, flyte_ctx, input_name, literal_var, python_type, default_val, get_upload_url_fn)
            )
        cmd = click.Command(
            name=exe_entity,
            params=params,
            callback=run_command(ctx, entity),
            help=f"Run {module}.{exe_entity} in script mode",
        )
        return cmd


class RunCommand(click.RichGroup):
    """
    A click command group for registering and executing flyte workflows & tasks in a file.
    """

    def __init__(self, *args, **kwargs):
        params = get_workflow_command_base_params()
        super().__init__(*args, params=params, **kwargs)

    def list_commands(self, ctx):
        return [str(p) for p in pathlib.Path(".").glob("*.py") if str(p) != "__init__.py"]

    def get_command(self, ctx, filename):
        if ctx.obj:
            ctx.obj[RUN_LEVEL_PARAMS_KEY] = ctx.params
        return WorkflowCommand(filename, name=filename, help="Run a [workflow|task] in a file using script mode")


_run_help = """
This command can execute either a workflow or a task from the command line, for fully self-contained scripts.
Tasks and workflows cannot be imported from other files currently. Please use ``pyflyte package`` or
``pyflyte register`` to handle those and then launch from the Flyte UI or ``flytectl``.

Note: This command only works on regular Python packages, not namespace packages. When determining
the root of your project, it finds the first folder that does not have an ``__init__.py`` file.
"""

run = RunCommand(
    name="run",
    help=_run_help,
)
