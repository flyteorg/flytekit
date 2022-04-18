import functools
import importlib
import inspect
import json
import os
from dataclasses import is_dataclass, dataclass
from datetime import datetime
from typing import Callable, Optional, cast

import click
import pandas as pd
import typing
from dataclasses_json import DataClassJsonMixin

from flytekit.clis.sdk_in_container.run import get_module_and_workflow_name, _load_naive_entity, _get_workflows_in_file
from flytekit.configuration import Config, ImageConfig, SerializationSettings
from flytekit.configuration.default_images import DefaultImages
from flytekit.core import context_manager
from flytekit.core.workflow import WorkflowBase, PythonFunctionWorkflow
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

REMOTE_KEY = "remote"


def json_value_type_callback(_: typing.Any, param: str, value: str) -> typing.Dict[typing.Any, typing.Any]:
    print(f"handling {value}")
    return json.loads(value)


def dataclass_callback(input_type: type):
    def dataclass_callback_inner(_: typing.Any, param: str, value: str) -> DataClassJsonMixin:
        print(f"handling dataclass: {value}")
        dataclass_type = input_type
        return cast(DataClassJsonMixin, dataclass_type).from_json(value)
    return dataclass_callback_inner


def dataframe_callback(ctx: typing.Any, param: str, value: str) -> pd.DataFrame:
    if not ctx.obj[REMOTE_KEY]:
        return pd.read_parquet(value)


class JsonParamType(click.ParamType):
    name = "json"

    def convert(self, value, param, ctx):
        json.loads(value)


def get_option_type_and_callback(input_type: typing.Any) -> [type, typing.Callable]:
    """
    This handles converting workflow input types to supported click parameters with callbacks to initialize
    the input values correctly
    """
    if input_type is datetime:
        return click.DateTime, None
    if is_dataclass(input_type):
        print(f"handling dataclass with {dataclass_callback(input_type)}")
        return input_type, dataclass_callback(input_type)
    if is_dataclass == pd.DataFrame:
        # The input type is a dataframe filepath, so we convert the option type accordingly
        return click.Path, dataframe_callback
    if inspect.isclass(input_type):
        if issubclass(input_type, (FlyteFile, FlyteSchema, StructuredDataset, FlyteDirectory)):
            raise NotImplementedError(
                click.style(
                    "Flyte[File, Schema, Directory] & StructuredDataSet is not yet implemented in pyflyte run", fg="red"
                )
            )

    origin_type = typing.get_origin(input_type)
    if origin_type in [dict, list]:
        return str, json_value_type_callback

    # Filter through the union of types to see if any of them has a registered callback
    if origin_type is typing.Union:
        types = input_type.__args__
        for t in types:
            option_type, callback = get_option_type_and_callback(t)
            if callback is not None:
                return option_type, callback

    # By default, handle the input type without any callback. This works natively with primitives.
    return input_type, None


def foo(*args, **kwargs):
    print("hello world")


class WorkflowCommand(click.MultiCommand):
    def __init__(self, filename:str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._filename = filename

    def list_commands(self, ctx):
        print(f"filename {self._filename}")
        workflows = _get_workflows_in_file(self._filename)
        return workflows

    def get_command(self, ctx, workflow):
        module = os.path.splitext(self._filename)[0].replace(os.path.sep, ".")
        wf_entity = _load_naive_entity(module, workflow)

        params = []
        for input_name, input_type in wf_entity.python_interface.inputs.items():
            print(f"looking at input_name {input_name} and input_type {input_type}")
            # TODO , figure out required
            # add remote flag
            option_type, callback = get_option_type_and_callback(input_type)
            params.append(click.Option(param_decls=[f"--{input_name}"], type=option_type, callback=callback))
        cmd = click.Command(name=workflow, params=params, callback=foo)

        return cmd


class RunCommand(click.MultiCommand):
    def list_commands(self, ctx):
        rv = []
        return rv

    def get_command(self, ctx, filename):
        return WorkflowCommand(filename, name=filename, help="foo")


cli = RunCommand(name="run", help='This tool\'s subcommands are loaded from a '
            'plugin folder dynamically.')

if __name__ == "main":
    cli()