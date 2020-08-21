from __future__ import absolute_import

import importlib as _importlib
import os as _os

import click as _click
import datetime as _datetime
import random as _random
from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.common import utils as _utils
from flytekit.common.exceptions import scopes as _scopes, system as _system_exceptions
from flytekit.configuration import internal as _internal_config, TemporaryConfiguration as _TemporaryConfiguration
from flytekit.engines import loader as _engine_loader
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.interfaces import random as _flyte_random
from flytekit.models import literals as _literal_models


@_scopes.system_entry_point
def _execute_task(task_module, task_name, output_prefix, test, sagemaker_args):
    with _TemporaryConfiguration(_internal_config.CONFIGURATION_PATH.get()):
        with _utils.AutoDeletingTempDir('input_dir') as input_dir:
            # Load user code
            task_module = _importlib.import_module(task_module)
            task_def = getattr(task_module, task_name)


            if not test:
                local_inputs_file = input_dir.get_named_tempfile('inputs.pb')

                # TODO: parse the unknown arguments, and create a litealmap out from the task definition to replace these two lines:
                # _data_proxy.Data.get_data(inputs, local_inputs_file)
                # input_proto = _utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)

                input_proto = createLiteralMap(sagemaker_args, task_def)
                _engine_loader.get_engine().get_task(task_def).execute(
                    _literal_models.LiteralMap.from_flyte_idl(input_proto),
                    context={'output_prefix': output_prefix}
                )


@_click.group()
def _pass_through():
    pass


@_pass_through.command('pyflyte-execute-alternative', context_settings=dict(ignore_unknown_options=True))
@_click.option('--task-module', required=True)
@_click.option('--task-name', required=True)
@_click.option('--output-prefix', required=True)
@_click.option('--test', is_flag=True)
@_click.argument('sagemaker_args', nargs=-1, type=_click.UNPROCESSED)
def execute_task_cmd(task_module, task_name, output_prefix, test, sagemaker_args):
    _click.echo(_utils.get_version_message())
    _click.echo('unknown_args : {}'.format(sagemaker_args))
    _click.echo(type(sagemaker_args))
    # _execute_task(task_module, task_name, inputs, output_prefix, test, sagemaker_args)


if __name__ == '__main__':
    _pass_through()