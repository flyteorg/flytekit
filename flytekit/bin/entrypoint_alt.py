from __future__ import absolute_import

import importlib as _importlib

import click as _click

import flytekit.common.types.helpers as _type_helpers
from flytekit.common import utils as _utils
from flytekit.common.exceptions import scopes as _scopes
from flytekit.configuration import internal as _internal_config, TemporaryConfiguration as _TemporaryConfiguration
from flytekit.engines import loader as _engine_loader


@_scopes.system_entry_point
def _execute_task(task_module, task_name, output_prefix, test, sagemaker_args):
    with _TemporaryConfiguration(_internal_config.CONFIGURATION_PATH.get()):
        # Load user code
        task_module = _importlib.import_module(task_module)
        task_def = getattr(task_module, task_name)

        if not test:

            # Parse the unknown arguments, and create a litealmap out from the task definition
            map_of_input_values = {}
            # Here we have an assumption that each option key will come with a value right after the key
            for i in range(0, len(sagemaker_args), 2):
                # Since the sagemaker_args are unprocessed, each of the option keys comes with a leading "--"
                # We need to remove them
                map_of_input_values[sagemaker_args[i][2:]] = sagemaker_args[i+1]

            # TODO: we might need to do some special handling of the blob-typed inputs, i.e., read them from predefined
            #       locations in the container

            map_of_sdk_types = {}
            for k, v in task_def.interface.inputs.items():
                # map_of_literal_types[k] = v.type
                map_of_sdk_types[k] = _type_helpers.get_sdk_type_from_literal_type(v.type)

            input_literal_map = _type_helpers.pack_python_string_map_to_literal_map(
                map_of_input_values,
                map_of_sdk_types,
            )

            _engine_loader.get_engine().get_task(task_def).execute(
                input_literal_map,
                context={'output_prefix': output_prefix}
            )


@_click.group()
def _pass_through():
    pass


# pyflyte-execute-alt is an alternative pyflyte entrypoint specifically designed for SageMaker (currently)
# This entrypoint assumes no --inputs command-line option, and therefore it doesn't accept the input.pb file
# All the inputs will be passed into the entrypoint as unknown arguments
@_pass_through.command('pyflyte-execute-alt', context_settings=dict(ignore_unknown_options=True))
@_click.option('--task-module', required=True)
@_click.option('--task-name', required=True)
@_click.option('--output-prefix', required=True)
@_click.option('--test', is_flag=True)
@_click.argument('sagemaker_args', nargs=-1, type=_click.UNPROCESSED)
def execute_task_cmd(task_module, task_name, output_prefix, test, sagemaker_args):
    _click.echo(_utils.get_version_message())
    _click.echo('sagemaker_args : {}'.format(sagemaker_args))
    # Note that the unknown arguments are entirely unprocessed, so the leading "--" are still there
    _execute_task(task_module, task_name, output_prefix, test, sagemaker_args)


if __name__ == '__main__':
    _pass_through()
