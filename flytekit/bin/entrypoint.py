import datetime as _datetime
import importlib as _importlib
import os as _os
import pathlib as _pathlib
import random as _random

import click as _click
from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.common import utils as _utils
from flytekit.common.exceptions import scopes as _scopes
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
from flytekit.configuration import internal as _internal_config
from flytekit.engines import loader as _engine_loader
from flytekit.interfaces import random as _flyte_random
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import literals as _literal_models
from flytekit.tools.fast_registration import download_distribution as _download_distribution


def _compute_array_job_index():
    # type () -> int
    """
    Computes the absolute index of the current array job. This is determined by summing the compute-environment-specific
    environment variable and the offset (if one's set). The offset will be set and used when the user request that the
    job runs in a number of slots less than the size of the input.
    :rtype: int
    """
    offset = 0
    if _os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET"):
        offset = int(_os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET"))
    return offset + int(_os.environ.get(_os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME")))


def _map_job_index_to_child_index(local_input_dir, datadir, index):
    local_lookup_file = local_input_dir.get_named_tempfile("indexlookup.pb")
    idx_lookup_file = _os.path.join(datadir, "indexlookup.pb")

    # if the indexlookup.pb does not exist, then just return the index
    if not _data_proxy.Data.data_exists(idx_lookup_file):
        return index

    _data_proxy.Data.get_data(idx_lookup_file, local_lookup_file)
    mapping_proto = _utils.load_proto_from_file(_literals_pb2.LiteralCollection, local_lookup_file)
    if len(mapping_proto.literals) < index:
        raise _system_exceptions.FlyteSystemAssertion(
            "dynamic task index lookup array size: {} is smaller than lookup index {}".format(
                len(mapping_proto.literals), index
            )
        )
    return mapping_proto.literals[index].scalar.primitive.integer


@_scopes.system_entry_point
def _execute_task(task_module, task_name, inputs, output_prefix, raw_output_data_prefix, test):
    with _TemporaryConfiguration(_internal_config.CONFIGURATION_PATH.get()):
        with _utils.AutoDeletingTempDir("input_dir") as input_dir:
            # Load user code

            task_module = _importlib.import_module(task_module)
            task_def = getattr(task_module, task_name)

            if not test:
                local_inputs_file = input_dir.get_named_tempfile("inputs.pb")

                # Handle inputs/outputs for array job.
                if _os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME"):
                    job_index = _compute_array_job_index()

                    # TODO: Perhaps remove.  This is a workaround to an issue we perceived with limited entropy in
                    # TODO: AWS batch array jobs.
                    _flyte_random.seed_flyte_random(
                        "{} {} {}".format(_random.random(), _datetime.datetime.utcnow(), job_index)
                    )

                    # If an ArrayTask is discoverable, the original job index may be different than the one specified in
                    # the environment variable. Look up the correct input/outputs in the index lookup mapping file.
                    job_index = _map_job_index_to_child_index(input_dir, inputs, job_index)

                    inputs = _os.path.join(inputs, str(job_index), "inputs.pb")
                    output_prefix = _os.path.join(output_prefix, str(job_index))

                _data_proxy.Data.get_data(inputs, local_inputs_file)
                input_proto = _utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)

                _engine_loader.get_engine().get_task(task_def).execute(
                    _literal_models.LiteralMap.from_flyte_idl(input_proto),
                    context={"output_prefix": output_prefix, "raw_output_data_prefix": raw_output_data_prefix},
                )


@_click.group()
def _pass_through():
    pass


_task_module_option = _click.option("--task-module", required=True)
_task_name_option = _click.option("--task-name", required=True)
_inputs_option = _click.option("--inputs", required=True)
_output_prefix_option = _click.option("--output-prefix", required=True)
_raw_output_date_prefix_option = _click.option("--raw-output-data-prefix", required=False)
_test = _click.option("--test", is_flag=True)


@_pass_through.command("pyflyte-execute")
@_click.option("--task-module", required=True)
@_click.option("--task-name", required=True)
@_click.option("--inputs", required=True)
@_click.option("--output-prefix", required=True)
@_click.option("--raw-output-data-prefix", required=False)
@_click.option("--test", is_flag=True)
def execute_task_cmd(task_module, task_name, inputs, output_prefix, raw_output_data_prefix, test):
    _click.echo(_utils.get_version_message())
    # Backwards compatibility - if Propeller hasn't filled this in, then it'll come through here as the original
    # template string, so let's explicitly set it to None so that the downstream functions will know to fall back
    # to the original shard formatter/prefix config.
    if raw_output_data_prefix == "{{.rawOutputDataPrefix}}":
        raw_output_data_prefix = None

    _execute_task(task_module, task_name, inputs, output_prefix, raw_output_data_prefix, test)


@_pass_through.command("pyflyte-fast-execute")
@_click.option("--additional-distribution", required=False)
@_click.argument("task-execute-cmd", nargs=-1, type=_click.UNPROCESSED)
def fast_execute_task_cmd(additional_distribution, task_execute_cmd):
    """
    Downloads a compressed code distribution specified by additional-distribution and then calls the underlying
    task execute command for the updated code.
    :param Text additional_distribution:
    :param task_execute_cmd:
    :return:
    """
    if additional_distribution is not None:
        _download_distribution(additional_distribution, _pathlib.Path(_os.getcwd()))

    # Use the commandline to run the task execute command rather than calling it directly in python code
    # since the current runtime bytecode references the older user code, rather than the downloaded distribution.
    _os.system(" ".join(task_execute_cmd))


if __name__ == "__main__":
    _pass_through()
