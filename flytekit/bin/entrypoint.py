import datetime as _datetime
import importlib as _importlib
import logging as _logging
import os as _os
import pathlib
import pathlib as _pathlib
import random as _random

import click as _click
from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit.annotated.base_task import IgnoreOutputs, PythonTask
from flytekit.annotated.context_manager import ExecutionState, FlyteContext, SerializationSettings, get_image_config
from flytekit.annotated.promise import VoidPromise
from flytekit.common import constants as _constants
from flytekit.common import utils as _common_utils
from flytekit.common import utils as _utils
from flytekit.common.exceptions import scopes as _scopes
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
from flytekit.configuration import internal as _internal_config
from flytekit.configuration import platform as _platform_config
from flytekit.configuration import sdk as _sdk_config
from flytekit.engines import loader as _engine_loader
from flytekit.interfaces import random as _flyte_random
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.interfaces.data.gcs import gcs_proxy as _gcs_proxy
from flytekit.interfaces.data.s3 import s3proxy as _s3proxy
from flytekit.interfaces.stats.taggable import get_stats as _get_stats
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import literals as _literal_models
from flytekit.models.core import identifier as _identifier
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


def _dispatch_execute(ctx: FlyteContext, task_def: PythonTask, inputs_path: str, output_prefix: str):
    """
    Dispatches execute to PythonTask
        Step1: Download inputs and load into a literal map
        Step2: Invoke task - dispatch_execute
        Step3:
            a: [Optional] Record outputs to output_prefix
            b: OR if IgnoreOutputs is raised, then ignore uploading outputs
            c: OR if an unhandled exception is retrieved - record it as an errors.pb
    """
    try:
        # Step1
        local_inputs_file = _os.path.join(ctx.execution_state.working_dir, "inputs.pb")
        ctx.file_access.get_data(inputs_path, local_inputs_file)
        input_proto = _utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)
        idl_input_literals = _literal_models.LiteralMap.from_flyte_idl(input_proto)
        # Step2
        outputs = task_def.dispatch_execute(ctx, idl_input_literals)
        if isinstance(outputs, VoidPromise):
            _logging.getLogger().warning("Task produces no outputs")
            output_file_dict = {_constants.OUTPUT_FILE_NAME: _literal_models.LiteralMap(literals={})}
        elif isinstance(outputs, _literal_models.LiteralMap):
            output_file_dict = {_constants.OUTPUT_FILE_NAME: outputs}
        elif isinstance(outputs, _dynamic_job.DynamicJobSpec):
            output_file_dict = {_constants.FUTURES_FILE_NAME: outputs}
        else:
            _logging.getLogger().error(f"SystemError: received unknown outputs from task {outputs}")
            # TODO This should probably cause an error file
            return

        for k, v in output_file_dict.items():
            _common_utils.write_proto_to_file(v.to_flyte_idl(), _os.path.join(ctx.execution_state.engine_dir, k))

        # Step3a
        ctx.file_access.upload_directory(ctx.execution_state.engine_dir, output_prefix)
        _logging.info(f"Outputs written successful the the output prefix {output_prefix}")
    except Exception as e:
        if isinstance(e, IgnoreOutputs):
            # Step 3b
            _logging.warning(f"IgnoreOutputs received! Outputs.pb will not be uploaded. reason {e}")
            return
        # Step 3c
        _logging.error(f"Exception when executing task {task_def.name}, reason {str(e)}")
        raise e


def _handle_annotated_task(task_def: PythonTask, inputs: str, output_prefix: str, raw_output_data_prefix: str):
    """
    Entrypoint for all PythonTask extensions
    """
    _click.echo("Running native-typed task")
    cloud_provider = _platform_config.CLOUD_PROVIDER.get()
    log_level = _internal_config.LOGGING_LEVEL.get() or _sdk_config.LOGGING_LEVEL.get()
    _logging.getLogger().setLevel(log_level)

    ctx = FlyteContext.current_context()

    # Create directories
    user_workspace_dir = ctx.file_access.local_access.get_random_directory()
    _click.echo(f"Using user directory {user_workspace_dir}")
    pathlib.Path(user_workspace_dir).mkdir(parents=True, exist_ok=True)
    from flytekit import __version__ as _api_version

    execution_parameters = ExecutionParameters(
        execution_id=_identifier.WorkflowExecutionIdentifier(
            project=_internal_config.EXECUTION_PROJECT.get(),
            domain=_internal_config.EXECUTION_DOMAIN.get(),
            name=_internal_config.EXECUTION_NAME.get(),
        ),
        execution_date=_datetime.datetime.utcnow(),
        stats=_get_stats(
            # Stats metric path will be:
            # registration_project.registration_domain.app.module.task_name.user_stats
            # and it will be tagged with execution-level values for project/domain/wf/lp
            "{}.{}.{}.user_stats".format(
                _internal_config.TASK_PROJECT.get() or _internal_config.PROJECT.get(),
                _internal_config.TASK_DOMAIN.get() or _internal_config.DOMAIN.get(),
                _internal_config.TASK_NAME.get() or _internal_config.NAME.get(),
            ),
            tags={
                "exec_project": _internal_config.EXECUTION_PROJECT.get(),
                "exec_domain": _internal_config.EXECUTION_DOMAIN.get(),
                "exec_workflow": _internal_config.EXECUTION_WORKFLOW.get(),
                "exec_launchplan": _internal_config.EXECUTION_LAUNCHPLAN.get(),
                "api_version": _api_version,
            },
        ),
        logging=_logging,
        tmp_dir=user_workspace_dir,
    )

    if cloud_provider == _constants.CloudProvider.AWS:
        file_access = _data_proxy.FileAccessProvider(
            local_sandbox_dir=_sdk_config.LOCAL_SANDBOX.get(), remote_proxy=_s3proxy.AwsS3Proxy(raw_output_data_prefix),
        )
    elif cloud_provider == _constants.CloudProvider.GCP:
        file_access = _data_proxy.FileAccessProvider(
            local_sandbox_dir=_sdk_config.LOCAL_SANDBOX.get(), remote_proxy=_gcs_proxy.GCSProxy(raw_output_data_prefix),
        )
    elif cloud_provider == _constants.CloudProvider.LOCAL:
        # A fake remote using the local disk will automatically be created
        file_access = _data_proxy.FileAccessProvider(local_sandbox_dir=_sdk_config.LOCAL_SANDBOX.get())
    else:
        raise Exception(f"Bad cloud provider {cloud_provider}")

    with ctx.new_file_access_context(file_access_provider=file_access) as ctx:
        # TODO: This is copied from serialize, which means there's a similarity here I'm not seeing.
        env = {
            _internal_config.CONFIGURATION_PATH.env_var: _internal_config.CONFIGURATION_PATH.get(),
            _internal_config.IMAGE.env_var: _internal_config.IMAGE.get(),
        }

        serialization_settings = SerializationSettings(
            project=_internal_config.TASK_PROJECT.get(),
            domain=_internal_config.TASK_DOMAIN.get(),
            version=_internal_config.TASK_VERSION.get(),
            image_config=get_image_config(),
            env=env,
        )

        # The reason we need this is because of dynamic tasks. Even if we move compilation all to Admin,
        # if a dynamic task calls some task, t1, we have to write to the DJ Spec the correct task
        # identifier for t1.
        with ctx.new_serialization_settings(serialization_settings=serialization_settings) as ctx:
            # Because execution states do not look up the context chain, it has to be made last
            with ctx.new_execution_context(
                mode=ExecutionState.Mode.TASK_EXECUTION, execution_params=execution_parameters
            ) as ctx:
                _dispatch_execute(ctx, task_def, inputs, output_prefix)


@_scopes.system_entry_point
def _execute_task(task_module, task_name, inputs, output_prefix, raw_output_data_prefix, test):
    with _TemporaryConfiguration(_internal_config.CONFIGURATION_PATH.get()):
        with _utils.AutoDeletingTempDir("input_dir") as input_dir:
            # Load user code

            task_module = _importlib.import_module(task_module)
            task_def = getattr(task_module, task_name)

            # Everything else
            if not test and not isinstance(task_def, PythonTask):
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

            # New annotated style task
            elif not test and isinstance(task_def, PythonTask):
                _handle_annotated_task(task_def, inputs, output_prefix, raw_output_data_prefix)


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
@_click.option("--dest-dir", required=False)
@_click.argument("task-execute-cmd", nargs=-1, type=_click.UNPROCESSED)
def fast_execute_task_cmd(additional_distribution, dest_dir, task_execute_cmd):
    """
    Downloads a compressed code distribution specified by additional-distribution and then calls the underlying
    task execute command for the updated code.
    :param Text additional_distribution:
    :param Text dest_dir:
    :param task_execute_cmd:
    :return:
    """
    if additional_distribution is not None:
        if not dest_dir:
            dest_dir = _os.getcwd()
        _download_distribution(additional_distribution, _pathlib.Path(dest_dir))

    # Use the commandline to run the task execute command rather than calling it directly in python code
    # since the current runtime bytecode references the older user code, rather than the downloaded distribution.
    _os.system(" ".join(task_execute_cmd))


if __name__ == "__main__":
    _pass_through()
