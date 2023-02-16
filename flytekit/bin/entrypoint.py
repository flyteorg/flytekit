import contextlib
import datetime as _datetime
import os
import pathlib
import subprocess
import tempfile
import traceback as _traceback
from typing import List, Optional

import click as _click
from flyteidl.core import literals_pb2 as _literals_pb2

from flytekit import PythonFunctionTask
from flytekit.configuration import (
    SERIALIZED_CONTEXT_ENV_VAR,
    FastSerializationSettings,
    SerializationSettings,
    StatsConfig,
)
from flytekit.core import constants as _constants
from flytekit.core import utils
from flytekit.core.base_task import IgnoreOutputs, PythonTask
from flytekit.core.checkpointer import SyncCheckpoint
from flytekit.core.context_manager import ExecutionParameters, ExecutionState, FlyteContext, FlyteContextManager
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.map_task import MapPythonTask
from flytekit.core.promise import VoidPromise
from flytekit.exceptions import scopes as _scoped_exceptions
from flytekit.exceptions import scopes as _scopes
from flytekit.interfaces.stats.taggable import get_stats as _get_stats
from flytekit.loggers import entrypoint_logger as logger
from flytekit.loggers import user_space_logger
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import literals as _literal_models
from flytekit.models.core import errors as _error_models
from flytekit.models.core import execution as _execution_models
from flytekit.models.core import identifier as _identifier
from flytekit.tools.fast_registration import download_distribution as _download_distribution
from flytekit.tools.module_loader import load_object_from_module


def get_version_message():
    import flytekit

    return f"Welcome to Flyte! Version: {flytekit.__version__}"


def _compute_array_job_index():
    # type () -> int
    """
    Computes the absolute index of the current array job. This is determined by summing the compute-environment-specific
    environment variable and the offset (if one's set). The offset will be set and used when the user request that the
    job runs in a number of slots less than the size of the input.
    :rtype: int
    """
    offset = 0
    if os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET"):
        offset = int(os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET"))
    if os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME"):
        return offset + int(os.environ.get(os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME")))
    return offset


def _dispatch_execute(
    ctx: FlyteContext,
    task_def: PythonTask,
    inputs_path: str,
    output_prefix: str,
):
    """
    Dispatches execute to PythonTask
        Step1: Download inputs and load into a literal map
        Step2: Invoke task - dispatch_execute
        Step3:
            a: [Optional] Record outputs to output_prefix
            b: OR if IgnoreOutputs is raised, then ignore uploading outputs
            c: OR if an unhandled exception is retrieved - record it as an errors.pb
    """
    output_file_dict = {}
    logger.debug(f"Starting _dispatch_execute for {task_def.name}")
    try:
        # Step1
        local_inputs_file = os.path.join(ctx.execution_state.working_dir, "inputs.pb")
        ctx.file_access.get_data(inputs_path, local_inputs_file)
        input_proto = utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)
        idl_input_literals = _literal_models.LiteralMap.from_flyte_idl(input_proto)

        # Step2
        # Decorate the dispatch execute function before calling it, this wraps all exceptions into one
        # of the FlyteScopedExceptions
        outputs = _scoped_exceptions.system_entry_point(task_def.dispatch_execute)(ctx, idl_input_literals)
        # Step3a
        if isinstance(outputs, VoidPromise):
            logger.warning("Task produces no outputs")
            output_file_dict = {_constants.OUTPUT_FILE_NAME: _literal_models.LiteralMap(literals={})}
        elif isinstance(outputs, _literal_models.LiteralMap):
            output_file_dict = {_constants.OUTPUT_FILE_NAME: outputs}
        elif isinstance(outputs, _dynamic_job.DynamicJobSpec):
            output_file_dict = {_constants.FUTURES_FILE_NAME: outputs}
        else:
            logger.error(f"SystemError: received unknown outputs from task {outputs}")
            output_file_dict[_constants.ERROR_FILE_NAME] = _error_models.ErrorDocument(
                _error_models.ContainerError(
                    "UNKNOWN_OUTPUT",
                    f"Type of output received not handled {type(outputs)} outputs: {outputs}",
                    _error_models.ContainerError.Kind.RECOVERABLE,
                    _execution_models.ExecutionError.ErrorKind.SYSTEM,
                )
            )

    # Handle user-scoped errors
    except _scoped_exceptions.FlyteScopedUserException as e:
        if isinstance(e.value, IgnoreOutputs):
            logger.warning(f"User-scoped IgnoreOutputs received! Outputs.pb will not be uploaded. reason {e}!!")
            return
        output_file_dict[_constants.ERROR_FILE_NAME] = _error_models.ErrorDocument(
            _error_models.ContainerError(
                e.error_code, e.verbose_message, e.kind, _execution_models.ExecutionError.ErrorKind.USER
            )
        )
        logger.error("!! Begin User Error Captured by Flyte !!")
        logger.error(e.verbose_message)
        logger.error("!! End Error Captured by Flyte !!")

    # Handle system-scoped errors
    except _scoped_exceptions.FlyteScopedSystemException as e:
        if isinstance(e.value, IgnoreOutputs):
            logger.warning(f"System-scoped IgnoreOutputs received! Outputs.pb will not be uploaded. reason {e}!!")
            return
        output_file_dict[_constants.ERROR_FILE_NAME] = _error_models.ErrorDocument(
            _error_models.ContainerError(
                e.error_code, e.verbose_message, e.kind, _execution_models.ExecutionError.ErrorKind.SYSTEM
            )
        )
        logger.error("!! Begin System Error Captured by Flyte !!")
        logger.error(e.verbose_message)
        logger.error("!! End Error Captured by Flyte !!")

    # Interpret all other exceptions (some of which may be caused by the code in the try block outside of
    # dispatch_execute) as recoverable system exceptions.
    except Exception as e:
        # Step 3c
        exc_str = _traceback.format_exc()
        output_file_dict[_constants.ERROR_FILE_NAME] = _error_models.ErrorDocument(
            _error_models.ContainerError(
                "SYSTEM:Unknown",
                exc_str,
                _error_models.ContainerError.Kind.RECOVERABLE,
                _execution_models.ExecutionError.ErrorKind.SYSTEM,
            )
        )
        logger.error(f"Exception when executing task {task_def.name or task_def.id.name}, reason {str(e)}")
        logger.error("!! Begin Unknown System Error Captured by Flyte !!")
        logger.error(exc_str)
        logger.error("!! End Error Captured by Flyte !!")

    for k, v in output_file_dict.items():
        utils.write_proto_to_file(v.to_flyte_idl(), os.path.join(ctx.execution_state.engine_dir, k))

    ctx.file_access.put_data(ctx.execution_state.engine_dir, output_prefix, is_multipart=True)
    logger.info(f"Engine folder written successfully to the output prefix {output_prefix}")
    logger.debug("Finished _dispatch_execute")

    if os.environ.get("FLYTE_FAIL_ON_ERROR", "").lower() == "true" and _constants.ERROR_FILE_NAME in output_file_dict:
        # This env is set by the flytepropeller
        # AWS batch job get the status from the exit code, so once we catch the error,
        # we should return the error code here
        exit(1)


def get_one_of(*args) -> str:
    """
    Helper function to iterate through a series of different environment variables. This function exists because for
    some settings reference multiple environment variables for legacy reasons.
    :param args: List of environment variables to look for.
    :return: The first defined value in the environment, or an empty string if nothing is found.
    """
    for k in args:
        if k in os.environ:
            return os.environ[k]
    return ""


@contextlib.contextmanager
def setup_execution(
    raw_output_data_prefix: str,
    checkpoint_path: Optional[str] = None,
    prev_checkpoint: Optional[str] = None,
    dynamic_addl_distro: Optional[str] = None,
    dynamic_dest_dir: Optional[str] = None,
):
    """

    :param raw_output_data_prefix:
    :param checkpoint_path:
    :param prev_checkpoint:
    :param dynamic_addl_distro: Works in concert with the other dynamic arg. If present, indicates that if a dynamic
      task were to run, it should set fast serialize to true and use these values in FastSerializationSettings
    :param dynamic_dest_dir: See above.
    :return:
    """
    exe_project = get_one_of("FLYTE_INTERNAL_EXECUTION_PROJECT", "_F_PRJ")
    exe_domain = get_one_of("FLYTE_INTERNAL_EXECUTION_DOMAIN", "_F_DM")
    exe_name = get_one_of("FLYTE_INTERNAL_EXECUTION_ID", "_F_NM")
    exe_wf = get_one_of("FLYTE_INTERNAL_EXECUTION_WORKFLOW", "_F_WF")
    exe_lp = get_one_of("FLYTE_INTERNAL_EXECUTION_LAUNCHPLAN", "_F_LP")

    tk_project = get_one_of("FLYTE_INTERNAL_TASK_PROJECT", "_F_TK_PRJ")
    tk_domain = get_one_of("FLYTE_INTERNAL_TASK_DOMAIN", "_F_TK_DM")
    tk_name = get_one_of("FLYTE_INTERNAL_TASK_NAME", "_F_TK_NM")
    tk_version = get_one_of("FLYTE_INTERNAL_TASK_VERSION", "_F_TK_V")

    compressed_serialization_settings = os.environ.get(SERIALIZED_CONTEXT_ENV_VAR, "")

    ctx = FlyteContextManager.current_context()
    # Create directories
    user_workspace_dir = ctx.file_access.get_random_local_directory()
    logger.info(f"Using user directory {user_workspace_dir}")
    pathlib.Path(user_workspace_dir).mkdir(parents=True, exist_ok=True)
    from flytekit import __version__ as _api_version

    checkpointer = None
    if checkpoint_path is not None:
        checkpointer = SyncCheckpoint(checkpoint_dest=checkpoint_path, checkpoint_src=prev_checkpoint)
        logger.debug(f"Checkpointer created with source {prev_checkpoint} and dest {checkpoint_path}")

    execution_parameters = ExecutionParameters(
        execution_id=_identifier.WorkflowExecutionIdentifier(
            project=exe_project,
            domain=exe_domain,
            name=exe_name,
        ),
        execution_date=_datetime.datetime.utcnow(),
        stats=_get_stats(
            cfg=StatsConfig.auto(),
            # Stats metric path will be:
            # registration_project.registration_domain.app.module.task_name.user_stats
            # and it will be tagged with execution-level values for project/domain/wf/lp
            prefix=f"{tk_project}.{tk_domain}.{tk_name}.user_stats",
            tags={
                "exec_project": exe_project,
                "exec_domain": exe_domain,
                "exec_workflow": exe_wf,
                "exec_launchplan": exe_lp,
                "api_version": _api_version,
            },
        ),
        logging=user_space_logger,
        tmp_dir=user_workspace_dir,
        raw_output_prefix=raw_output_data_prefix,
        checkpoint=checkpointer,
        task_id=_identifier.Identifier(_identifier.ResourceType.TASK, tk_project, tk_domain, tk_name, tk_version),
    )

    try:
        file_access = FileAccessProvider(
            local_sandbox_dir=tempfile.mkdtemp(prefix="flyte"),
            raw_output_prefix=raw_output_data_prefix,
        )
    except TypeError:  # would be thrown from DataPersistencePlugins.find_plugin
        logger.error(f"No data plugin found for raw output prefix {raw_output_data_prefix}")
        raise

    es = ctx.new_execution_state().with_params(
        mode=ExecutionState.Mode.TASK_EXECUTION,
        user_space_params=execution_parameters,
    )
    cb = ctx.new_builder().with_file_access(file_access).with_execution_state(es)

    if compressed_serialization_settings:
        ss = SerializationSettings.from_transport(compressed_serialization_settings)
        ssb = ss.new_builder()
        ssb.project = exe_project
        ssb.domain = exe_domain
        ssb.version = tk_version
        if dynamic_addl_distro:
            ssb.fast_serialization_settings = FastSerializationSettings(
                enabled=True,
                destination_dir=dynamic_dest_dir,
                distribution_location=dynamic_addl_distro,
            )
        cb = cb.with_serialization_settings(ssb.build())

    with FlyteContextManager.with_context(cb) as ctx:
        yield ctx


def _handle_annotated_task(
    ctx: FlyteContext,
    task_def: PythonTask,
    inputs: str,
    output_prefix: str,
):
    """
    Entrypoint for all PythonTask extensions
    """
    _dispatch_execute(ctx, task_def, inputs, output_prefix)


@_scopes.system_entry_point
def _execute_task(
    inputs: str,
    output_prefix: str,
    test: bool,
    raw_output_data_prefix: str,
    resolver: str,
    resolver_args: List[str],
    checkpoint_path: Optional[str] = None,
    prev_checkpoint: Optional[str] = None,
    dynamic_addl_distro: Optional[str] = None,
    dynamic_dest_dir: Optional[str] = None,
):
    """
    This function should be called for new API tasks (those only available in 0.16 and later that leverage Python
    native typing).

    resolver should be something like:
        flytekit.core.python_auto_container.default_task_resolver
    resolver args should be something like
        task_module app.workflows task_name task_1
    have dashes seems to mess up click, like --task_module seems to interfere

    :param inputs: Where to read inputs
    :param output_prefix: Where to write primitive outputs
    :param raw_output_data_prefix: Where to write offloaded data (files, directories, dataframes).
    :param test: Dry run
    :param resolver: The task resolver to use. This needs to be loadable directly from importlib (and thus cannot be
      nested).
    :param resolver_args: Args that will be passed to the aforementioned resolver's load_task function
    :param dynamic_addl_distro: In the case of parent tasks executed using the 'fast' mode this captures where the
        compressed code archive has been uploaded.
    :param dynamic_dest_dir: In the case of parent tasks executed using the 'fast' mode this captures where compressed
        code archives should be installed in the flyte task container.
    :return:
    """
    if len(resolver_args) < 1:
        raise Exception("cannot be <1")

    with setup_execution(
        raw_output_data_prefix,
        checkpoint_path,
        prev_checkpoint,
        dynamic_addl_distro,
        dynamic_dest_dir,
    ) as ctx:
        resolver_obj = load_object_from_module(resolver)
        # Use the resolver to load the actual task object
        _task_def = resolver_obj.load_task(loader_args=resolver_args)
        if test:
            logger.info(
                f"Test detected, returning. Args were {inputs} {output_prefix} {raw_output_data_prefix} {resolver} {resolver_args}"
            )
            return
        _handle_annotated_task(ctx, _task_def, inputs, output_prefix)


@_scopes.system_entry_point
def _execute_map_task(
    inputs,
    output_prefix,
    raw_output_data_prefix,
    max_concurrency,
    test,
    resolver: str,
    resolver_args: List[str],
    checkpoint_path: Optional[str] = None,
    prev_checkpoint: Optional[str] = None,
    dynamic_addl_distro: Optional[str] = None,
    dynamic_dest_dir: Optional[str] = None,
):
    """
    This function should be called by map task and aws-batch task
    resolver should be something like:
        flytekit.core.python_auto_container.default_task_resolver
    resolver args should be something like
        task_module app.workflows task_name task_1
    have dashes seems to mess up click, like --task_module seems to interfere

    :param inputs: Where to read inputs
    :param output_prefix: Where to write primitive outputs
    :param raw_output_data_prefix: Where to write offloaded data (files, directories, dataframes).
    :param test: Dry run
    :param resolver: The task resolver to use. This needs to be loadable directly from importlib (and thus cannot be
      nested).
    :param resolver_args: Args that will be passed to the aforementioned resolver's load_task function
    :return:
    """
    if len(resolver_args) < 1:
        raise Exception(f"Resolver args cannot be <1, got {resolver_args}")

    with setup_execution(
        raw_output_data_prefix, checkpoint_path, prev_checkpoint, dynamic_addl_distro, dynamic_dest_dir
    ) as ctx:
        resolver_obj = load_object_from_module(resolver)
        # Use the resolver to load the actual task object
        _task_def = resolver_obj.load_task(loader_args=resolver_args)
        if not isinstance(_task_def, PythonFunctionTask):
            raise Exception("Map tasks cannot be run with instance tasks.")
        map_task = MapPythonTask(_task_def, max_concurrency)

        task_index = _compute_array_job_index()
        output_prefix = os.path.join(output_prefix, str(task_index))

        if test:
            logger.info(
                f"Test detected, returning. Inputs: {inputs} Computed task index: {task_index} "
                f"New output prefix: {output_prefix} Raw output path: {raw_output_data_prefix} "
                f"Resolver and args: {resolver} {resolver_args}"
            )
            return

        _handle_annotated_task(ctx, map_task, inputs, output_prefix)


def normalize_inputs(
    raw_output_data_prefix: Optional[str], checkpoint_path: Optional[str], prev_checkpoint: Optional[str]
):
    # Backwards compatibility - if Propeller hasn't filled this in, then it'll come through here as the original
    # template string, so let's explicitly set it to None so that the downstream functions will know to fall back
    # to the original shard formatter/prefix config.
    if raw_output_data_prefix == "{{.rawOutputDataPrefix}}":
        raw_output_data_prefix = None
    if checkpoint_path == "{{.checkpointOutputPrefix}}":
        checkpoint_path = None
    if prev_checkpoint == "{{.prevCheckpointPrefix}}" or prev_checkpoint == "" or prev_checkpoint == '""':
        prev_checkpoint = None

    return raw_output_data_prefix, checkpoint_path, prev_checkpoint


@_click.group()
def _pass_through():
    pass


@_pass_through.command("pyflyte-execute")
@_click.option("--inputs", required=True)
@_click.option("--output-prefix", required=True)
@_click.option("--raw-output-data-prefix", required=False)
@_click.option("--checkpoint-path", required=False)
@_click.option("--prev-checkpoint", required=False)
@_click.option("--test", is_flag=True)
@_click.option("--dynamic-addl-distro", required=False)
@_click.option("--dynamic-dest-dir", required=False)
@_click.option("--resolver", required=False)
@_click.argument(
    "resolver-args",
    type=_click.UNPROCESSED,
    nargs=-1,
)
def execute_task_cmd(
    inputs,
    output_prefix,
    raw_output_data_prefix,
    test,
    prev_checkpoint,
    checkpoint_path,
    dynamic_addl_distro,
    dynamic_dest_dir,
    resolver,
    resolver_args,
):
    logger.info(get_version_message())
    # We get weird errors if there are no click echo messages at all, so emit an empty string so that unit tests pass.
    _click.echo("")
    raw_output_data_prefix, checkpoint_path, prev_checkpoint = normalize_inputs(
        raw_output_data_prefix, checkpoint_path, prev_checkpoint
    )

    # For new API tasks (as of 0.16.x), we need to call a different function.
    # Use the presence of the resolver to differentiate between old API tasks and new API tasks
    # The addition of a new top-level command seemed out of scope at the time of this writing to pursue given how
    # pervasive this top level command already (plugins mostly).

    logger.debug(f"Running task execution with resolver {resolver}...")
    _execute_task(
        inputs=inputs,
        output_prefix=output_prefix,
        raw_output_data_prefix=raw_output_data_prefix,
        test=test,
        resolver=resolver,
        resolver_args=resolver_args,
        dynamic_addl_distro=dynamic_addl_distro,
        dynamic_dest_dir=dynamic_dest_dir,
        checkpoint_path=checkpoint_path,
        prev_checkpoint=prev_checkpoint,
    )


@_pass_through.command("pyflyte-fast-execute")
@_click.option("--additional-distribution", required=False)
@_click.option("--dest-dir", required=False)
@_click.argument("task-execute-cmd", nargs=-1, type=_click.UNPROCESSED)
def fast_execute_task_cmd(additional_distribution: str, dest_dir: str, task_execute_cmd: List[str]):
    """
    Downloads a compressed code distribution specified by additional-distribution and then calls the underlying
    task execute command for the updated code.
    """
    if additional_distribution is not None:
        if not dest_dir:
            dest_dir = os.getcwd()
        _download_distribution(additional_distribution, dest_dir)

    # Insert the call to fast before the unbounded resolver args
    cmd = []
    for arg in task_execute_cmd:
        if arg == "--resolver":
            cmd.extend(["--dynamic-addl-distro", additional_distribution, "--dynamic-dest-dir", dest_dir])
        cmd.append(arg)

    # Use the commandline to run the task execute command rather than calling it directly in python code
    # since the current runtime bytecode references the older user code, rather than the downloaded distribution.
    subprocess.run(cmd, check=True)


@_pass_through.command("pyflyte-map-execute")
@_click.option("--inputs", required=True)
@_click.option("--output-prefix", required=True)
@_click.option("--raw-output-data-prefix", required=False)
@_click.option("--max-concurrency", type=int, required=False)
@_click.option("--test", is_flag=True)
@_click.option("--dynamic-addl-distro", required=False)
@_click.option("--dynamic-dest-dir", required=False)
@_click.option("--resolver", required=True)
@_click.option("--checkpoint-path", required=False)
@_click.option("--prev-checkpoint", required=False)
@_click.argument(
    "resolver-args",
    type=_click.UNPROCESSED,
    nargs=-1,
)
def map_execute_task_cmd(
    inputs,
    output_prefix,
    raw_output_data_prefix,
    max_concurrency,
    test,
    dynamic_addl_distro,
    dynamic_dest_dir,
    resolver,
    resolver_args,
    prev_checkpoint,
    checkpoint_path,
):
    logger.info(get_version_message())

    raw_output_data_prefix, checkpoint_path, prev_checkpoint = normalize_inputs(
        raw_output_data_prefix, checkpoint_path, prev_checkpoint
    )

    _execute_map_task(
        inputs=inputs,
        output_prefix=output_prefix,
        raw_output_data_prefix=raw_output_data_prefix,
        max_concurrency=max_concurrency,
        test=test,
        dynamic_addl_distro=dynamic_addl_distro,
        dynamic_dest_dir=dynamic_dest_dir,
        resolver=resolver,
        resolver_args=resolver_args,
        checkpoint_path=checkpoint_path,
        prev_checkpoint=prev_checkpoint,
    )


if __name__ == "__main__":
    _pass_through()
