import asyncio
import contextlib
import datetime
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import traceback
import uuid
import warnings
from sys import exit
from typing import Callable, Dict, List, Optional

import click
from flyteidl.core import literals_pb2 as _literals_pb2
from google.protobuf.timestamp_pb2 import Timestamp

from flytekit.configuration import (
    SERIALIZED_CONTEXT_ENV_VAR,
    FastSerializationSettings,
    ImageConfig,
    SerializationSettings,
    StatsConfig,
)
from flytekit.core import constants as _constants
from flytekit.core import utils
from flytekit.core.base_task import IgnoreOutputs, PythonTask
from flytekit.core.checkpointer import SyncCheckpoint
from flytekit.core.constants import FLYTE_FAIL_ON_ERROR
from flytekit.core.context_manager import (
    ExecutionParameters,
    ExecutionState,
    FlyteContext,
    FlyteContextManager,
    OutputMetadataTracker,
)
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.promise import VoidPromise
from flytekit.core.utils import str2bool
from flytekit.deck.deck import _output_deck
from flytekit.exceptions.base import FlyteException
from flytekit.exceptions.system import FlyteNonRecoverableSystemException
from flytekit.exceptions.user import FlyteRecoverableException, FlyteUserRuntimeException
from flytekit.interfaces.stats.taggable import get_stats as _get_stats
from flytekit.loggers import logger, user_space_logger
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import literals as _literal_models
from flytekit.models.core import errors as _error_models
from flytekit.models.core import execution as _execution_models
from flytekit.models.core import identifier as _identifier
from flytekit.tools.fast_registration import download_distribution as _download_distribution
from flytekit.tools.module_loader import load_object_from_module
from flytekit.utils.pbhash import compute_hash_string


def get_version_message():
    import flytekit

    return f"Welcome to Flyte! Version: {flytekit.__version__}"


def _compute_array_job_index():
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


def _build_error_file_name() -> str:
    """Get name of error file uploaded to the raw output prefix bucket.

    For distributed tasks, all workers upload error files which must not overwrite each other, leading to a race condition.
    A uuid is included to prevent this.

    Returns
    -------
    str
        Name of the error file.
    """
    dist_error_strategy = get_one_of("FLYTE_INTERNAL_DIST_ERROR_STRATEGY", "_F_DES")
    if not dist_error_strategy:
        return _constants.ERROR_FILE_NAME
    error_file_name_base, error_file_name_extension = os.path.splitext(_constants.ERROR_FILE_NAME)
    error_file_name_base += f"-{uuid.uuid4().hex}"
    return f"{error_file_name_base}{error_file_name_extension}"


def _get_worker_name() -> str:
    """Get the name of the worker

    For distributed tasks, the backend plugin can set a worker name to be used for error reporting.

    Returns
    -------
    str
        Name of the worker
    """
    dist_error_strategy = get_one_of("FLYTE_INTERNAL_DIST_ERROR_STRATEGY", "_F_DES")
    if not dist_error_strategy:
        return ""
    return get_one_of("FLYTE_INTERNAL_WORKER_NAME", "_F_WN")


def _get_working_loop():
    """Returns a running event loop."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            try:
                return asyncio.get_event_loop_policy().get_event_loop()
            # Since version 3.12, DeprecationWarning is emitted if there is no
            # current event loop.
            except DeprecationWarning:
                loop = asyncio.get_event_loop_policy().new_event_loop()
                asyncio.set_event_loop(loop)
                return loop


def _dispatch_execute(
    ctx: FlyteContext,
    load_task: Callable[[], PythonTask],
    inputs_path: str,
    output_prefix: str,
    is_map_task: bool = False,
):
    """
    Dispatches execute to PythonTask
        Step1: Download inputs and load into a literal map
        Step2: Invoke task - dispatch_execute
        Step3:
            a: [Optional] Record outputs to output_prefix
            b: OR if IgnoreOutputs is raised, then ignore uploading outputs
            c: OR if an unhandled exception is retrieved - record it as an errors.pb

    :param ctx: FlyteContext
    :param load_task: Callable[[], PythonTask]
    :param inputs: Where to read inputs
    :param output_prefix: Where to write primitive outputs
    :param is_map_task: Whether this task is executing as part of a map task
    """
    error_file_name = _build_error_file_name()
    worker_name = _get_worker_name()

    output_file_dict = {}

    task_def = None
    try:
        try:
            task_def = load_task()
        except Exception as e:
            # If the task can not be loaded, then it's most likely a user error. For example,
            # a dependency is not installed during execution.
            raise FlyteUserRuntimeException(e) from e

        logger.debug(f"Starting _dispatch_execute for {task_def.name}")
        # Step1
        local_inputs_file = os.path.join(ctx.execution_state.working_dir, "inputs.pb")
        ctx.file_access.get_data(inputs_path, local_inputs_file)
        input_proto = utils.load_proto_from_file(_literals_pb2.LiteralMap, local_inputs_file)
        idl_input_literals = _literal_models.LiteralMap.from_flyte_idl(input_proto)

        # Step2
        # Invoke task - dispatch_execute
        outputs = task_def.dispatch_execute(ctx, idl_input_literals)

        # Step3a
        if isinstance(outputs, VoidPromise):
            logger.warning("Task produces no outputs")
            output_file_dict = {_constants.OUTPUT_FILE_NAME: _literal_models.LiteralMap(literals={})}
        elif isinstance(outputs, _literal_models.LiteralMap):
            # The keys in this map hold the filenames to the offloaded proto literals.
            offloaded_literals: Dict[str, _literal_models.Literal] = {}
            literal_map_copy = {}

            offloading_enabled = os.environ.get("_F_L_MIN_SIZE_MB", None) is not None
            min_offloaded_size = -1
            max_offloaded_size = -1
            if offloading_enabled:
                min_offloaded_size = int(os.environ.get("_F_L_MIN_SIZE_MB", "10")) * 1024 * 1024
                max_offloaded_size = int(os.environ.get("_F_L_MAX_SIZE_MB", "1000")) * 1024 * 1024

            # Go over each output and create a separate offloaded in case its size is too large
            for k, v in outputs.literals.items():
                literal_map_copy[k] = v

                if not offloading_enabled:
                    continue

                lit = v.to_flyte_idl()
                if max_offloaded_size != -1 and lit.ByteSize() >= max_offloaded_size:
                    raise ValueError(
                        f"Literal {k} is too large to be offloaded. Max literal size is {max_offloaded_size} whereas the literal size is {lit.ByteSize()} bytes"
                    )

                if min_offloaded_size != -1 and lit.ByteSize() >= min_offloaded_size:
                    logger.debug(f"Literal {k} is too large to be inlined, offloading to metadata bucket")
                    inferred_type = task_def.interface.outputs[k].type

                    # In the case of map tasks we need to use the type of the collection as inferred type as the task
                    # typed interface of the offloaded literal. This is done because the map task interface present in
                    # the task template contains the (correct) type for the entire map task, not the single node execution.
                    # For that reason we "unwrap" the collection type and use it as the inferred type of the offloaded literal.
                    if is_map_task:
                        inferred_type = inferred_type.collection_type

                    # This file will hold the offloaded literal and will be written to the output prefix
                    # alongside the regular outputs.pb, deck.pb, etc.
                    # N.B.: by construction `offloaded_filename` is guaranteed to be unique
                    offloaded_filename = f"{k}_offloaded_metadata.pb"
                    offloaded_literal = _literal_models.Literal(
                        offloaded_metadata=_literal_models.LiteralOffloadedMetadata(
                            uri=f"{output_prefix}/{offloaded_filename}",
                            size_bytes=lit.ByteSize(),
                            # TODO: remove after https://github.com/flyteorg/flyte/pull/5909 is merged
                            inferred_type=inferred_type,
                        ),
                        hash=v.hash if v.hash is not None else compute_hash_string(lit),
                    )
                    literal_map_copy[k] = offloaded_literal
                    offloaded_literals[offloaded_filename] = v
            outputs = _literal_models.LiteralMap(literals=literal_map_copy)

            output_file_dict = {_constants.OUTPUT_FILE_NAME: outputs, **offloaded_literals}
        elif isinstance(outputs, _dynamic_job.DynamicJobSpec):
            output_file_dict = {_constants.FUTURES_FILE_NAME: outputs}
        else:
            logger.error(f"SystemError: received unknown outputs from task {outputs}")
            output_file_dict[error_file_name] = _error_models.ErrorDocument(
                _error_models.ContainerError(
                    code="UNKNOWN_OUTPUT",
                    message=f"Type of output received not handled {type(outputs)} outputs: {outputs}",
                    kind=_error_models.ContainerError.Kind.RECOVERABLE,
                    origin=_execution_models.ExecutionError.ErrorKind.SYSTEM,
                    timestamp=get_container_error_timestamp(),
                    worker=worker_name,
                )
            )

    # Handle user-scoped errors
    except FlyteUserRuntimeException as e:
        # Step3b
        if isinstance(e.value, IgnoreOutputs):
            logger.warning(f"User-scoped IgnoreOutputs received! Outputs.pb will not be uploaded. reason {e}!!")
            return

        # Step3c
        if isinstance(e.value, FlyteRecoverableException):
            kind = _error_models.ContainerError.Kind.RECOVERABLE
        else:
            kind = _error_models.ContainerError.Kind.NON_RECOVERABLE

        exc_str = get_traceback_str(e)
        output_file_dict[error_file_name] = _error_models.ErrorDocument(
            _error_models.ContainerError(
                code=e.error_code,
                message=exc_str,
                kind=kind,
                origin=_execution_models.ExecutionError.ErrorKind.USER,
                timestamp=get_container_error_timestamp(e.value),
                worker=worker_name,
            )
        )
        if task_def is not None:
            logger.error(f"Exception when executing task {task_def.name or task_def.id.name}, reason {str(e)}")
        else:
            logger.error(f"Exception when loading_task, reason {str(e)}")
        logger.error("!! Begin User Error Captured by Flyte !!")
        logger.error(exc_str)
        logger.error("!! End Error Captured by Flyte !!")

    except FlyteNonRecoverableSystemException as e:
        exc_str = get_traceback_str(e.value)
        output_file_dict[error_file_name] = _error_models.ErrorDocument(
            _error_models.ContainerError(
                code="SYSTEM",
                message=exc_str,
                kind=_error_models.ContainerError.Kind.NON_RECOVERABLE,
                origin=_execution_models.ExecutionError.ErrorKind.SYSTEM,
                timestamp=get_container_error_timestamp(e.value),
                worker=worker_name,
            )
        )

        logger.error("!! Begin Non-recoverable System Error Captured by Flyte !!")
        logger.error(exc_str)
        logger.error("!! End Error Captured by Flyte !!")

    # All other errors are captured here, and are considered system errors
    except Exception as e:
        exc_str = get_traceback_str(e)
        output_file_dict[error_file_name] = _error_models.ErrorDocument(
            _error_models.ContainerError(
                code="SYSTEM",
                message=exc_str,
                kind=_error_models.ContainerError.Kind.RECOVERABLE,
                origin=_execution_models.ExecutionError.ErrorKind.SYSTEM,
                timestamp=get_container_error_timestamp(e),
                worker=worker_name,
            )
        )

        logger.error("!! Begin Unknown System Error Captured by Flyte !!")
        logger.error(exc_str)
        logger.error("!! End Error Captured by Flyte !!")

    for k, v in output_file_dict.items():
        utils.write_proto_to_file(v.to_flyte_idl(), os.path.join(ctx.execution_state.engine_dir, k))

    ctx.file_access.put_data(ctx.execution_state.engine_dir, output_prefix, is_multipart=True)
    logger.info(f"Engine folder written successfully to the output prefix {output_prefix}")

    if task_def is not None and not getattr(task_def, "disable_deck", True):
        _output_deck(task_name=task_def.name.split(".")[-1], new_user_params=ctx.user_space_params)

    logger.debug("Finished _dispatch_execute")

    if str2bool(os.getenv(FLYTE_FAIL_ON_ERROR)) and error_file_name in output_file_dict:
        """
        If the environment variable FLYTE_FAIL_ON_ERROR is set to true, the task execution will fail if an error file is
        generated. This environment variable is set to true by the plugin author if they want the task to fail on error.
        Otherwise, the task will always succeed and just write the error file to the blob store.

        For example, you can see the task fails on Databricks or AWS batch UI by setting this environment variable to true.
        """
        exit(1)


def get_traceback_str(e: Exception) -> str:
    # First, format the exception stack trace
    root_exception = e
    if isinstance(e, FlyteUserRuntimeException):
        # If the exception is a user exception, we want to capture the traceback of the exception that was raised by the
        # user code, not the Flyte internals.
        root_exception = e.__cause__ if e.__cause__ else e
    indentation = "    "
    exception_str = textwrap.indent(
        text="".join(traceback.format_exception(type(root_exception), root_exception, root_exception.__traceback__)),
        prefix=indentation,
    )
    # Second, format a summary exception message
    value = e.value if isinstance(e, FlyteUserRuntimeException) else e
    message = f"{type(value).__name__}: {value}"
    message_str = textwrap.indent(text=message, prefix=indentation)
    # Last, create the overall traceback string
    format_str = "Trace:\n\n{exception_str}\nMessage:\n\n{message_str}"
    return format_str.format(exception_str=exception_str, message_str=message_str)


def get_container_error_timestamp(e: Optional[Exception] = None) -> Timestamp:
    """Get timestamp for ContainerError.

    If a flyte exception is passed, use its timestamp, otherwise, use the current time.

    Parameters
    ----------
    e : Exception, optional
        Exception that has occurred.

    Returns
    -------
    Timestamp
        Timestamp to be reported in ContainerError
    """
    timestamp = None
    if isinstance(e, FlyteException):
        timestamp = e.timestamp
    if timestamp is None:
        timestamp = time.time()
    timstamp_secs = int(timestamp)
    timestamp_fsecs = timestamp - timstamp_secs
    timestamp_nanos = int(timestamp_fsecs * 1_000_000_000)
    return Timestamp(seconds=timstamp_secs, nanos=timestamp_nanos)


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
    output_metadata_prefix: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    prev_checkpoint: Optional[str] = None,
    dynamic_addl_distro: Optional[str] = None,
    dynamic_dest_dir: Optional[str] = None,
):
    """

    :param raw_output_data_prefix: Where to write offloaded data (files, directories, dataframes).
    :param output_metadata_prefix: Where to write primitive outputs.
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
        execution_date=datetime.datetime.now(datetime.timezone.utc),
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
        output_metadata_prefix=output_metadata_prefix,
        checkpoint=checkpointer,
        task_id=_identifier.Identifier(_identifier.ResourceType.TASK, tk_project, tk_domain, tk_name, tk_version),
    )

    metadata = {
        "flyte-execution-project": exe_project,
        "flyte-execution-domain": exe_domain,
        "flyte-execution-launchplan": exe_lp,
        "flyte-execution-workflow": exe_wf,
        "flyte-execution-name": exe_name,
    }
    try:
        file_access = FileAccessProvider(
            local_sandbox_dir=tempfile.mkdtemp(prefix="flyte"),
            raw_output_prefix=raw_output_data_prefix,
            execution_metadata=metadata,
        )
    except TypeError:  # would be thrown from DataPersistencePlugins.find_plugin
        logger.error(f"No data plugin found for raw output prefix {raw_output_data_prefix}")
        raise

    ctx = ctx.new_builder().with_file_access(file_access).build()

    es = ctx.new_execution_state().with_params(
        mode=ExecutionState.Mode.TASK_EXECUTION,
        user_space_params=execution_parameters,
    )
    # create new output metadata tracker
    omt = OutputMetadataTracker()
    cb = ctx.new_builder().with_execution_state(es).with_output_metadata_tracker(omt)

    if compressed_serialization_settings:
        ss = SerializationSettings.from_transport(compressed_serialization_settings)
        ssb = ss.new_builder()
    else:
        ss = SerializationSettings(ImageConfig.auto())
        ssb = ss.new_builder()

    ssb.project = ssb.project or exe_project
    ssb.domain = ssb.domain or exe_domain
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
        raise ValueError("cannot be <1")

    with setup_execution(
        raw_output_data_prefix,
        output_prefix,
        checkpoint_path,
        prev_checkpoint,
        dynamic_addl_distro,
        dynamic_dest_dir,
    ) as ctx:
        working_dir = os.getcwd()
        if all(os.path.realpath(path) != working_dir for path in sys.path):
            sys.path.append(working_dir)
        resolver_obj = load_object_from_module(resolver)

        def load_task():
            # Use the resolver to load the actual task object
            return resolver_obj.load_task(loader_args=resolver_args)

        if test:
            logger.info(
                f"Test detected, returning. Args were {inputs} {output_prefix} {raw_output_data_prefix} {resolver} {resolver_args}"
            )
            return
        _dispatch_execute(ctx, load_task, inputs, output_prefix)


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
        raise ValueError(f"Resolver args cannot be <1, got {resolver_args}")

    with setup_execution(
        raw_output_data_prefix, output_prefix, checkpoint_path, prev_checkpoint, dynamic_addl_distro, dynamic_dest_dir
    ) as ctx:
        working_dir = os.getcwd()
        if all(os.path.realpath(path) != working_dir for path in sys.path):
            sys.path.append(working_dir)
        task_index = _compute_array_job_index()
        mtr = load_object_from_module(resolver)()

        def load_task():
            return mtr.load_task(loader_args=resolver_args, max_concurrency=max_concurrency)

        # Special case for the map task resolver, we need to append the task index to the output prefix.
        # TODO: (https://github.com/flyteorg/flyte/issues/5011) Remove legacy map task
        if mtr.name() == "flytekit.core.legacy_map_task.MapTaskResolver":
            output_prefix = os.path.join(output_prefix, str(task_index))

        if test:
            logger.info(
                f"Test detected, returning. Inputs: {inputs} Computed task index: {task_index} "
                f"New output prefix: {output_prefix} Raw output path: {raw_output_data_prefix} "
                f"Resolver and args: {resolver} {resolver_args}"
            )
            return

        _dispatch_execute(ctx, load_task, inputs, output_prefix, is_map_task=True)


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


@click.group()
def _pass_through():
    pass


@_pass_through.command("pyflyte-execute")
@click.option("--inputs", required=True)
@click.option("--output-prefix", required=True)
@click.option("--raw-output-data-prefix", required=False)
@click.option("--checkpoint-path", required=False)
@click.option("--prev-checkpoint", required=False)
@click.option("--test", is_flag=True)
@click.option("--dynamic-addl-distro", required=False)
@click.option("--dynamic-dest-dir", required=False)
@click.option("--resolver", required=False)
@click.argument(
    "resolver-args",
    type=click.UNPROCESSED,
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
    click.echo("")
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
@click.option("--additional-distribution", required=False)
@click.option("--dest-dir", required=False)
@click.argument("task-execute-cmd", nargs=-1, type=click.UNPROCESSED)
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
    env = os.environ.copy()
    if dest_dir is not None:
        dest_dir_resolved = os.path.realpath(os.path.expanduser(dest_dir))
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] += os.pathsep + dest_dir_resolved
        else:
            env["PYTHONPATH"] = dest_dir_resolved
    p = subprocess.Popen(cmd, env=env)

    def handle_sigterm(signum, frame):
        logger.info(f"passing signum {signum} [frame={frame}] to subprocess")
        p.send_signal(signum)

    signal.signal(signal.SIGTERM, handle_sigterm)
    returncode = p.wait()
    exit(returncode)


@_pass_through.command("pyflyte-map-execute")
@click.option("--inputs", required=True)
@click.option("--output-prefix", required=True)
@click.option("--raw-output-data-prefix", required=False)
@click.option("--max-concurrency", type=int, required=False)
@click.option("--test", is_flag=True)
@click.option("--dynamic-addl-distro", required=False)
@click.option("--dynamic-dest-dir", required=False)
@click.option("--resolver", required=True)
@click.option("--checkpoint-path", required=False)
@click.option("--prev-checkpoint", required=False)
@click.argument(
    "resolver-args",
    type=click.UNPROCESSED,
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
