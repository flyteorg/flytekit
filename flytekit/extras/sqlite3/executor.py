#!/usr/bin/env python

import argparse
import datetime as _datetime
import logging
import os as _os
import pathlib
import random as _random
import traceback as _traceback
from typing import List

import click as _click
from flyteidl.core import literals_pb2 as _literals_pb2
from flyteidl.core import tasks_pb2

from flytekit import PythonFunctionTask, logger
from flytekit.common import constants as _constants
from flytekit.common import utils as _common_utils
from flytekit.common import utils as _utils
from flytekit.common.exceptions import scopes as _scoped_exceptions
from flytekit.common.exceptions import scopes as _scopes
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
from flytekit.configuration import internal as _internal_config
from flytekit.configuration import platform as _platform_config
from flytekit.configuration import sdk as _sdk_config
from flytekit.core.base_task import IgnoreOutputs, PythonTask
from flytekit.core.context_manager import ExecutionState, FlyteContext, SerializationSettings, get_image_config
from flytekit.core.map_task import MapPythonTask
from flytekit.core.promise import VoidPromise
from flytekit.core.python_auto_container import TaskResolverMixin
from flytekit.engines import loader as _engine_loader
from flytekit.interfaces import random as _flyte_random
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.interfaces.data.gcs import gcs_proxy as _gcs_proxy
from flytekit.interfaces.data.s3 import s3proxy as _s3proxy
from flytekit.interfaces.stats.taggable import get_stats as _get_stats
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import literals as _literal_models
from flytekit.models.core import errors as _error_models
from flytekit.models.core import identifier as _identifier
from flytekit.tools.fast_registration import download_distribution as _download_distribution

logger.setLevel(logging.DEBUG)


def run(inputs: str, output_prefix: str, raw_output_data_prefix: str, task_template_path: str):
    # Download the task template file
    ctx = FlyteContext.current_context()
    task_template_local_path = _os.path.join(ctx.execution_state.working_dir, "task_template.pb")
    ctx.file_access.get_data(task_template_path, task_template_local_path)

    input_proto = _utils.load_proto_from_file(tasks_pb2.TaskTemplate, task_template_local_path)


def setup(inputs: str, output_prefix: str, raw_output_data_prefix: str, task_template_path: str):
    ctx = FlyteContext.current_context()
    cloud_provider = _platform_config.CLOUD_PROVIDER.get()
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
        logging=logging,
        tmp_dir=user_workspace_dir,
    )

    if cloud_provider == _constants.CloudProvider.AWS:
        file_access = _data_proxy.FileAccessProvider(
            local_sandbox_dir=_sdk_config.LOCAL_SANDBOX.get(),
            remote_proxy=_s3proxy.AwsS3Proxy(raw_output_data_prefix),
        )
    elif cloud_provider == _constants.CloudProvider.GCP:
        file_access = _data_proxy.FileAccessProvider(
            local_sandbox_dir=_sdk_config.LOCAL_SANDBOX.get(),
            remote_proxy=_gcs_proxy.GCSProxy(raw_output_data_prefix),
        )
    elif cloud_provider == _constants.CloudProvider.LOCAL:
        # A fake remote using the local disk will automatically be created
        file_access = _data_proxy.FileAccessProvider(local_sandbox_dir=_sdk_config.LOCAL_SANDBOX.get())
    else:
        raise Exception(f"Bad cloud provider {cloud_provider}")

    with ctx.new_file_access_context(file_access_provider=file_access) as ctx:
        with ctx.new_execution_context(mode=ExecutionState.Mode.TASK_EXECUTION, execution_params=execution_parameters):
            return run(inputs, output_prefix, raw_output_data_prefix, task_template_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Do the thing")
    parser.add_argument("--inputs", dest="inputs", action="store", type=str, required=True, help="inputs.pb")

    parser.add_argument(
        "--output-prefix", dest="output_prefix", action="store", type=str, required=True, help="output prefix"
    )

    parser.add_argument(
        "--raw-output-data-prefix",
        dest="raw_output_data_prefix",
        action="store",
        type=str,
        required=True,
        help="Offloaded data",
    )

    parser.add_argument(
        "--task-template-path", dest="task_template_path", action="store", type=str, required=True, help="fdsafasd"
    )

    args = parser.parse_args()
    logger.info(
        f"Parsed arguments are: inputs path {args.inputs}"
        f" outputs prefix {args.output_prefix}"
        f" offloaded data {args.raw_output_data_prefix}"
        f" task template {args.task_template_path}"
    )

    # set up the environment for execution
