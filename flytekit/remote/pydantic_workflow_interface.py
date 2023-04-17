from __future__ import annotations

import datetime
import logging
import os
import pathlib
import textwrap
import time
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel, Field, create_model
from pydantic.fields import FieldInfo, Undefined
from rich.console import Console

from flytekit.core.type_engine import LiteralsResolver, TypeEngine
from flytekit.models.interface import Parameter
from flytekit.remote.executions import FlyteWorkflowExecution
from flytekit.types.file import FlyteFile

if TYPE_CHECKING:
    from flytekit.remote import FlyteRemote


logger = logging.getLogger(__name__)

console = Console()

DEFAULT_DOCSTRING = (
    "To execute this workflow on Flyte, provide the required "
    "arguments and call `execute()`, which takes all parameters of "
    "`FlyteRemote.execute`.\n\n"
    "Workflow parameters:\n"
    "{parameter_docs}\n\n"
    "Workflow outputs:\n"
    "{output_docs}\n\n"
    "Example usage of `PydanticWorkflowModel`:\n\n"
    "Provide keyword arguments to model and call execute:\n"
    "wf(value=1).execute()"
)


def get_flyte_pydantic_model_info(
    project: str, domain: str, name: str, version: str
) -> str:
    simple_name = name.split(".")[-1]
    docs_string = (
        f"PydanticWorkflowModel ({simple_name})\n\n"
        f"Project: {project!r}\n"
        f"Domain: {domain!r}\n"
        f"Name: {name!r}\n"
        f"Version: {version!r}\n"
    )
    return docs_string


class PydanticWorkflowModel(BaseModel):
    _project: str = Field(default=None)
    _domain: str = Field(default=None)
    _name: str = Field(default=None)
    _version: str = Field(default=None)

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def _set_workflow_info(
        cls,
        project: str,
        domain: str,
        name: str,
        version: str,
        remote: FlyteRemote,
    ) -> None:
        cls._project = project
        cls._domain = domain
        cls._name = name
        cls._version = version
        cls._remote = remote

    def execute(
        self,
        wait_for_outputs: bool = True,
        execution_name: str = "",
        **execution_kwargs: Any,
    ) -> Any:
        """Execute Flyte workflow on remote.

        Args:
            wait_for_outputs: If True, wait for Flyte workflow outputs, otherwise
                return FlyteWorkflowExecution
            execution_name: Custom execution name. Defaults to "", which creates a
                randomly generated execution name.

        Kwargs:
            All arguments supported by FlyteRemote.execute can be passed as keyword
            arguments to this function.

        """
        lp = self._remote.fetch_launch_plan(
            project=self._project,
            domain=self._domain,
            name=self._name,
            version=self._version,
        )
        name_info = f"[deep_sky_blue4]Workflow name:[/deep_sky_blue4] [italic]{self._name}[/italic]"
        version_info = f"[deep_sky_blue4]Workflow version:[/deep_sky_blue4] [italic]{self._version}[/italic]"
        console.print(
            f"[bold]🛫 Launching Flyte Workflow 🛬[/bold]\n\n{name_info}\n{version_info}",
        )
        execution = self._remote.execute(
            lp, inputs=self.dict(), execution_name=execution_name, **execution_kwargs
        )
        execution_url = get_execution_url(execution)
        console.print(
            f"Track progress at the [link={execution_url}]Flyte Console[/link]"
        )
        if wait_for_outputs:
            outputs = get_flyte_workflow_execution_outputs(
                execution, log_interval=10, remote=self._remote
            )
            return outputs
        return execution


def get_default_parameter(parameter: Parameter) -> Any:
    if not parameter.default:
        raise ValueError("No default for parameter.")
    k = "param"
    default = LiteralsResolver({k: parameter.default}, {k: parameter.var}).get(k)
    if isinstance(default, FlyteFile):
        default = default.remote_source
    return default


def create_parameter_docs(params: dict[str, Any]) -> str:
    return "\n".join(
        textwrap.fill(
            f"{k}: {v.description}",
            width=80,
            initial_indent=" " * 4,
            subsequent_indent=" " * 8,
        )
        for k, v in params.items()
    )


def create_workflow_model(
    project: str, domain: str, name: str, version: str, remote: FlyteRemote
) -> PydanticWorkflowModel:
    """Creates a Pydantic model based on workflow's interface from remote."""
    lp = remote.fetch_launch_plan(project, domain, name, version)
    assert lp.interface
    model_parameters = {}
    keys_with_defaults = []
    for k, parameter in lp.default_inputs.parameters.items():
        default: Any = Undefined
        if parameter.default:
            keys_with_defaults.append(k)
            default = get_default_parameter(parameter)
        python_type = TypeEngine.guess_python_type(parameter.var.type)
        if python_type is FlyteFile:
            python_type = python_type | str | pathlib.Path
        model_parameters[k] = (
            python_type,
            FieldInfo(default, description=parameter.var.description),
        )

    model_parameters = dict(
        sorted(model_parameters.items(), key=lambda x: x[0] in keys_with_defaults)
    )
    sorted_lp_inputs = {k: lp.interface.inputs[k] for k in model_parameters}
    parameter_docs = create_parameter_docs(sorted_lp_inputs)
    output_docs = create_parameter_docs(lp.interface.outputs)
    flyte_pydantic_docs = get_flyte_pydantic_model_info(project, domain, name, version)
    model = create_model(  # type: ignore
        flyte_pydantic_docs,
        __base__=PydanticWorkflowModel,
        **model_parameters,
    )
    model._set_workflow_info(project, domain, name, version, remote=remote)
    model_parameter_docs = DEFAULT_DOCSTRING.format(
        parameter_docs=parameter_docs,
        output_docs=output_docs,
    )
    wrapped_lines = [
        textwrap.fill(line, width=80)
        for line in f"{flyte_pydantic_docs}\n{model_parameter_docs}".splitlines()
    ]
    wrapped_text = "\n".join(wrapped_lines)
    model.__doc__ = wrapped_text
    return cast(PydanticWorkflowModel, model)


def get_platform_url() -> str:
    return os.getenv("FLYTE_PLATFORM_URL", "localhost:30080")


def get_flyte_base_console_url() -> str:
    platform_url = get_platform_url()
    return os.environ.get("FLYTE_CONSOLE_BASE_URL", f"http://{platform_url}/console")


def get_execution_url(execution: FlyteWorkflowExecution) -> str:
    """Return execution URL if user previously called execute, raise otherwise."""
    base_url = get_flyte_base_console_url()
    return (
        f"{base_url}/projects/{execution.id.project}/domains/"
        f"{execution.id.domain}/executions/{execution.id.name}"
    )


def get_flyte_workflow_execution_outputs(
    execution: FlyteWorkflowExecution,
    remote: FlyteRemote,
    log_interval: int | None = None,
) -> Any:
    """Return outputs after workflow has completed."""
    finished_execution = wait_for_workflow_execution_to_finish(
        execution,
        log_interval=log_interval,
        remote=remote,
    )
    if finished_execution.outputs:
        output_keys = list(finished_execution.outputs.literals.keys())
        if len(output_keys) == 1:
            return finished_execution.outputs.get(output_keys[0])
        else:
            return tuple(finished_execution.outputs.get(key) for key in output_keys)
    else:
        raise ValueError("Workflow aborted.")


def wait_for_workflow_execution_to_finish(
    execution: FlyteWorkflowExecution,
    remote: FlyteRemote,
    log_interval: int | None = None,
) -> FlyteWorkflowExecution:
    """Wait for workflow to complete and return execution.

    Args:
        execution: FlyteWorkflowExecution object
        log_interval: Interval for logging (in seconds). Defaults to 60.

    Raises:
        ValueError: If workflow execution failed

    Returns:
        Synced workflow execution
    """
    log_interval = log_interval or 60
    start_time = time.time()
    log_idx = 0
    if execution.is_done:
        execution = remote.sync_execution(execution, sync_nodes=True)
        return execution

    logger.info("Waiting for workflow to complete before returning output.")
    while not execution.is_done:
        current_time = time.time()
        seconds_elapsed = current_time - start_time
        if seconds_elapsed >= log_interval * log_idx:
            elapsed = datetime.timedelta(seconds=seconds_elapsed)
            logger.info(f"Syncing execution. Total time elapsed: {elapsed}.")
            log_idx += 1
        execution = remote.sync_execution(execution, sync_nodes=True)
        time.sleep(5)
    if execution.error:
        raise ValueError(execution.closure.error.message)
    elapsed = datetime.timedelta(seconds=time.time() - start_time)
    logger.info(f"Workflow completed. Total time elapsed: {elapsed}")
    return execution
