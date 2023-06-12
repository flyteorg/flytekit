from __future__ import annotations

import pathlib
import textwrap
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from flytekitplugins.pydantic_remote.remote_utils import (
    get_execution_url,
    get_flyte_workflow_execution_outputs,
)
from pydantic import BaseModel, Field, create_model
from pydantic.fields import FieldInfo, Undefined
from rich.console import Console

from flytekit.core.type_engine import LiteralsResolver, TypeEngine
from flytekit.models.interface import Parameter
from flytekit.remote.entities import FlyteLaunchPlan
from flytekit.types.file import FlyteFile

if TYPE_CHECKING:
    from flytekit.remote import FlyteRemote


console = Console()


class PydanticWorkflowInterface(BaseModel):
    _project: str = Field(default=None)
    _domain: str = Field(default=None)
    _name: str = Field(default=None)
    _version: str = Field(default=None)
    _other_versions: list[str] = Field(default=None)
    _remote: FlyteRemote = Field(default=None)

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def _set_workflow_info(
        cls,
        project: str,
        domain: str,
        name: str,
        version: str,
        other_versions: list[str],
        remote: FlyteRemote,
    ) -> None:
        cls._project = project
        cls._domain = domain
        cls._name = name
        cls._version = version
        cls._other_versions = other_versions
        cls._remote = remote

    @classmethod
    def get_other_versions(cls) -> list[str]:
        return cls._other_versions

    @classmethod
    def load_version(cls, version: str) -> PydanticWorkflowInterface:
        return create_pydantic_workflow_interface(
            cls._project, cls._domain, cls._name, version, cls._remote
        )

    def execute(
        self,
        wait_for_outputs: bool = True,
        **execution_kwargs: Any,
    ) -> Any:
        """Execute Flyte workflow on remote.

        Args:
            wait_for_outputs: If True, wait for Flyte workflow outputs, otherwise
                return FlyteWorkflowExecution

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
        execution = self._remote.execute(lp, inputs=self.dict(), **execution_kwargs)
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


def create_pydantic_interface_docstring(
    lp: FlyteLaunchPlan, lp_inputs: dict[str, Any], lp_outputs: dict[str, Any]
) -> str:
    simple_name = lp.id.name.split(".")[-1]
    parameter_docs = create_parameter_docs(lp_inputs)
    output_docs = create_parameter_docs(lp_outputs)
    model_docs = (
        f"PydanticWorkflowInterface ({simple_name})\n\n"
        f"Project: {lp.id.project!r}\n"
        f"Domain: {lp.id.domain!r}\n"
        f"Name: {lp.id.name!r}\n"
        f"Version: {lp.id.version!r}\n\n"
        "To execute this workflow on Flyte, provide the required "
        "arguments and call `execute()`, which takes all parameters of "
        "`FlyteRemote.execute`.\n\n"
        "Workflow parameters:\n"
        f"{parameter_docs}\n\n"
        "Workflow outputs:\n"
        f"{output_docs}\n\n"
        "Example usage of `PydanticWorkflowInterface`:\n\n"
        "Provide keyword arguments to model and call execute:\n"
        "wf(value=1).execute()\n\n"
        "Other versions of this workflow can be accessed using `get_other_versions()` "
        "and `load_version()` class methods."
    )
    wrapped_lines = [textwrap.fill(line, width=80) for line in model_docs.splitlines()]
    wrapped_text = "\n".join(wrapped_lines)
    return wrapped_text


def get_default_parameter(parameter: Parameter) -> Any:
    if not parameter.default:
        raise ValueError("No default for parameter.")
    k = "param"
    default = LiteralsResolver({k: parameter.default}, {k: parameter.var}).get(k)
    if isinstance(default, FlyteFile):
        default = default.remote_source
    return default


def create_pydantic_workflow_interface(
    project: str,
    domain: str,
    name: str,
    version: str,
    remote: FlyteRemote,
    other_versions: Optional[list[str]] = None,
) -> PydanticWorkflowInterface:
    """Creates a Pydantic model based on workflow's interface from remote."""
    lp = remote.fetch_launch_plan(project, domain, name, version)
    assert lp.interface

    # Iterate through LaunchPlan, extract parameters with defaults which will be used
    # to sort parameters when creating the Pydantic model.
    model_parameters = {}
    keys_with_defaults = []
    for k, parameter in lp.default_inputs.parameters.items():
        default: Any = Undefined
        if parameter.default:
            keys_with_defaults.append(k)
            default = get_default_parameter(parameter)
        python_type = TypeEngine.guess_python_type(parameter.var.type)
        if python_type is FlyteFile:
            python_type = Union[python_type, str, pathlib.Path]  # type: ignore
        model_parameters[k] = (
            python_type,
            FieldInfo(default, description=parameter.var.description),
        )
    model_parameters = dict(
        sorted(model_parameters.items(), key=lambda x: x[0] in keys_with_defaults)
    )

    sorted_lp_inputs = {k: lp.interface.inputs[k] for k in model_parameters}
    model_docstring = create_pydantic_interface_docstring(
        lp=lp,
        lp_inputs=sorted_lp_inputs,
        lp_outputs=lp.interface.outputs,
    )
    repr_name = model_docstring.splitlines()[0]
    model = create_model(  # type: ignore
        repr_name, __base__=PydanticWorkflowInterface, **model_parameters
    )
    model.__doc__ = model_docstring
    model._set_workflow_info(
        project,
        domain,
        name,
        version,
        remote=remote,
        other_versions=other_versions or [],
    )
    return cast(PydanticWorkflowInterface, model)
