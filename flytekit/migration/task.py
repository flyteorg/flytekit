from __future__ import annotations

import datetime
import inspect
import os
from functools import partial, update_wrapper
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union, overload
from typing import Literal as L

from typing_extensions import ParamSpec  # type: ignore

import flytekit
from flytekit.core import launch_plan as _annotated_launchplan
from flytekit.core import workflow as _annotated_workflow
from flytekit.core.base_task import PythonTask, TaskMetadata, TaskResolverMixin
from flytekit.core.cache import Cache, VersionParameters
from flytekit.core.interface import Interface, output_name_generator, transform_function_to_interface
from flytekit.core.pod_template import PodTemplate
from flytekit.core.python_function_task import AsyncPythonFunctionTask, EagerAsyncPythonFunctionTask, PythonFunctionTask
from flytekit.core.reference_entity import ReferenceEntity, TaskReference
from flytekit.core.resources import Resources
from flytekit.core.utils import str2bool
from flytekit.deck import DeckField
from flytekit.extras.accelerators import BaseAccelerator
from flytekit.image_spec.image_spec import ImageSpec
from flytekit.interactive import vscode
from flytekit.interactive.constants import FLYTE_ENABLE_VSCODE_KEY
from flytekit.models.documentation import Documentation
from flytekit.models.security import Secret

import flyte
from flyte import Image, Resources, TaskEnvironment
from flyte._doc import Documentation
from flyte._task import AsyncFunctionTaskTemplate, P, R

P = ParamSpec("P")
T = TypeVar("T")
FuncOut = TypeVar("FuncOut")

def task_shim(
    _task_function: Optional[Callable[P, FuncOut]] = None,
    task_config: Optional[T] = None,
    cache: Union[bool, Cache] = False,
    retries: int = 0,
    interruptible: Optional[bool] = None,
    deprecated: str = "",
    timeout: Union[datetime.timedelta, int] = 0,
    container_image: Optional[Union[str, ImageSpec]] = None,
    environment: Optional[Dict[str, str]] = None,
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
    secret_requests: Optional[List[Secret]] = None,
    execution_mode: PythonFunctionTask.ExecutionBehavior = PythonFunctionTask.ExecutionBehavior.DEFAULT,
    node_dependency_hints: Optional[
        Iterable[
            Union[
                PythonFunctionTask,
                _annotated_launchplan.LaunchPlan,
                _annotated_workflow.WorkflowBase,
            ]
        ]
    ] = None,
    task_resolver: Optional[TaskResolverMixin] = None,
    docs: Optional[Documentation] = None,
    disable_deck: Optional[bool] = None,
    enable_deck: Optional[bool] = None,
    deck_fields: Optional[Tuple[DeckField, ...]] = (
        DeckField.SOURCE_CODE,
        DeckField.DEPENDENCIES,
        DeckField.TIMELINE,
        DeckField.INPUT,
        DeckField.OUTPUT,
    ),
    pod_template: Optional[PodTemplate] = None,
    pod_template_name: Optional[str] = None,
    accelerator: Optional[BaseAccelerator] = None,
    pickle_untyped: bool = False,
    shared_memory: Optional[Union[L[True], str]] = None,
    resources: Optional[Resources] = None,
    labels: Optional[dict[str, str]] = None,
    annotations: Optional[dict[str, str]] = None,
    **kwargs,
) -> Union[AsyncFunctionTaskTemplate, Callable[P, R]]:
    """
    Shim to allow using flytekit configuration to run flyte-sdk tasks.
    Converts flytekit-native config to flyte-sdk compatible task.
    """
    def wrapper(fn: Callable[P, FuncOut]) -> PythonFunctionTask[T]:
        # Convert flytekit-native types to flyte-sdk compatible types if needed
        # (This is a placeholder for actual conversion logic if required)
        # For now, we just call the main task function, but this is where bridging logic would go.
        plugin_config = task_config
        _pod_template = (
            flyte.PodTemplate(
                pod_spec=pod_template.pod_spec,
                primary_container_name=pod_template.primary_container_name,
                labels=pod_template.labels,
                annotations=pod_template.annotations,
            )
            if pod_template
            else None
        )

        if isinstance(container_image, flytekit.ImageSpec):
            image = Image.from_debian_base()
            if container_image.apt_packages:
                image = image.with_apt_packages(*container_image.apt_packages)
            pip_packages = ["flytekit"]
            if container_image.packages:
                pip_packages.extend(container_image.packages)
            image = image.with_pip_packages(*pip_packages)
        elif isinstance(container_image, str):
            image = Image.from_base(container_image).with_pip_packages("flyte")
        else:
            image = Image.from_debian_base().with_pip_packages("flytekit")

        _docs = Documentation(description=docs.short_description) if docs else None

        env = TaskEnvironment(
            name="flytekit",
            resources=resources,
            image=image,
            cache="enabled" if cache else "disable",
            plugin_config=plugin_config,
        )
        return env.task(retries=retries, pod_template=pod_template_name or _pod_template, docs=_docs)(fn)


    if _task_function is not None:
        return wrapper(_task_function)
    return wrapper
