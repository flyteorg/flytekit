from __future__ import annotations

import datetime as _datetime
from functools import update_wrapper
from typing import Any, Callable, Dict, Iterable, List, Optional, Type, TypeVar, Union, overload

from flytekit.core import launch_plan as _annotated_launchplan
from flytekit.core import workflow as _annotated_workflow
from flytekit.core.base_task import TaskMetadata, TaskResolverMixin
from flytekit.core.interface import transform_function_to_interface
from flytekit.core.pod_template import PodTemplate
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.reference_entity import ReferenceEntity, TaskReference
from flytekit.core.resources import Resources
from flytekit.extras.accelerators import BaseAccelerator
from flytekit.image_spec.image_spec import ImageSpec
from flytekit.models.documentation import Documentation
from flytekit.models.security import Secret


# @hamersaw added
from dataclasses import dataclass, field
from flytekit.core.task import TaskPlugins


@dataclass
class Actor:
    backlog_length: Optional[int] = None
    parallelism: Optional[int] = None
    replica_count: Optional[int] = None
    ttl_seconds: Optional[int] = None

class ActorTask(PythonFunctionTask[Actor]):
    _ACTOR_TASK_TYPE = "fast-task"

    def __init__(self, task_config: Actor, task_function: Callable, **kwargs):
        super(ActorTask, self).__init__(
            task_config=task_config,
            task_type=self._ACTOR_TASK_TYPE,
            task_function=task_function,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Optional[Dict[str, Any]]:
        """
        Serialize the `dask` task config into a dict.

        :param settings: Current serialization settings
        :return: Dictionary representation of the dask task config.
        """
        return {
            "id":"todo", # pod names must be lowercase
            "type":"fast-task",
            "spec":{
                "backlog_length": self.task_config.backlog_length,
                "parallelism":    self.task_config.parallelism,
                "replica_count":  self.task_config.replica_count,
                "ttl_seconds":    self.task_config.ttl_seconds,
            },
        }


# Inject the `actor` plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(Actor, ActorTask)


@overload
def actor(
    _task_function: None = ...,
    task_config: Optional[T] = ...,
    cache: bool = ...,
    cache_serialize: bool = ...,
    cache_version: str = ...,
    retries: int = ...,
    interruptible: Optional[bool] = ...,
    deprecated: str = ...,
    timeout: Union[_datetime.timedelta, int] = ...,
    container_image: Optional[Union[str, ImageSpec]] = ...,
    environment: Optional[Dict[str, str]] = ...,
    requests: Optional[Resources] = ...,
    limits: Optional[Resources] = ...,
    secret_requests: Optional[List[Secret]] = ...,
    execution_mode: PythonFunctionTask.ExecutionBehavior = ...,
    node_dependency_hints: Optional[
        Iterable[Union[PythonFunctionTask, _annotated_launchplan.LaunchPlan, _annotated_workflow.WorkflowBase]]
    ] = ...,
    task_resolver: Optional[TaskResolverMixin] = ...,
    docs: Optional[Documentation] = ...,
    disable_deck: Optional[bool] = ...,
    enable_deck: Optional[bool] = ...,
    pod_template: Optional["PodTemplate"] = ...,
    pod_template_name: Optional[str] = ...,
    accelerator: Optional[BaseAccelerator] = ...,
) -> Callable[[Callable[..., FuncOut]], PythonFunctionTask[T]]:
    ...


@overload
def actor(
    _task_function: Callable[..., FuncOut],
    task_config: Optional[T] = ...,
    cache: bool = ...,
    cache_serialize: bool = ...,
    cache_version: str = ...,
    retries: int = ...,
    interruptible: Optional[bool] = ...,
    deprecated: str = ...,
    timeout: Union[_datetime.timedelta, int] = ...,
    container_image: Optional[Union[str, ImageSpec]] = ...,
    environment: Optional[Dict[str, str]] = ...,
    requests: Optional[Resources] = ...,
    limits: Optional[Resources] = ...,
    secret_requests: Optional[List[Secret]] = ...,
    execution_mode: PythonFunctionTask.ExecutionBehavior = ...,
    node_dependency_hints: Optional[
        Iterable[Union[PythonFunctionTask, _annotated_launchplan.LaunchPlan, _annotated_workflow.WorkflowBase]]
    ] = ...,
    task_resolver: Optional[TaskResolverMixin] = ...,
    docs: Optional[Documentation] = ...,
    disable_deck: Optional[bool] = ...,
    enable_deck: Optional[bool] = ...,
    pod_template: Optional["PodTemplate"] = ...,
    pod_template_name: Optional[str] = ...,
    accelerator: Optional[BaseAccelerator] = ...,
) -> Union[PythonFunctionTask[T], Callable[..., FuncOut]]:
    ...


def actor(
    _task_function: Optional[Callable[..., FuncOut]] = None,
    backlog_length: Optional[int] = None,
    parallelism: Optional[int] = None,
    replica_count: Optional[int] = None,
    ttl_seconds: Optional[int] = None,
    cache: bool = False,
    cache_serialize: bool = False,
    cache_version: str = "",
    retries: int = 0,
    interruptible: Optional[bool] = None,
    deprecated: str = "",
    timeout: Union[_datetime.timedelta, int] = 0,
    container_image: Optional[Union[str, ImageSpec]] = None,
    environment: Optional[Dict[str, str]] = None,
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
    secret_requests: Optional[List[Secret]] = None,
    execution_mode: PythonFunctionTask.ExecutionBehavior = PythonFunctionTask.ExecutionBehavior.DEFAULT,
    node_dependency_hints: Optional[
        Iterable[Union[PythonFunctionTask, _annotated_launchplan.LaunchPlan, _annotated_workflow.WorkflowBase]]
    ] = None,
    task_resolver: Optional[TaskResolverMixin] = None,
    docs: Optional[Documentation] = None,
    disable_deck: Optional[bool] = None,
    enable_deck: Optional[bool] = None,
    pod_template: Optional["PodTemplate"] = None,
    pod_template_name: Optional[str] = None,
    accelerator: Optional[BaseAccelerator] = None,
) -> Union[Callable[[Callable[..., FuncOut]], PythonFunctionTask[T]], PythonFunctionTask[T], Callable[..., FuncOut]]:

    def wrapper(fn: Callable[..., Any]) -> PythonFunctionTask[T]:
        _metadata = TaskMetadata(
            cache=cache,
            cache_serialize=cache_serialize,
            cache_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            timeout=timeout,
        )

        task_config=Actor(
            backlog_length=backlog_length,
            parallelism=parallelism,
            replica_count=replica_count,
            ttl_seconds=ttl_seconds,
        )

        task_instance = TaskPlugins.find_pythontask_plugin(type(task_config))(
            task_config,
            fn,
            metadata=_metadata,
            container_image=container_image,
            environment=environment,
            requests=requests,
            limits=limits,
            secret_requests=secret_requests,
            execution_mode=execution_mode,
            node_dependency_hints=node_dependency_hints,
            task_resolver=task_resolver,
            disable_deck=disable_deck,
            enable_deck=enable_deck,
            docs=docs,
            pod_template=pod_template,
            pod_template_name=pod_template_name,
            accelerator=accelerator,
        )
        update_wrapper(task_instance, fn)
        return task_instance

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper

