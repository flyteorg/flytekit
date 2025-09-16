from __future__ import annotations
import datetime
from typing import Callable, Dict, Iterable, List, Literal, Optional, Tuple, Union, Any

import flytekit
from flytekit.core import launch_plan, workflow
from flytekit.core.base_task import T, TaskResolverMixin
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.task import FuncOut
from flytekit.deck import DeckField
from flytekit.extras.accelerators import BaseAccelerator

from flyte import Image, Resources, TaskEnvironment
from flyte._doc import Documentation as V2Docs
from flyte._task import AsyncFunctionTaskTemplate, P, R


def _to_v2_resources(req: Optional[flytekit.Resources], lim: Optional[flytekit.Resources]) -> Optional[Resources]:
    if not req and not lim:
        return None
    # Pick requests first, then fall back to limits if requests missing.
    def pick(getter: Callable[[flytekit.Resources], Any], fallback_getter: Callable[[flytekit.Resources], Any]):
        if req and getter(req) is not None:
            return getter(req)
        if lim and fallback_getter(lim) is not None:
            return fallback_getter(lim)
        return None

    cpu = pick(lambda r: r.cpu, lambda r: r.cpu)
    mem = pick(lambda r: r.mem, lambda r: r.mem)
    gpu = pick(lambda r: r.gpu, lambda r: r.gpu)
    # Flyte-SDK Resources accepts cpu as float/str, memory as str like "800Mi"
    return Resources(cpu=cpu, memory=mem, gpu=gpu)


def _to_v2_image(container_image: Optional[Union[str, flytekit.ImageSpec]]) -> Image:
    if isinstance(container_image, flytekit.ImageSpec):
        img = Image.from_debian_base()
        if container_image.apt_packages:
            img = img.with_apt_packages(*container_image.apt_packages)
        pip_packages = []
        pip_packages.append("flyte")
        pip_packages.append("flytekit")
        if container_image.packages:
            pip_packages.extend(container_image.packages)
        return img.with_pip_packages(*pip_packages)
    if isinstance(container_image, str):
        return Image.from_base(container_image).with_pip_packages("flyte", "flytekit")
    # default
    return Image.from_debian_base().with_pip_packages("flyte", "flytekit")


def task_shim(
    _task_function: Optional[Callable[P, FuncOut]] = None,
    task_config: Optional[T] = None,
    cache: Union[bool, flytekit.Cache] = False,
    retries: int = 0,
    interruptible: Optional[bool] = None,
    deprecated: str = "",
    timeout: Union[datetime.timedelta, int] = 0,
    container_image: Optional[Union[str, flytekit.ImageSpec]] = None,
    environment: Optional[Dict[str, str]] = None,
    requests: Optional[flytekit.Resources] = None,
    limits: Optional[flytekit.Resources] = None,
    secret_requests: Optional[List[flytekit.Secret]] = None,
    execution_mode: PythonFunctionTask.ExecutionBehavior = PythonFunctionTask.ExecutionBehavior.DEFAULT,
    node_dependency_hints: Optional[
        Iterable[
            Union[
                flytekit.PythonFunctionTask,
                launch_plan.LaunchPlan,
                workflow.WorkflowBase,
            ]
        ]
    ] = None,
    task_resolver: Optional[TaskResolverMixin] = None,
    docs: Optional[flytekit.Documentation] = None,
    disable_deck: Optional[bool] = None,
    enable_deck: Optional[bool] = None,
    deck_fields: Optional[Tuple[DeckField, ...]] = (
        DeckField.SOURCE_CODE,
        DeckField.DEPENDENCIES,
        DeckField.TIMELINE,
        DeckField.INPUT,
        DeckField.OUTPUT,
    ),
    pod_template: Optional[flytekit.PodTemplate] = None,
    pod_template_name: Optional[str] = None,
    accelerator: Optional[BaseAccelerator] = None,
    pickle_untyped: bool = False,
    shared_memory: Optional[Union[Literal[True], str]] = None,
    resources: Optional[Resources] = None,   # explicit v2 resources passthrough
    labels: Optional[dict[str, str]] = None,
    annotations: Optional[dict[str, str]] = None,
    **kwargs,
) -> Union[AsyncFunctionTaskTemplate, Callable[[Callable[P, R]], AsyncFunctionTaskTemplate]]:
    """
    Decorator that mimics flytekit.task but registers a Flyte 2 task under the hood.
    Returns a decorator if called with no function; otherwise returns the wrapped task.
    """

    # Build V2 image/resources
    image = _to_v2_image(container_image)
    if resources is None:
        resources = _to_v2_resources(requests, limits)

    v2_docs = V2Docs(description=getattr(docs, "short_description", None)) if docs else None

    # cache mapping
    cache_mode: Literal["enabled", "disabled"]
    cache_mode = "enabled" if (cache is True or str(cache).lower() == "true") else "disabled"

    # PodTemplate passthrough: prefer name, else object
    pod_tpl = pod_template_name or (pod_template and pod_template.pod_spec and pod_template) or None

    env = TaskEnvironment(
        name="flytekit",
        resources=resources or Resources(cpu=0.8, memory="800Mi"),
        image=image,
        cache=cache_mode,
        plugin_config=task_config,
        env=environment,
    )

    def _decorator(fn: Callable[P, R]) -> AsyncFunctionTaskTemplate:
        # You can add retries, timeout, accelerator, secrets mapping here as needed
        return env.task(
            retries=retries,
            pod_template=pod_tpl,
            docs=v2_docs,
            timeout=timeout if isinstance(timeout, int) else int(timeout.total_seconds()) if timeout else 0,
            # You may add accelerator, secrets, interruptible, etc. when Flyte 2 exposes them
        )(fn)

    # Support both @task and @task()
    if _task_function is not None:
        return _decorator(_task_function)
    return _decorator
