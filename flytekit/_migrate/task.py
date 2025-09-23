import datetime
from typing import Callable, Dict, Iterable, List, Literal, Optional, Tuple, Union, TYPE_CHECKING


import flyte
from flyte import Image, Resources, TaskEnvironment
from flyte._doc import Documentation
from flyte._task import AsyncFunctionTaskTemplate, P, R

import flytekit

if TYPE_CHECKING:
    from flytekit import Cache, Resources, Secret, ImageSpec, Documentation, PodTemplate
    from flytekit.core.base_task import T, TaskResolverMixin
    from flytekit.core.python_function_task import PythonFunctionTask
    from flytekit.core.task import FuncOut
    from flytekit.deck import DeckField
    from flytekit.extras.accelerators import BaseAccelerator


def task_shim(
    _task_function: Optional[Callable[P, "FuncOut"]] = None,
    task_config: Optional["T"] = None,
    cache: Union[bool, "Cache"] = False,
    retries: int = 0,
    interruptible: Optional[bool] = None,
    deprecated: str = "",
    timeout: Union[datetime.timedelta, int] = 0,
    container_image: Optional[Union[str, "ImageSpec"]] = None,
    environment: Optional[Dict[str, str]] = None,
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
    secret_requests: Optional[List["Secret"]] = None,
    docs: Optional["Documentation"] = None,
    disable_deck: Optional[bool] = None,
    enable_deck: Optional[bool] = None,
    pod_template: Optional["PodTemplate"] = None,
    pod_template_name: Optional[str] = None,
    accelerator: Optional["BaseAccelerator"] = None,
    pickle_untyped: bool = False,
    shared_memory: Optional[Union[Literal[True], str]] = None,
    resources: Optional[Resources] = None,
    labels: Optional[dict[str, str]] = None,
    annotations: Optional[dict[str, str]] = None,
    **kwargs,
) -> Union[AsyncFunctionTaskTemplate, Callable[P, R]]:
    plugin_config = task_config
    pod_template = (
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

    docs = Documentation(description=docs.short_description) if docs else None

    env = TaskEnvironment(
        name="flytekit_task",
        resources=Resources(cpu=0.8, memory="800Mi"),
        image=image,
        cache="auto" if cache else "disable",
        plugin_config=plugin_config,
    )
    return env.task(retries=retries, pod_template=pod_template_name or pod_template, docs=docs)


flytekit.task = task_shim
