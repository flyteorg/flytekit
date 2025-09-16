from __future__ import annotations

from typing import Any, Callable, Optional

from flyte import Image, Resources, TaskEnvironment
from flyte._doc import Documentation as V2Docs


def workflow_shim(
    _fn: Optional[Callable[..., Any]] = None,
    *,
    image: Optional[Image] = None,
    resources: Optional[Resources] = None,
    docs: Optional[str] = None,
):
    """
    Simple replacement for @flytekit.workflow that wraps the Python function
    as a Flyte 2 task (pure-Python orchestration).
    """
    env = TaskEnvironment(
        name="flytekit",
        resources=resources or Resources(cpu=0.8, memory="800Mi"),
        image=image or Image.from_debian_base().with_pip_packages("flyte", "flytekit"),
    )
    v2_docs = V2Docs(description=docs) if docs else None

    def _decorator(fn: Callable[..., Any]):
        # Turn the "workflow" into a task that calls user code directly.
        # In Flyte 2 you orchestrate with Python (loops/await/gather inside fn).
        return env.task(docs=v2_docs)(fn)

    if _fn is not None:
        return _decorator(_fn)
    return _decorator
