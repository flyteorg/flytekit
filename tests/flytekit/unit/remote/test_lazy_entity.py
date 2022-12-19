import pytest
from mock import patch

from flytekit import TaskMetadata
from flytekit.core import context_manager
from flytekit.models.core.identifier import Identifier, ResourceType
from flytekit.models.interface import TypedInterface
from flytekit.remote import FlyteTask
from flytekit.remote.lazy_entity import LazyEntity


def test_missing_getter():
    with pytest.raises(ValueError):
        LazyEntity("x", None)


dummy_task = FlyteTask(
    id=Identifier(ResourceType.TASK, "p", "d", "n", "v"),
    type="t",
    metadata=TaskMetadata().to_taskmetadata_model(),
    interface=TypedInterface(inputs={}, outputs={}),
    custom=None,
)


def test_lazy_loading():
    o = FlyteTask(
        id=Identifier(ResourceType.TASK, "p", "d", "n", "v"),
        type="t",
        metadata=TaskMetadata().to_taskmetadata_model(),
        interface=TypedInterface(inputs={}, outputs={}),
        custom=None,
    )

    once = True

    def _getter():
        nonlocal once
        if not once:
            raise ValueError("Should be called once only")
        once = False
        return dummy_task

    e = LazyEntity("x", _getter)
    assert e.name == "x"
    assert e._entity is None
    v = e.entity
    assert e._entity is not None
    assert v == dummy_task
    assert e.entity == dummy_task


@patch("flytekit.remote.remote_callable.create_and_link_node_from_remote")
def test_lazy_loading_compile(create_and_link_node_from_remote_mock):
    once = True

    def _getter():
        nonlocal once
        if not once:
            raise ValueError("Should be called once only")
        once = False
        return dummy_task

    e = LazyEntity("x", _getter)
    assert e.name == "x"
    assert e._entity is None
    ctx = context_manager.FlyteContext.current_context()
    e.compile(ctx)
    assert e._entity is not None
    assert e.entity == dummy_task
