import typing
from collections import OrderedDict

import pytest

from flytekit.common.exceptions.user import FlyteValidationException, FlyteValueException
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.condition import conditional
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.core.task import task
from flytekit.core.workflow import WorkflowFailurePolicy, WorkflowMetadata, WorkflowMetadataDefaults, workflow


def test_metadata_values():
    with pytest.raises(FlyteValidationException):
        WorkflowMetadata(on_failure=0)

    wm = WorkflowMetadata(on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY)
    assert wm.on_failure == WorkflowFailurePolicy.FAIL_IMMEDIATELY


def test_default_metadata_values():
    with pytest.raises(FlyteValidationException):
        WorkflowMetadataDefaults(3)

    wm = WorkflowMetadataDefaults(interruptible=False)
    assert wm.interruptible is False


def test_workflow_values():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @workflow(interruptible=True, failure_policy=WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE)
    def wf(a: int) -> (str, str):
        x, y = t1(a=a)
        u, v = t1(a=x)
        return y, v

    serialization_settings = context_manager.SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
        env={},
    )
    sdk_wf = get_serializable(OrderedDict(), serialization_settings, wf)
    assert sdk_wf.metadata_defaults.interruptible
    assert sdk_wf.metadata.on_failure == 1


def test_default_values():
    @task
    def t() -> bool:
        return True

    @task
    def f() -> bool:
        return False

    @workflow
    def wf(a: bool = True) -> bool:
        return (
            conditional("bool")
            .if_(a.is_true())
            .then(t())
            .else_()
            .then(f())
        )

    assert wf() is True
    assert wf(a=False) is False


def test_list_output_wf():
    @task
    def t1(a: int) -> int:
        a = a + 5
        return a

    @workflow
    def list_output_wf() -> typing.List[int]:
        v = []
        for i in range(2):
            v.append(t1(a=i))
        return v

    x = list_output_wf()
    assert x == [5, 6]


def test_sub_wf_single_named_tuple():
    nt = typing.NamedTuple("SingleNamedOutput", named1=int)

    @task
    def t1(a: int) -> nt:
        a = a + 2
        return (a,)

    @workflow
    def subwf(a: int) -> nt:
        return t1(a=a)

    @workflow
    def wf(b: int) -> nt:
        out = subwf(a=b)
        return t1(a=out.named1)

    x = wf(b=3)
    assert x == (7,)


def test_unexpected_outputs():
    @task
    def t1(a: int) -> int:
        a = a + 5
        return a

    @workflow
    def no_outputs_wf():
        return t1(a=3)

    # Should raise an exception because the workflow returns something when it shouldn't
    with pytest.raises(FlyteValueException):
        no_outputs_wf()

    # Should raise an exception because it doesn't return something when it should
    with pytest.raises(AssertionError):

        @workflow
        def one_output_wf() -> int:  # noqa
            t1(a=3)


def test_wf_no_output():
    @task
    def t1(a: int) -> int:
        a = a + 5
        return a

    @workflow
    def no_outputs_wf():
        t1(a=3)

    assert no_outputs_wf() is None
