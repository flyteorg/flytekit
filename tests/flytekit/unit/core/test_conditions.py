import typing
from collections import OrderedDict

import mock
import pytest

from flytekit import task, workflow
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.condition import conditional
from flytekit.core.context_manager import Image, ImageConfig, SerializationSettings

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


@task
def five() -> int:
    return 5


@task
def square(n: float) -> float:
    """
    Parameters:
        n (float): name of the parameter for the task will be derived from the name of the input variable
               the type will be automatically deduced to be Types.Integer

    Return:
        float: The label for the output will be automatically assigned and type will be deduced from the annotation

    """
    return n * n


@task
def double(n: float) -> float:
    """
    Parameters:
        n (float): name of the parameter for the task will be derived from the name of the input variable
               the type will be automatically deduced to be Types.Integer

    Return:
        float: The label for the output will be automatically assigned and type will be deduced from the annotation

    """
    return 2 * n


def test_condition_else_fail():
    @workflow
    def multiplier_2(my_input: float) -> float:
        return (
            conditional("fractions")
            .if_((my_input > 0.1) & (my_input < 1.0))
            .then(double(n=my_input))
            .elif_((my_input > 1.0) & (my_input < 10.0))
            .then(square(n=my_input))
            .else_()
            .fail("The input must be between 0 and 10")
        )

    with pytest.raises(ValueError):
        multiplier_2(my_input=10)


def test_condition_sub_workflows():
    @task
    def sum_div_sub(a: int, b: int) -> typing.NamedTuple("Outputs", sum=int, div=int, sub=int):
        return a + b, a / b, a - b

    @task
    def sum_sub(a: int, b: int) -> typing.NamedTuple("Outputs", sum=int, sub=int):
        return a + b, a - b

    @workflow
    def sub_wf(a: int, b: int) -> (int, int):
        return sum_sub(a=a, b=b)

    @workflow
    def math_ops(a: int, b: int) -> (int, int):
        # Flyte will only make `sum` and `sub` available as outputs because they are common between all branches
        sum, sub = (
            conditional("noDivByZero")
            .if_(a > b)
            .then(sub_wf(a=a, b=b))
            .else_()
            .fail("Only positive results are allowed")
        )

        return sum, sub

    x, y = math_ops(a=3, b=2)
    assert x == 5
    assert y == 1


def test_condition_tuple_branches():
    @task
    def sum_sub(a: int, b: int) -> typing.NamedTuple("Outputs", sum=int, sub=int):
        return a + b, a - b

    @workflow
    def math_ops(a: int, b: int) -> (int, int):
        add, sub = (
            conditional("noDivByZero")
            .if_(a > b)
            .then(sum_sub(a=a, b=b))
            .else_()
            .fail("Only positive results are allowed")
        )

        return add, sub

    x, y = math_ops(a=3, b=2)
    assert x == 5
    assert y == 1

    sdk_wf = get_serializable(OrderedDict(), serialization_settings, math_ops)
    assert len(sdk_wf.nodes) == 1
    assert sdk_wf.nodes[0].branch_node.if_else.case.then_node.task_node.reference_id.name == "test_conditions.sum_sub"


def test_condition_unary_bool():
    @task
    def return_true() -> bool:
        return True

    @workflow
    def failed() -> int:
        return 10

    @workflow
    def success() -> int:
        return 20

    with pytest.raises(AssertionError):

        @workflow
        def decompose_unary() -> int:
            result = return_true()
            return conditional("test").if_(result).then(success()).else_().then(failed())

    with pytest.raises(AssertionError):

        @workflow
        def decompose_none() -> int:
            return conditional("test").if_(None).then(success()).else_().then(failed())

    with pytest.raises(AssertionError):

        @workflow
        def decompose_is() -> int:
            result = return_true()
            return conditional("test").if_(result is True).then(success()).else_().then(failed())

    @workflow
    def decompose() -> int:
        result = return_true()
        return conditional("test").if_(result.is_true()).then(success()).else_().then(failed())

    assert decompose() == 20


def test_subworkflow_condition_serialization():
    """Test that subworkflows are correctly extracted from serialized workflows with condiationals."""

    @task
    def t() -> int:
        return 5

    @workflow
    def wf1() -> int:
        return t()

    @workflow
    def wf2() -> int:
        return t()

    @workflow
    def wf3() -> int:
        return t()

    @workflow
    def wf4() -> int:
        return t()

    @workflow
    def ifelse_branching(x: int) -> int:
        return conditional("simple branching test").if_(x == 2).then(wf1()).else_().then(wf2())

    @workflow
    def ifelse_branching_fail(x: int) -> int:
        return conditional("simple branching test").if_(x == 2).then(wf1()).else_().fail("failed")

    @workflow
    def if_elif_else_branching(x: int) -> int:
        return (
            conditional("test")
            .if_(x == 2)
            .then(wf1())
            .elif_(x == 3)
            .then(wf2())
            .elif_(x == 4)
            .then(wf3())
            .else_()
            .then(wf4())
        )

    @workflow
    def wf5() -> int:
        return t()

    @workflow
    def nested_branching(x: int) -> int:
        return conditional("nested test").if_(x == 2).then(ifelse_branching(x=x)).else_().then(wf5())

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    for wf, expected_subworkflows in [
        (ifelse_branching, ["test_conditions.{}".format(x) for x in ("wf1", "wf2")]),
        (ifelse_branching_fail, ["test_conditions.{}".format(x) for x in ("wf1",)]),
        (if_elif_else_branching, ["test_conditions.{}".format(x) for x in ("wf1", "wf2", "wf3", "wf4")]),
        (nested_branching, ["test_conditions.{}".format(x) for x in ("ifelse_branching", "wf1", "wf2", "wf5")]),
    ]:
        serializable_wf = get_serializable(OrderedDict(), serialization_settings, wf)
        subworkflows = serializable_wf.get_sub_workflows()

        assert [sub_wf.id.name for sub_wf in subworkflows] == expected_subworkflows


def test_subworkflow_condition():
    @task
    def t() -> int:
        return 5

    @workflow
    def wf1() -> int:
        return t()

    @workflow
    def branching(x: int) -> int:
        return conditional("test").if_(x == 2).then(t()).else_().then(wf1())

    assert branching(x=2) == 5
    assert branching(x=3) == 5


def test_subworkflow_condition_named_tuple():
    nt = typing.NamedTuple("SampleNamedTuple", b=int, c=str)

    @task
    def t() -> nt:
        return 5, "foo"

    @workflow
    def wf1() -> nt:
        return 3, "bar"

    @workflow
    def branching(x: int) -> nt:
        return conditional("test").if_(x == 2).then(t()).else_().then(wf1())

    assert branching(x=2) == (5, "foo")
    assert branching(x=3) == (3, "bar")


def test_subworkflow_condition_single_named_tuple():
    nt = typing.NamedTuple("SampleNamedTuple", b=int)

    @task
    def t() -> nt:
        return (5,)

    @workflow
    def wf1() -> nt:
        return t()

    @workflow
    def branching(x: int) -> int:
        return conditional("test").if_(x == 2).then(t().b).else_().then(wf1().b)

    assert branching(x=2) == 5


@mock.patch.object(five, "execute")
def test_call_counts(five_mock):
    five_mock.return_value = 5

    @workflow
    def if_elif_else_branching(x: int) -> int:
        return (
            conditional("test")
            .if_(x == 2)
            .then(five())
            .elif_(x == 3)
            .then(five())
            .elif_(x == 4)
            .then(five())
            .else_()
            .then(five())
        )

    res = if_elif_else_branching(x=2)

    assert res == 5
    assert five_mock.call_count == 1


def test_nested_condition():
    @workflow
    def multiplier_2(my_input: float) -> float:
        return (
            conditional("fractions")
            .if_((my_input > 0.1) & (my_input < 1.0))
            .then(
                conditional("inner_fractions")
                .if_(my_input < 0.5)
                .then(double(n=my_input))
                .else_()
                .fail("Only <0.5 allowed")
            )
            .elif_((my_input > 1.0) & (my_input < 10.0))
            .then(square(n=my_input))
            .else_()
            .fail("The input must be between 0 and 10")
        )

    srz_wf = get_serializable(OrderedDict(), serialization_settings, multiplier_2)
    # print(srz_wf)

    with pytest.raises(ValueError):
        multiplier_2(my_input=0.5)  # this is still getting called, should not be - 1.0 should not be printed

    res = multiplier_2(my_input=0.3)
    assert res == 0.6

    # # failing...
    # res = multiplier_2(my_input=5)
    # assert res == 25
