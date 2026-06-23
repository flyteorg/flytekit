import typing

import pytest

from flytekit import task, workflow
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeTransformerFailedError
from flytekit.models.literals import Literal, Scalar, Primitive
from flytekit.types.iterator.iterator import IteratorTransformer


@task
def t1(a: int) -> typing.Iterator[int]:
    for i in range(a):
        yield i


@task
def t2(ls: typing.Iterator[int]) -> typing.List[int]:
    return [x for x in ls]


@workflow
def wf(a: int) -> typing.List[int]:
    return t1(a=a)


def test_iterator():
    assert wf(a=4) == [0, 1, 2, 3]


def test_to_python_value_non_collection_raises_with_message():
    ctx = FlyteContextManager.current_context()
    lit = Literal(scalar=Scalar(primitive=Primitive(integer=42)))
    trans = IteratorTransformer()
    with pytest.raises(TypeTransformerFailedError) as exc_info:
        trans.to_python_value(ctx, lit, typing.Iterator[int])
    assert exc_info.value.args[0]  # args[0] must be set — not empty tuple
