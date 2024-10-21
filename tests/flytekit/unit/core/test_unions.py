import typing
from dataclasses import dataclass

import pytest

from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine, TypeTransformerFailedError


def test_asserting():
    @dataclass
    class A:
        a: str = None

    @dataclass
    class B:
        b: str = None

    @dataclass
    class C:
        c: str = None

    ctx = FlyteContextManager.current_context()

    pt = typing.Union[A, B, str]
    lt = TypeEngine.to_literal_type(pt)
    # mimic a register/remote fetch
    guessed = TypeEngine.guess_python_type(lt)

    TypeEngine.to_literal(ctx, A("a"), guessed, lt)
    TypeEngine.to_literal(ctx, B(b="bb"), guessed, lt)
    TypeEngine.to_literal(ctx, "hello", guessed, lt)

    with pytest.raises(TypeTransformerFailedError):
        TypeEngine.to_literal(ctx, C("cc"), guessed, lt)

    with pytest.raises(TypeTransformerFailedError):
        TypeEngine.to_literal(ctx, 3, guessed, lt)
