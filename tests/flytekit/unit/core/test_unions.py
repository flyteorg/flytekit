import typing
from dataclasses import dataclass
from enum import Enum
import sys
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


@pytest.mark.skipif(
    sys.version_info < (3, 10), reason="enum checking only works in 3.10+"
)
def test_asserting_enum():
    class Color(Enum):
        RED = "one"
        GREEN = "two"
        BLUE = "blue"

    lt = TypeEngine.to_literal_type(Color)
    guessed = TypeEngine.guess_python_type(lt)
    tf = TypeEngine.get_transformer(guessed)
    tf.assert_type(guessed, "one")
    tf.assert_type(guessed, guessed("two"))
    tf.assert_type(Color, "one")

    guessed2 = TypeEngine.guess_python_type(lt)
    tf.assert_type(guessed, guessed2("two"))


@pytest.mark.skipif(
    sys.version_info >= (3, 10), reason="3.9 enum testing"
)
def test_asserting_enum_39():
    class Color(Enum):
        RED = "one"
        GREEN = "two"
        BLUE = "blue"

    lt = TypeEngine.to_literal_type(Color)
    guessed = TypeEngine.guess_python_type(lt)
    tf = TypeEngine.get_transformer(guessed)
    tf.assert_type(guessed, guessed("two"))
    tf.assert_type(Color, Color.GREEN)


@pytest.mark.sandbox_test
def test_with_remote():
    from flytekit.remote.remote import FlyteRemote
    from typing_extensions import Annotated, get_args
    from flytekit.configuration import Config, Image, ImageConfig, SerializationSettings

    r = FlyteRemote(
        Config.auto(config_file="/Users/ytong/.flyte/config-sandbox.yaml"),
        default_project="flytesnacks",
        default_domain="development",
    )
    lp = r.fetch_launch_plan(name="yt_dbg.scratchpad.union_enums.wf", version="oppOd5jst-LWExhTLM0F2w")
    guessed_union_type = TypeEngine.guess_python_type(lp.interface.inputs["x"].type)
    guessed_enum = get_args(guessed_union_type)[0]
    val = guessed_enum("one")
    r.execute(lp, inputs={"x": val})
