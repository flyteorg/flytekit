import typing
from dataclasses import dataclass
from enum import Enum
import sys
import pytest
import mock
from flytekit import task

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

# Issue Link: https://github.com/flyteorg/flyte/issues/5986
@mock.patch("flytekit.core.data_persistence.FileAccessProvider.async_put_data")
def test_union_in_dataclass(mock_upload_dir):
    mock_upload_dir.return_value = True

    from dataclasses_json import DataClassJsonMixin
    from flytekit.core.type_engine import DataclassTransformer
    import msgpack
    from flytekit.types.file import FlyteFile

    remote_path = "s3://my-bucket"
    f1 = FlyteFile("f1", remote_path=remote_path)
    mock_upload_dir.return_value = remote_path

    @dataclass
    class A(DataClassJsonMixin):
        a: int

    @dataclass
    class TestDataclass(DataClassJsonMixin):
        a: typing.Union[A, str, bool, int, float, None]
        b: typing.Union[A, FlyteFile, bool, int, float, None]
        c: typing.Union[A, typing.Dict[str, str], typing.List[int], None]
        d: typing.Union[A, FlyteFile, int, bool, float, None]

    o = TestDataclass(a=A(a=1), b=f1, c={"a": "value"}, d=remote_path)
    ctx = FlyteContextManager.current_context()

    tf = DataclassTransformer()
    lt = tf.get_literal_type(TestDataclass)
    lv = tf.to_literal(ctx, o, TestDataclass, lt)

    msgpack_bytes = lv.scalar.binary.value
    dict_obj = msgpack.loads(msgpack_bytes)

    assert dict_obj["a"]["a"] == 1
    assert dict_obj["b"]["path"] == remote_path
    assert dict_obj["c"]["a"] == "value"
    assert dict_obj["d"]["path"] == remote_path

    # Convert back to python object
    ot = tf.to_python_value(ctx, lv, TestDataclass)

    assert o.a.a == ot.a.a
    assert o.b.remote_path == ot.b.remote_source
    assert o.c["a"] == ot.c["a"]
    assert o.d == FlyteFile(remote_path)

def test_ambiguous_union_transformer_to_literal():
    @dataclass
    class A:
        a: int

    @dataclass
    class B:
        a: int

    @task
    def t1() -> typing.Union[A, B]:
        return A(a=1)

    with pytest.raises(
        TypeError,
        match=(
            "Failed to convert outputs of task '.*?' at position 0\\.\n"
            "Failed to convert type <class '.*?\\.A'> to type typing\\.Union\\[.*?\\]\\.\n"
            "Error Message: Ambiguous choice of variant for union type\\.\n"
            "Potential types: \\[<class '.*?\\.A'>, <class '.*?\\.B'>\\]\n"
            "These types are structurally the same, because it's attributes have the same names and associated types\\."
        ),
    ):
        t1()
