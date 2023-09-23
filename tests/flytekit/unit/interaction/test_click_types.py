import functools
import json
import tempfile
import typing
from datetime import datetime, timedelta
from enum import Enum

import click
import mock
import pytest
import yaml

from flytekit import FlyteContextManager
from flytekit.configuration import Config
from flytekit.core.type_engine import TypeEngine
from flytekit.interaction.click_types import (
    DateTimeType,
    DurationParamType,
    FileParamType,
    FlyteLiteralConverter,
    JsonParamType,
    key_value_callback,
)
from flytekit.models.types import SimpleType
from flytekit.remote import FlyteRemote


def test_file_param():
    m = mock.MagicMock()
    l = FileParamType().convert(__file__, m, m)
    assert l.local
    r = FileParamType().convert("https://tmp/file", m, m)
    assert r.local is False


class Color(Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"


@pytest.mark.parametrize(
    "python_type, python_value",
    [
        (typing.Union[typing.List[int], str, Color], "flyte"),
        (typing.Union[typing.List[int], str, Color], "red"),
        (typing.Union[typing.List[int], str, Color], [1, 2, 3]),
        (typing.List[int], [1, 2, 3]),
        (typing.Dict[str, int], {"flyte": 2}),
    ],
)
def test_literal_converter(python_type, python_value):
    get_upload_url_fn = functools.partial(
        FlyteRemote(Config.auto()).client.get_upload_signed_url, project="p", domain="d"
    )
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(python_type)

    lc = FlyteLiteralConverter(
        ctx,
        literal_type=lt,
        python_type=python_type,
        get_upload_url_fn=get_upload_url_fn,
        is_remote=True,
    )

    click_ctx = click.Context(click.Command("test_command"), obj={"remote": True})
    assert lc.convert(click_ctx, ctx, python_value) == TypeEngine.to_literal(ctx, python_value, python_type, lt)


def test_enum_converter():
    pt = typing.Union[str, Color]

    get_upload_url_fn = functools.partial(FlyteRemote(Config.auto()).client.get_upload_signed_url)
    click_ctx = click.Context(click.Command("test_command"), obj={"remote": True})
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(pt)
    lc = FlyteLiteralConverter(
        ctx, literal_type=lt, python_type=pt, get_upload_url_fn=get_upload_url_fn, is_remote=True
    )
    union_lt = lc.convert(click_ctx, ctx, "red").scalar.union

    assert union_lt.stored_type.simple == SimpleType.STRING
    assert union_lt.stored_type.enum_type is None

    pt = typing.Union[Color, str]
    lt = TypeEngine.to_literal_type(typing.Union[Color, str])
    lc = FlyteLiteralConverter(
        ctx, literal_type=lt, python_type=pt, get_upload_url_fn=get_upload_url_fn, is_remote=True
    )
    union_lt = lc.convert(click_ctx, ctx, "red").scalar.union

    assert union_lt.stored_type.simple is None
    assert union_lt.stored_type.enum_type.values == ["red", "green", "blue"]


def test_duration_type():
    t = DurationParamType()
    assert t.convert(value="1 day", param=None, ctx=None) == timedelta(days=1)

    with pytest.raises(click.BadParameter):
        t.convert(None, None, None)


def test_datetime_type():
    t = DateTimeType()

    assert t.convert("2020-01-01", None, None) == datetime(2020, 1, 1)

    now = datetime.now()
    v = t.convert("now", None, None)
    assert v.day == now.day
    assert v.month == now.month


def test_json_type():
    t = JsonParamType()
    assert t.convert(value='{"a": "b"}', param=None, ctx=None) == {"a": "b"}

    with pytest.raises(click.BadParameter):
        t.convert(None, None, None)

    # test that it loads a json file
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        json.dump({"a": "b"}, f)
        f.flush()
        assert t.convert(value=f.name, param=None, ctx=None) == {"a": "b"}

    # test that if the file is not a valid json, it raises an error
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write("asdf")
        f.flush()
        with pytest.raises(click.BadParameter):
            t.convert(value=f.name, param="asdf", ctx=None)

    # test if the file does not exist
    with pytest.raises(click.BadParameter):
        t.convert(value="asdf", param=None, ctx=None)

    # test if the file is yaml and ends with .yaml it works correctly
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        yaml.dump({"a": "b"}, f)
        f.flush()
        assert t.convert(value=f.name, param=None, ctx=None) == {"a": "b"}


def test_key_value_callback():
    """Write a test that verifies that the callback works correctly."""
    ctx = click.Context(click.Command("test_command"), obj={"remote": True})
    assert key_value_callback(ctx, "a", None) is None
    assert key_value_callback(ctx, "a", ["a=b"]) == {"a": "b"}
    assert key_value_callback(ctx, "a", ["a=b", "c=d"]) == {"a": "b", "c": "d"}
    assert key_value_callback(ctx, "a", ["a=b", "c=d", "e=f"]) == {"a": "b", "c": "d", "e": "f"}
    with pytest.raises(click.BadParameter):
        key_value_callback(ctx, "a", ["a=b", "c"])
    with pytest.raises(click.BadParameter):
        key_value_callback(ctx, "a", ["a=b", "c=d", "e"])
    with pytest.raises(click.BadParameter):
        key_value_callback(ctx, "a", ["a=b", "c=d", "e=f", "g"])
