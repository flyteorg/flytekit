import os
from dataclasses import field
import json
import sys
import tempfile
import typing
from datetime import datetime, timedelta
from enum import Enum

import click
import mock
import pytest
import yaml

from flytekit import FlyteContextManager
from flytekit.core.artifact import Artifact
from flytekit.core.type_engine import TypeEngine
from flytekit.interaction.click_types import (
    DateTimeType,
    DirParamType,
    DurationParamType,
    EnumParamType,
    FileParamType,
    FlyteLiteralConverter,
    JsonParamType,
    PickleParamType,
    StructuredDatasetParamType,
    UnionParamType,
    key_value_callback,
)

dummy_param = click.Option(["--dummy"], type=click.STRING, default="dummy")

def test_dir_param():
    import os
    m = mock.MagicMock()
    current_file_directory = os.path.dirname(os.path.abspath(__file__))
    l = DirParamType().convert(current_file_directory, m, m)
    assert l.path == current_file_directory
    r = DirParamType().convert("https://tmp/dir", m, m)
    assert r.path == "https://tmp/dir"

def test_file_param():
    m = mock.MagicMock()
    l = FileParamType().convert(__file__, m, m)
    assert l.path == __file__
    r = FileParamType().convert("https://tmp/file", m, m)
    assert r.path == "https://tmp/file"


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
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(python_type)

    lc = FlyteLiteralConverter(
        ctx,
        literal_type=lt,
        python_type=python_type,
        is_remote=True,
    )

    click_ctx = click.Context(click.Command("test_command"), obj={"remote": True})
    assert lc.convert(click_ctx, dummy_param, python_value) == TypeEngine.to_literal(ctx, python_value, python_type, lt)


def test_literal_converter_query():
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(int)

    lc = FlyteLiteralConverter(
        ctx,
        literal_type=lt,
        python_type=int,
        is_remote=True,
    )

    a = Artifact(name="test-artifact")
    query = a.query()
    click_ctx = click.Context(click.Command("test_command"), obj={"remote": True})
    assert lc.convert(click_ctx, dummy_param, query) == query


@pytest.mark.parametrize(
    "python_type, python_str_value, python_value",
    [
        (Color, "red", Color.RED),
        (int, "1", 1),
        (float, "1.0", 1.0),
        (bool, "True", True),
        (bool, "False", False),
        (str, "flyte", "flyte"),
        (typing.List[int], "[1, 2, 3]", [1, 2, 3]),
        (typing.Dict[str, int], '{"a": 1}', {"a": 1}),
        (bool, "true", True),
        (bool, "false", False),
        (bool, "TRUE", True),
        (bool, "FALSE", False),
        (bool, "t", True),
        (bool, "f", False),
        (typing.Optional[int], "1", 1),
        (typing.Union[int, str, Color], "1", 1),
        (typing.Union[Color, str], "hello", "hello"),
        (typing.Union[Color, str], "red", Color.RED),
        (typing.Union[int, str, Color], "Hello", "Hello"),
        (typing.Union[int, str, Color], "red", Color.RED),
        (typing.Union[int, str, Color, float], "1.1", 1.1),
    ],
)
def test_enum_converter(python_type: typing.Type, python_str_value: str, python_value: typing.Any):
    p = dummy_param
    pt = python_type
    click_ctx = click.Context(click.Command("test_command"), obj={"remote": True})
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(pt)
    lc = FlyteLiteralConverter(ctx, literal_type=lt, python_type=pt, is_remote=True)
    assert lc.convert(click_ctx, p, lc.click_type.convert(python_str_value, p, click_ctx)) == TypeEngine.to_literal(
        ctx, python_value, pt, lt
    )


def test_duration_type():
    t = DurationParamType()
    assert t.convert(value="1 day", param=None, ctx=None) == timedelta(days=1)

    with pytest.raises(click.BadParameter):
        t.convert(None, None, None)


# write a helper function that calls convert and checks the result
def _datetime_helper(t: click.ParamType, value: str, expected: datetime):
    v = t.convert(value, None, None)
    assert v.day == expected.day
    assert v.month == expected.month


def test_datetime_type():
    t = DateTimeType()

    assert t.convert("2020-01-01", None, None) == datetime(2020, 1, 1)

    now = datetime.now()
    _datetime_helper(t, "now", now)

    today = datetime.today()
    _datetime_helper(t, "today", today)

    add = datetime.now() + timedelta(days=1)
    _datetime_helper(t, "now + 1d", add)

    sub = datetime.now() - timedelta(days=1)
    _datetime_helper(t, "now - 1d", sub)

    fmt_v = "2020-01-01T10:10:00"
    d = t.convert(fmt_v, None, None)
    _datetime_helper(t, fmt_v, d)

    _datetime_helper(t, f"{fmt_v} + 1d", d + timedelta(days=1))

    with pytest.raises(click.BadParameter):
        t.convert("now-1d", None, None)

    with pytest.raises(click.BadParameter):
        t.convert("now + 1", None, None)

    with pytest.raises(click.BadParameter):
        t.convert("now + 1abc", None, None)

    with pytest.raises(click.BadParameter):
        t.convert("aaa + 1d", None, None)

    fmt_v = "2024-07-29 13:47:07.643004+00:00"
    d = t.convert(fmt_v, None, None)
    _datetime_helper(t, fmt_v, d)


def test_json_type():
    t = JsonParamType(typing.Dict[str, str])
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
            t.convert(value=f.name, param=dummy_param, ctx=None)

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


@pytest.mark.parametrize(
    "param_type",
    [
        (DateTimeType()),
        (DurationParamType()),
        (JsonParamType(typing.Dict[str, str])),
        (UnionParamType([click.FLOAT, click.INT])),
        (EnumParamType(Color)),
        (PickleParamType()),
        (StructuredDatasetParamType()),
        (DirParamType()),
    ],
)
def test_query_passing(param_type: click.ParamType):
    a = Artifact(name="test-artifact")
    query = a.query()

    assert param_type.convert(value=query, param=None, ctx=None) is query


def test_dataclass_type():
    from dataclasses import dataclass

    @dataclass
    class Datum:
        x: int
        y: str
        z: typing.Dict[int, str]
        w: typing.List[int]

    t = JsonParamType(Datum)
    value = '{ "x": 1, "y": "2", "z": { "1": "one", "2": "two" }, "w": [1, 2, 3] }'
    v = t.convert(value=value, param=None, ctx=None)

    assert v.x == 1
    assert v.y == "2"
    assert v.z == {1: "one", 2: "two"}
    assert v.w == [1, 2, 3]


def test_nested_dataclass_type():
    from dataclasses import dataclass

    @dataclass
    class Datum:
        w: int
        x: str = "default"
        y: typing.Dict[str, str] = field(default_factory=lambda: {"key": "value"})
        z: typing.List[int] = field(default_factory=lambda: [1, 2, 3])

    @dataclass
    class NestedDatum:
        w: Datum
        x: typing.List[Datum]
        y: typing.Dict[str, Datum] = field(default_factory=lambda: {"key": Datum(1)})


    # typing.List[Datum]
    value = '[{ "w": 1 }]'
    t = JsonParamType(typing.List[Datum])
    v = t.convert(value=value, param=None, ctx=None)

    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(typing.List[Datum])
    literal_converter = FlyteLiteralConverter(
        ctx, literal_type=lt, python_type=typing.List[Datum], is_remote=False
    )
    v = literal_converter.convert(ctx, None, v)

    assert v[0].w == 1
    assert v[0].x == "default"
    assert v[0].y == {"key": "value"}
    assert v[0].z == [1, 2, 3]

    # typing.Dict[str, Datum]
    value = '{ "x": { "w": 1 } }'
    t = JsonParamType(typing.Dict[str, Datum])
    v = t.convert(value=value, param=None, ctx=None)
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(typing.Dict[str, Datum])
    literal_converter = FlyteLiteralConverter(
        ctx, literal_type=lt, python_type=typing.Dict[str, Datum], is_remote=False
    )
    v = literal_converter.convert(ctx, None, v)

    assert v["x"].w == 1
    assert v["x"].x == "default"
    assert v["x"].y == {"key": "value"}
    assert v["x"].z == [1, 2, 3]

    # typing.List[NestedDatum]
    value = '[{"w":{ "w" : 1 },"x":[{ "w" : 1 }]}]'
    t = JsonParamType(typing.List[NestedDatum])
    v = t.convert(value=value, param=None, ctx=None)
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(typing.List[NestedDatum])
    literal_converter = FlyteLiteralConverter(
        ctx, literal_type=lt, python_type=typing.List[NestedDatum], is_remote=False
    )
    v = literal_converter.convert(ctx, None, v)

    assert v[0].w.w == 1
    assert v[0].w.x == "default"
    assert v[0].w.y == {"key": "value"}
    assert v[0].w.z == [1, 2, 3]
    assert v[0].x[0].w == 1
    assert v[0].x[0].x == "default"
    assert v[0].x[0].y == {"key": "value"}
    assert v[0].x[0].z == [1, 2, 3]

    # typing.List[typing.List[Datum]]
    value = '[[{ "w": 1 }]]'
    t = JsonParamType(typing.List[typing.List[Datum]])
    v = t.convert(value=value, param=None, ctx=None)
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(typing.List[typing.List[Datum]])
    literal_converter = FlyteLiteralConverter(
        ctx, literal_type=lt, python_type=typing.List[typing.List[Datum]], is_remote=False
    )
    v = literal_converter.convert(ctx, None, v)

    assert v[0][0].w == 1
    assert v[0][0].x == "default"
    assert v[0][0].y == {"key": "value"}
    assert v[0][0].z == [1, 2, 3]

def test_dataclass_with_default_none():
    from dataclasses import dataclass

    @dataclass
    class Datum:
        x: int
        y: str = None
        z: typing.Dict[int, str] = None
        w: typing.List[int] = None

    t = JsonParamType(Datum)
    value = '{ "x": 1 }'
    v = t.convert(value=value, param=None, ctx=None)
    lt = TypeEngine.to_literal_type(Datum)
    ctx = FlyteContextManager.current_context()
    literal_converter = FlyteLiteralConverter(
        ctx, literal_type=lt, python_type=Datum, is_remote=False
    )
    v = literal_converter.convert(ctx=ctx, param=None, value=v)

    assert v.x == 1
    assert v.y is None
    assert v.z is None
    assert v.w is None


def test_dataclass_with_flyte_type_exception():
    from dataclasses import dataclass
    from flytekit import StructuredDataset
    from flytekit.types.directory import FlyteDirectory
    from flytekit.types.file import FlyteFile
    import os

    DIR_NAME = os.path.dirname(os.path.realpath(__file__))
    parquet_file = os.path.join(DIR_NAME, "testdata/df.parquet")

    @dataclass
    class Datum:
        x: FlyteFile
        y: FlyteDirectory
        z: StructuredDataset

    t = JsonParamType(Datum)
    value = { "x": parquet_file, "y": DIR_NAME, "z": os.path.join(DIR_NAME, "testdata")}

    with pytest.raises(AttributeError):
        t.convert(value=value, param=None, ctx=None)

def test_dataclass_with_optional_fields():
    from dataclasses import dataclass
    from typing import Optional

    @dataclass
    class Datum:
        x: int
        y: Optional[str] = None
        z: Optional[typing.Dict[int, str]] = None
        w: Optional[typing.List[int]] = None

    t = JsonParamType(Datum)
    value = '{ "x": 1 }'
    v = t.convert(value=value, param=None, ctx=None)
    lt = TypeEngine.to_literal_type(Datum)
    ctx = FlyteContextManager.current_context()
    literal_converter = FlyteLiteralConverter(
        ctx, literal_type=lt, python_type=Datum, is_remote=False
    )
    v = literal_converter.convert(ctx=ctx, param=None, value=v)

    # Assertions to check the Optional fields
    assert v.x == 1
    assert v.y is None  # Optional field with no value provided
    assert v.z is None  # Optional field with no value provided
    assert v.w is None  # Optional field with no value provided

    # Test with all fields provided
    value = '{ "x": 2, "y": "test", "z": {"1": "value"}, "w": [1, 2, 3] }'
    v = t.convert(value=value, param=None, ctx=None)
    v = literal_converter.convert(ctx=ctx, param=None, value=v)

    assert v.x == 2
    assert v.y == "test"
    assert v.z == {1: "value"}
    assert v.w == [1, 2, 3]

def test_nested_dataclass_with_optional_fields():
    from dataclasses import dataclass
    from typing import Optional, List, Dict

    @dataclass
    class InnerDatum:
        a: int
        b: Optional[str] = None

    @dataclass
    class Datum:
        x: int
        y: Optional[InnerDatum] = None
        z: Optional[Dict[str, InnerDatum]] = None
        w: Optional[List[InnerDatum]] = None

    t = JsonParamType(Datum)

    # Case 1: Only required field provided
    value = '{ "x": 1 }'
    v = t.convert(value=value, param=None, ctx=None)
    lt = TypeEngine.to_literal_type(Datum)
    ctx = FlyteContextManager.current_context()
    literal_converter = FlyteLiteralConverter(
        ctx, literal_type=lt, python_type=Datum, is_remote=False
    )
    v = literal_converter.convert(ctx=ctx, param=None, value=v)

    # Assertions to check the Optional fields
    assert v.x == 1
    assert v.y is None  # Optional field with no value provided
    assert v.z is None  # Optional field with no value provided
    assert v.w is None  # Optional field with no value provided

    # Case 2: All fields provided with nested structures
    value = '''
    {
        "x": 2,
        "y": {"a": 10, "b": "inner"},
        "z": {"key": {"a": 20, "b": "nested"}},
        "w": [{"a": 30, "b": "list_item"}]
    }
    '''
    v = t.convert(value=value, param=None, ctx=None)
    v = literal_converter.convert(ctx=ctx, param=None, value=v)

    # Assertions for nested structure
    assert v.x == 2
    assert v.y.a == 10
    assert v.y.b == "inner"
    assert v.z["key"].a == 20
    assert v.z["key"].b == "nested"
    assert v.w[0].a == 30
    assert v.w[0].b == "list_item"


@pytest.mark.skipif(
    sys.version_info < (3, 12), reason="handling for windows is nicer with delete_on_close, which doesn't exist before 3.12"
)
def test_pickle_type():
    t = PickleParamType()
    value = {"a": "b"}
    v = t.convert(value=value, param=None, ctx=None)
    assert v == value

    t.convert("", None, None)

    t.convert("module.x", None, None)

    with pytest.raises(click.BadParameter):
        t.convert("module:var", None, None)

    with pytest.raises(click.BadParameter):
        t.convert("typing:not_exists", None, None)

    # test that it can load a variable from a module
    with tempfile.NamedTemporaryFile("w", dir=".", suffix=".py", delete=True, delete_on_close=False) as f:
        f.write("a = 1")
        f.flush()
        f.close()
        # find the base name of the file
        basename = os.path.basename(f.name).split(".")[0]
        assert t.convert(f"{basename}:a", None, None) == 1
