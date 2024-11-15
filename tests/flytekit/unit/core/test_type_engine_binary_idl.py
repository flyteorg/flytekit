import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Union

import pytest
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct
from mashumaro.codecs.json import JSONEncoder
from mashumaro.codecs.msgpack import MessagePackEncoder

from flytekit import task, workflow
from flytekit.core.constants import MESSAGEPACK
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import DataclassTransformer, TypeEngine
from flytekit.models.literals import Binary, Literal, Scalar
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


class Status(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


def test_simple_type_transformer():
    ctx = FlyteContextManager.current_context()

    int_inputs = [1, 2, 20240918, -1, -2, -20240918]
    encoder = MessagePackEncoder(int)
    for int_input in int_inputs:
        int_msgpack_bytes = encoder.encode(int_input)
        lv = Literal(
            scalar=Scalar(binary=Binary(value=int_msgpack_bytes, tag=MESSAGEPACK))
        )
        int_output = TypeEngine.to_python_value(ctx, lv, int)
        assert int_input == int_output

    float_inputs = [2024.0918, 5.0, -2024.0918, -5.0]
    encoder = MessagePackEncoder(float)
    for float_input in float_inputs:
        float_msgpack_bytes = encoder.encode(float_input)
        lv = Literal(
            scalar=Scalar(binary=Binary(value=float_msgpack_bytes, tag=MESSAGEPACK))
        )
        float_output = TypeEngine.to_python_value(ctx, lv, float)
        assert float_input == float_output

    bool_inputs = [True, False]
    encoder = MessagePackEncoder(bool)
    for bool_input in bool_inputs:
        bool_msgpack_bytes = encoder.encode(bool_input)
        lv = Literal(
            scalar=Scalar(binary=Binary(value=bool_msgpack_bytes, tag=MESSAGEPACK))
        )
        bool_output = TypeEngine.to_python_value(ctx, lv, bool)
        assert bool_input == bool_output

    str_inputs = ["hello", "world", "flyte", "kit", "is", "awesome"]
    encoder = MessagePackEncoder(str)
    for str_input in str_inputs:
        str_msgpack_bytes = encoder.encode(str_input)
        lv = Literal(
            scalar=Scalar(binary=Binary(value=str_msgpack_bytes, tag=MESSAGEPACK))
        )
        str_output = TypeEngine.to_python_value(ctx, lv, str)
        assert str_input == str_output

    datetime_inputs = [
        datetime.now(),
        datetime(2024, 9, 18),
        datetime(2024, 9, 18, 1),
        datetime(2024, 9, 18, 1, 1),
        datetime(2024, 9, 18, 1, 1, 1),
        datetime(2024, 9, 18, 1, 1, 1, 1),
    ]
    encoder = MessagePackEncoder(datetime)
    for datetime_input in datetime_inputs:
        datetime_msgpack_bytes = encoder.encode(datetime_input)
        lv = Literal(
            scalar=Scalar(binary=Binary(value=datetime_msgpack_bytes, tag=MESSAGEPACK))
        )
        datetime_output = TypeEngine.to_python_value(ctx, lv, datetime)
        assert datetime_input == datetime_output

    date_inputs = [date.today(), date(2024, 9, 18)]
    encoder = MessagePackEncoder(date)
    for date_input in date_inputs:
        date_msgpack_bytes = encoder.encode(date_input)
        lv = Literal(
            scalar=Scalar(binary=Binary(value=date_msgpack_bytes, tag=MESSAGEPACK))
        )
        date_output = TypeEngine.to_python_value(ctx, lv, date)
        assert date_input == date_output

    timedelta_inputs = [
        timedelta(days=1),
        timedelta(days=1, seconds=1),
        timedelta(days=1, seconds=1, microseconds=1),
        timedelta(days=1, seconds=1, microseconds=1, milliseconds=1),
        timedelta(days=1, seconds=1, microseconds=1, milliseconds=1, minutes=1),
        timedelta(
            days=1, seconds=1, microseconds=1, milliseconds=1, minutes=1, hours=1
        ),
        timedelta(
            days=1,
            seconds=1,
            microseconds=1,
            milliseconds=1,
            minutes=1,
            hours=1,
            weeks=1,
        ),
        timedelta(
            days=-1,
            seconds=-1,
            microseconds=-1,
            milliseconds=-1,
            minutes=-1,
            hours=-1,
            weeks=-1,
        ),
    ]
    encoder = MessagePackEncoder(timedelta)
    for timedelta_input in timedelta_inputs:
        timedelta_msgpack_bytes = encoder.encode(timedelta_input)
        lv = Literal(
            scalar=Scalar(binary=Binary(value=timedelta_msgpack_bytes, tag=MESSAGEPACK))
        )
        timedelta_output = TypeEngine.to_python_value(ctx, lv, timedelta)
        assert timedelta_input == timedelta_output


def test_untyped_dict():
    ctx = FlyteContextManager.current_context()

    dict_inputs = [
        # Basic key-value combinations with int, str, bool, float
        {1: "a", "key": 2.5, True: False, 3.14: 100},
        {"a": 1, -2: "b", 3.5: True, False: 3.1415},
        {
            1: [-1, "a", 2.5, False],
            "key_list": ["str", 3.14, True, 42],
            True: [False, 2.718, "test"],
        },
        {
            "nested_dict": {-1: 2, "key": "value", True: 3.14, False: "string"},
            3.14: {"pi": 3.14, "e": 2.718, 42: True},
        },
        {
            "list_in_dict": [
                {"inner_dict_1": [1, -2.5, "a"], "inner_dict_2": [True, False, 3.14]},
                [1, -2, 3, {"nested_list_dict": [False, "test"]}],
            ]
        },
        {
            "complex_nested": {
                1: {"nested_dict": {True: [-1, "a", 2.5]}},
                "string_key": {False: {-3.14: {"deep": [1, "deep_value"]}}},
            }
        },
        {
            "list_of_dicts": [{"a": 1, "b": -2}, {"key1": "value1", "key2": "value2"}],
            10: [{"nested_list": [-1, "value", 3.14]}, {"another_list": [True, False]}],
        },
        # More nested combinations of list and dict
        {
            "outer_list": [
                [1, -2, 3],
                {
                    "inner_dict": {"key1": [True, "string", -3.14], "key2": [1, -2.5]}
                },  # Dict inside list
            ],
            "another_dict": {
                "key1": {"subkey": [1, -2, "str"]},
                "key2": [False, -3.14, "test"],
            },
        },
    ]

    for dict_input in dict_inputs:
        # dict_msgpack_bytes = msgpack.dumps(dict_input)
        dict_msgpack_bytes = MessagePackEncoder(dict).encode(dict_input)
        lv = Literal(
            scalar=Scalar(binary=Binary(value=dict_msgpack_bytes, tag=MESSAGEPACK))
        )
        dict_output = TypeEngine.to_python_value(ctx, lv, dict)
        assert dict_input == dict_output


def test_list_transformer():
    ctx = FlyteContextManager.current_context()

    list_int_input = [1, -2, 3]
    encoder = MessagePackEncoder(List[int])
    list_int_msgpack_bytes = encoder.encode(list_int_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=list_int_msgpack_bytes, tag=MESSAGEPACK))
    )
    list_int_output = TypeEngine.to_python_value(ctx, lv, List[int])
    assert list_int_input == list_int_output

    list_float_input = [1.0, -2.0, 3.0]
    encoder = MessagePackEncoder(List[float])
    list_float_msgpack_bytes = encoder.encode(list_float_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=list_float_msgpack_bytes, tag=MESSAGEPACK))
    )
    list_float_output = TypeEngine.to_python_value(ctx, lv, List[float])
    assert list_float_input == list_float_output

    list_str_input = ["a", "b", "c"]
    encoder = MessagePackEncoder(List[str])
    list_str_msgpack_bytes = encoder.encode(list_str_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=list_str_msgpack_bytes, tag=MESSAGEPACK))
    )
    list_str_output = TypeEngine.to_python_value(ctx, lv, List[str])
    assert list_str_input == list_str_output

    list_bool_input = [True, False, True]
    encoder = MessagePackEncoder(List[bool])
    list_bool_msgpack_bytes = encoder.encode(list_bool_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=list_bool_msgpack_bytes, tag=MESSAGEPACK))
    )
    list_bool_output = TypeEngine.to_python_value(ctx, lv, List[bool])
    assert list_bool_input == list_bool_output

    list_list_int_input = [[1, -2], [-3, 4]]
    encoder = MessagePackEncoder(List[List[int]])
    list_list_int_msgpack_bytes = encoder.encode(list_list_int_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=list_list_int_msgpack_bytes, tag=MESSAGEPACK))
    )
    list_list_int_output = TypeEngine.to_python_value(ctx, lv, List[List[int]])
    assert list_list_int_input == list_list_int_output

    list_list_float_input = [[1.0, -2.0], [-3.0, 4.0]]
    encoder = MessagePackEncoder(List[List[float]])
    list_list_float_msgpack_bytes = encoder.encode(list_list_float_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=list_list_float_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    list_list_float_output = TypeEngine.to_python_value(ctx, lv, List[List[float]])
    assert list_list_float_input == list_list_float_output

    list_list_str_input = [["a", "b"], ["c", "d"]]
    encoder = MessagePackEncoder(List[List[str]])
    list_list_str_msgpack_bytes = encoder.encode(list_list_str_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=list_list_str_msgpack_bytes, tag=MESSAGEPACK))
    )
    list_list_str_output = TypeEngine.to_python_value(ctx, lv, List[List[str]])
    assert list_list_str_input == list_list_str_output

    list_list_bool_input = [[True, False], [False, True]]
    encoder = MessagePackEncoder(List[List[bool]])
    list_list_bool_msgpack_bytes = encoder.encode(list_list_bool_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=list_list_bool_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    list_list_bool_output = TypeEngine.to_python_value(ctx, lv, List[List[bool]])
    assert list_list_bool_input == list_list_bool_output

    list_dict_str_int_input = [{"key1": -1, "key2": 2}]
    encoder = MessagePackEncoder(List[Dict[str, int]])
    list_dict_str_int_msgpack_bytes = encoder.encode(list_dict_str_int_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=list_dict_str_int_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    list_dict_str_int_output = TypeEngine.to_python_value(ctx, lv, List[Dict[str, int]])
    assert list_dict_str_int_input == list_dict_str_int_output

    list_dict_str_float_input = [{"key1": 1.0, "key2": -2.0}]
    encoder = MessagePackEncoder(List[Dict[str, float]])
    list_dict_str_float_msgpack_bytes = encoder.encode(list_dict_str_float_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=list_dict_str_float_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    list_dict_str_float_output = TypeEngine.to_python_value(
        ctx, lv, List[Dict[str, float]]
    )
    assert list_dict_str_float_input == list_dict_str_float_output

    list_dict_str_str_input = [{"key1": "a", "key2": "b"}]
    encoder = MessagePackEncoder(List[Dict[str, str]])
    list_dict_str_str_msgpack_bytes = encoder.encode(list_dict_str_str_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=list_dict_str_str_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    list_dict_str_str_output = TypeEngine.to_python_value(ctx, lv, List[Dict[str, str]])
    assert list_dict_str_str_input == list_dict_str_str_output

    list_dict_str_bool_input = [{"key1": True, "key2": False}]
    encoder = MessagePackEncoder(List[Dict[str, bool]])
    list_dict_str_bool_msgpack_bytes = encoder.encode(list_dict_str_bool_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=list_dict_str_bool_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    list_dict_str_bool_output = TypeEngine.to_python_value(
        ctx, lv, List[Dict[str, bool]]
    )
    assert list_dict_str_bool_input == list_dict_str_bool_output

    @dataclass
    class InnerDC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        g: List[Dict[int, bool]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        h: Dict[int, bool] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        i: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        j: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        k: dict = field(default_factory=lambda: {"key": "value"})
        enum_status: Status = field(default=Status.PENDING)

    @dataclass
    class DC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        g: List[Dict[int, bool]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        h: Dict[int, bool] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        i: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        j: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        k: dict = field(default_factory=lambda: {"key": "value"})
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())
        enum_status: Status = field(default=Status.PENDING)

    list_dict_int_inner_dc_input = [{1: InnerDC(), -1: InnerDC(), 0: InnerDC()}]
    encoder = MessagePackEncoder(List[Dict[int, InnerDC]])
    list_dict_int_inner_dc_msgpack_bytes = encoder.encode(list_dict_int_inner_dc_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=list_dict_int_inner_dc_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    list_dict_int_inner_dc_output = TypeEngine.to_python_value(
        ctx, lv, List[Dict[int, InnerDC]]
    )
    assert list_dict_int_inner_dc_input == list_dict_int_inner_dc_output

    list_dict_int_dc_input = [{1: DC(), -1: DC(), 0: DC()}]
    encoder = MessagePackEncoder(List[Dict[int, DC]])
    list_dict_int_dc_msgpack_bytes = encoder.encode(list_dict_int_dc_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=list_dict_int_dc_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    list_dict_int_dc_output = TypeEngine.to_python_value(ctx, lv, List[Dict[int, DC]])
    assert list_dict_int_dc_input == list_dict_int_dc_output

    list_list_inner_dc_input = [[InnerDC(), InnerDC(), InnerDC()]]
    encoder = MessagePackEncoder(List[List[InnerDC]])
    list_list_inner_dc_msgpack_bytes = encoder.encode(list_list_inner_dc_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=list_list_inner_dc_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    list_list_inner_dc_output = TypeEngine.to_python_value(ctx, lv, List[List[InnerDC]])
    assert list_list_inner_dc_input == list_list_inner_dc_output

    list_list_dc_input = [[DC(), DC(), DC()]]
    encoder = MessagePackEncoder(List[List[DC]])
    list_list_dc_msgpack_bytes = encoder.encode(list_list_dc_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=list_list_dc_msgpack_bytes, tag=MESSAGEPACK))
    )
    list_list_dc_output = TypeEngine.to_python_value(ctx, lv, List[List[DC]])
    assert list_list_dc_input == list_list_dc_output


def test_dict_transformer(local_dummy_file, local_dummy_directory):
    ctx = FlyteContextManager.current_context()

    dict_str_int_input = {"key1": 1, "key2": -2}
    encoder = MessagePackEncoder(Dict[str, int])
    dict_str_int_msgpack_bytes = encoder.encode(dict_str_int_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=dict_str_int_msgpack_bytes, tag=MESSAGEPACK))
    )
    dict_str_int_output = TypeEngine.to_python_value(ctx, lv, Dict[str, int])
    assert dict_str_int_input == dict_str_int_output

    dict_str_float_input = {"key1": 1.0, "key2": -2.0}
    encoder = MessagePackEncoder(Dict[str, float])
    dict_str_float_msgpack_bytes = encoder.encode(dict_str_float_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=dict_str_float_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    dict_str_float_output = TypeEngine.to_python_value(ctx, lv, Dict[str, float])
    assert dict_str_float_input == dict_str_float_output

    dict_str_str_input = {"key1": "a", "key2": "b"}
    encoder = MessagePackEncoder(Dict[str, str])
    dict_str_str_msgpack_bytes = encoder.encode(dict_str_str_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=dict_str_str_msgpack_bytes, tag=MESSAGEPACK))
    )
    dict_str_str_output = TypeEngine.to_python_value(ctx, lv, Dict[str, str])
    assert dict_str_str_input == dict_str_str_output

    dict_str_bool_input = {"key1": True, "key2": False}
    encoder = MessagePackEncoder(Dict[str, bool])
    dict_str_bool_msgpack_bytes = encoder.encode(dict_str_bool_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=dict_str_bool_msgpack_bytes, tag=MESSAGEPACK))
    )
    dict_str_bool_output = TypeEngine.to_python_value(ctx, lv, Dict[str, bool])
    assert dict_str_bool_input == dict_str_bool_output

    dict_str_list_int_input = {"key1": [1, -2, 3]}
    encoder = MessagePackEncoder(Dict[str, List[int]])
    dict_str_list_int_msgpack_bytes = encoder.encode(dict_str_list_int_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=dict_str_list_int_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    dict_str_list_int_output = TypeEngine.to_python_value(ctx, lv, Dict[str, List[int]])
    assert dict_str_list_int_input == dict_str_list_int_output

    dict_str_dict_str_int_input = {"key1": {"subkey1": 1, "subkey2": -2}}
    encoder = MessagePackEncoder(Dict[str, Dict[str, int]])
    dict_str_dict_str_int_msgpack_bytes = encoder.encode(dict_str_dict_str_int_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=dict_str_dict_str_int_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    dict_str_dict_str_int_output = TypeEngine.to_python_value(
        ctx, lv, Dict[str, Dict[str, int]]
    )
    assert dict_str_dict_str_int_input == dict_str_dict_str_int_output

    dict_str_dict_str_list_int_input = {
        "key1": {"subkey1": [1, -2], "subkey2": [-3, 4]}
    }
    encoder = MessagePackEncoder(Dict[str, Dict[str, List[int]]])
    dict_str_dict_str_list_int_msgpack_bytes = encoder.encode(
        dict_str_dict_str_list_int_input
    )
    lv = Literal(
        scalar=Scalar(
            binary=Binary(
                value=dict_str_dict_str_list_int_msgpack_bytes, tag=MESSAGEPACK
            )
        )
    )
    dict_str_dict_str_list_int_output = TypeEngine.to_python_value(
        ctx, lv, Dict[str, Dict[str, List[int]]]
    )
    assert dict_str_dict_str_list_int_input == dict_str_dict_str_list_int_output

    dict_str_list_dict_str_int_input = {"key1": [{"subkey1": -1}, {"subkey2": 2}]}
    encoder = MessagePackEncoder(Dict[str, List[Dict[str, int]]])
    dict_str_list_dict_str_int_msgpack_bytes = encoder.encode(
        dict_str_list_dict_str_int_input
    )
    lv = Literal(
        scalar=Scalar(
            binary=Binary(
                value=dict_str_list_dict_str_int_msgpack_bytes, tag=MESSAGEPACK
            )
        )
    )
    dict_str_list_dict_str_int_output = TypeEngine.to_python_value(
        ctx, lv, Dict[str, List[Dict[str, int]]]
    )
    assert dict_str_list_dict_str_int_input == dict_str_list_dict_str_int_output

    # non-strict types
    dict_int_str_input = {1: "a", -2: "b"}
    encoder = MessagePackEncoder(dict)
    dict_int_str_msgpack_bytes = encoder.encode(dict_int_str_input)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=dict_int_str_msgpack_bytes, tag=MESSAGEPACK))
    )
    dict_int_str_output = TypeEngine.to_python_value(ctx, lv, dict)
    assert dict_int_str_input == dict_int_str_output

    dict_int_dict_int_list_int_input = {1: {-2: [1, -2]}, -3: {4: [-3, 4]}}
    encoder = MessagePackEncoder(Dict[int, Dict[int, List[int]]])
    dict_int_dict_int_list_int_msgpack_bytes = encoder.encode(
        dict_int_dict_int_list_int_input
    )
    lv = Literal(
        scalar=Scalar(
            binary=Binary(
                value=dict_int_dict_int_list_int_msgpack_bytes, tag=MESSAGEPACK
            )
        )
    )
    dict_int_dict_int_list_int_output = TypeEngine.to_python_value(
        ctx, lv, Dict[int, Dict[int, List[int]]]
    )
    assert dict_int_dict_int_list_int_input == dict_int_dict_int_list_int_output

    @dataclass
    class InnerDC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        g: List[Dict[int, bool]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        h: Dict[int, bool] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        i: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        j: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        k: dict = field(default_factory=lambda: {"key": "value"})
        enum_status: Status = field(default=Status.PENDING)

    @dataclass
    class DC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        g: List[Dict[int, bool]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        h: Dict[int, bool] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        i: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        j: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        k: dict = field(default_factory=lambda: {"key": "value"})
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())
        enum_status: Status = field(default=Status.PENDING)

    dict_int_inner_dc_input = {1: InnerDC(), -2: InnerDC(), 0: InnerDC()}
    encoder = MessagePackEncoder(Dict[int, InnerDC])
    dict_int_inner_dc_msgpack_bytes = encoder.encode(dict_int_inner_dc_input)
    lv = Literal(
        scalar=Scalar(
            binary=Binary(value=dict_int_inner_dc_msgpack_bytes, tag=MESSAGEPACK)
        )
    )
    dict_int_inner_dc_output = TypeEngine.to_python_value(ctx, lv, Dict[int, InnerDC])
    assert dict_int_inner_dc_input == dict_int_inner_dc_output

    dict_int_dc = {1: DC(), -2: DC(), 0: DC()}
    encoder = MessagePackEncoder(Dict[int, DC])
    dict_int_dc_msgpack_bytes = encoder.encode(dict_int_dc)
    lv = Literal(
        scalar=Scalar(binary=Binary(value=dict_int_dc_msgpack_bytes, tag=MESSAGEPACK))
    )
    dict_int_dc_output = TypeEngine.to_python_value(ctx, lv, Dict[int, DC])
    assert dict_int_dc == dict_int_dc_output


@pytest.fixture
def local_dummy_file():
    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "w") as tmp:
            tmp.write("Hello FlyteFile")
        yield path
    finally:
        os.remove(path)


@pytest.fixture
def local_dummy_directory():
    temp_dir = tempfile.TemporaryDirectory()
    try:
        with open(os.path.join(temp_dir.name, "file"), "w") as tmp:
            tmp.write("Hello FlyteDirectory")
        yield temp_dir.name
    finally:
        temp_dir.cleanup()


def test_flytetypes_in_dataclass_wf(local_dummy_file, local_dummy_directory):
    @dataclass
    class InnerDC:
        flytefile: FlyteFile = field(
            default_factory=lambda: FlyteFile(local_dummy_file)
        )
        flytedir: FlyteDirectory = field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )

    @dataclass
    class DC:
        flytefile: FlyteFile = field(
            default_factory=lambda: FlyteFile(local_dummy_file)
        )
        flytedir: FlyteDirectory = field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())

    @task
    def t1(path: FlyteFile) -> FlyteFile:
        return path

    @task
    def t2(path: FlyteDirectory) -> FlyteDirectory:
        return path

    @workflow
    def wf(dc: DC) -> (FlyteFile, FlyteFile, FlyteDirectory, FlyteDirectory):
        f1 = t1(path=dc.flytefile)
        f2 = t1(path=dc.inner_dc.flytefile)
        d1 = t2(path=dc.flytedir)
        d2 = t2(path=dc.inner_dc.flytedir)
        return f1, f2, d1, d2

    o1, o2, o3, o4 = wf(dc=DC())
    with open(o1, "r") as fh:
        assert fh.read() == "Hello FlyteFile"

    with open(o2, "r") as fh:
        assert fh.read() == "Hello FlyteFile"

    with open(os.path.join(o3, "file"), "r") as fh:
        assert fh.read() == "Hello FlyteDirectory"

    with open(os.path.join(o4, "file"), "r") as fh:
        assert fh.read() == "Hello FlyteDirectory"


def test_all_types_in_dataclass_wf(local_dummy_file, local_dummy_directory):
    @dataclass
    class InnerDC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(
            default_factory=lambda: [FlyteFile(local_dummy_file)]
        )
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Dict[int, bool] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Dict[int, FlyteFile] = field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        enum_status: Status = field(default=Status.PENDING)

    @dataclass
    class DC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(
            default_factory=lambda: [
                FlyteFile(local_dummy_file),
            ]
        )
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Dict[int, bool] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Dict[int, FlyteFile] = field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())
        enum_status: Status = field(default=Status.PENDING)

    @task
    def t_inner(inner_dc: InnerDC):
        assert type(inner_dc) is InnerDC

        # f: List[FlyteFile]
        for ff in inner_dc.f:
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # j: Dict[int, FlyteFile]
        for _, ff in inner_dc.j.items():
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # n: FlyteFile
        assert type(inner_dc.n) is FlyteFile
        with open(inner_dc.n, "r") as f:
            assert f.read() == "Hello FlyteFile"
        # o: FlyteDirectory
        assert type(inner_dc.o) is FlyteDirectory
        assert not inner_dc.o.downloaded
        with open(os.path.join(inner_dc.o, "file"), "r") as fh:
            assert fh.read() == "Hello FlyteDirectory"
        assert inner_dc.o.downloaded

        # enum: Status
        assert inner_dc.enum_status == Status.PENDING

    @task
    def t_test_all_attributes(
        a: int,
        b: float,
        c: str,
        d: bool,
        e: List[int],
        f: List[FlyteFile],
        g: List[List[int]],
        h: List[Dict[int, bool]],
        i: Dict[int, bool],
        j: Dict[int, FlyteFile],
        k: Dict[int, List[int]],
        l: Dict[int, Dict[int, int]],
        m: dict,
        n: FlyteFile,
        o: FlyteDirectory,
        enum_status: Status,
    ):
        # Strict type checks for simple types
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"

        # Strict type checks for List[int]
        assert isinstance(e, list) and all(
            isinstance(i, int) for i in e
        ), "e is not List[int]"

        # Strict type checks for List[FlyteFile]
        assert isinstance(f, list) and all(
            isinstance(i, FlyteFile) for i in f
        ), "f is not List[FlyteFile]"

        # Strict type checks for List[List[int]]
        assert isinstance(g, list) and all(
            isinstance(i, list) and all(isinstance(j, int) for j in i) for i in g
        ), "g is not List[List[int]]"

        # Strict type checks for List[Dict[int, bool]]
        assert isinstance(h, list) and all(
            isinstance(i, dict)
            and all(isinstance(k, int) and isinstance(v, bool) for k, v in i.items())
            for i in h
        ), "h is not List[Dict[int, bool]]"

        # Strict type checks for Dict[int, bool]
        assert isinstance(i, dict) and all(
            isinstance(k, int) and isinstance(v, bool) for k, v in i.items()
        ), "i is not Dict[int, bool]"

        # Strict type checks for Dict[int, FlyteFile]
        assert isinstance(j, dict) and all(
            isinstance(k, int) and isinstance(v, FlyteFile) for k, v in j.items()
        ), "j is not Dict[int, FlyteFile]"

        # Strict type checks for Dict[int, List[int]]
        assert isinstance(k, dict) and all(
            isinstance(k, int)
            and isinstance(v, list)
            and all(isinstance(i, int) for i in v)
            for k, v in k.items()
        ), "k is not Dict[int, List[int]]"

        # Strict type checks for Dict[int, Dict[int, int]]
        assert isinstance(l, dict) and all(
            isinstance(k, int)
            and isinstance(v, dict)
            and all(
                isinstance(sub_k, int) and isinstance(sub_v, int)
                for sub_k, sub_v in v.items()
            )
            for k, v in l.items()
        ), "l is not Dict[int, Dict[int, int]]"

        # Strict type check for a generic dict
        assert isinstance(m, dict), "m is not dict"

        # Strict type check for FlyteFile
        assert isinstance(n, FlyteFile), "n is not FlyteFile"

        # Strict type check for FlyteDirectory
        assert isinstance(o, FlyteDirectory), "o is not FlyteDirectory"

        # Strict type check for Enum
        assert isinstance(enum_status, Status), "enum_status is not Status"

    @workflow
    def wf(dc: DC):
        t_inner(dc.inner_dc)
        t_test_all_attributes(
            a=dc.a,
            b=dc.b,
            c=dc.c,
            d=dc.d,
            e=dc.e,
            f=dc.f,
            g=dc.g,
            h=dc.h,
            i=dc.i,
            j=dc.j,
            k=dc.k,
            l=dc.l,
            m=dc.m,
            n=dc.n,
            o=dc.o,
            enum_status=dc.enum_status,
        )

        t_test_all_attributes(
            a=dc.inner_dc.a,
            b=dc.inner_dc.b,
            c=dc.inner_dc.c,
            d=dc.inner_dc.d,
            e=dc.inner_dc.e,
            f=dc.inner_dc.f,
            g=dc.inner_dc.g,
            h=dc.inner_dc.h,
            i=dc.inner_dc.i,
            j=dc.inner_dc.j,
            k=dc.inner_dc.k,
            l=dc.inner_dc.l,
            m=dc.inner_dc.m,
            n=dc.inner_dc.n,
            o=dc.inner_dc.o,
            enum_status=dc.inner_dc.enum_status,
        )

    wf(dc=DC())


def test_backward_compatible_with_dataclass_in_protobuf_struct(
    local_dummy_file, local_dummy_directory
):
    # Flyte Console will send the input data as protobuf Struct
    # This test also test how Flyte Console with attribute access on the
    # Struct object

    @dataclass
    class InnerDC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(
            default_factory=lambda: [FlyteFile(local_dummy_file)]
        )
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Dict[int, bool] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Dict[int, FlyteFile] = field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        enum_status: Status = field(default=Status.PENDING)

    @dataclass
    class DC:
        a: int = -1
        b: float = 2.1
        c: str = "Hello, Flyte"
        d: bool = False
        e: List[int] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: List[FlyteFile] = field(
            default_factory=lambda: [
                FlyteFile(local_dummy_file),
            ]
        )
        g: List[List[int]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: List[Dict[int, bool]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Dict[int, bool] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Dict[int, FlyteFile] = field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Dict[int, List[int]] = field(default_factory=lambda: {0: [0, 1, -1]})
        l: Dict[int, Dict[int, int]] = field(default_factory=lambda: {1: {-1: 0}})
        m: dict = field(default_factory=lambda: {"key": "value"})
        n: FlyteFile = field(default_factory=lambda: FlyteFile(local_dummy_file))
        o: FlyteDirectory = field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        inner_dc: InnerDC = field(default_factory=lambda: InnerDC())
        enum_status: Status = field(default=Status.PENDING)

    def t_inner(inner_dc: InnerDC):
        assert type(inner_dc) is InnerDC

        # f: List[FlyteFile]
        for ff in inner_dc.f:
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # j: Dict[int, FlyteFile]
        for _, ff in inner_dc.j.items():
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # n: FlyteFile
        assert type(inner_dc.n) is FlyteFile
        with open(inner_dc.n, "r") as f:
            assert f.read() == "Hello FlyteFile"
        # o: FlyteDirectory
        assert type(inner_dc.o) is FlyteDirectory
        assert not inner_dc.o.downloaded
        with open(os.path.join(inner_dc.o, "file"), "r") as fh:
            assert fh.read() == "Hello FlyteDirectory"
        assert inner_dc.o.downloaded

        # enum: Status
        assert inner_dc.enum_status == Status.PENDING

    def t_test_all_attributes(
        a: int,
        b: float,
        c: str,
        d: bool,
        e: List[int],
        f: List[FlyteFile],
        g: List[List[int]],
        h: List[Dict[int, bool]],
        i: Dict[int, bool],
        j: Dict[int, FlyteFile],
        k: Dict[int, List[int]],
        l: Dict[int, Dict[int, int]],
        m: dict,
        n: FlyteFile,
        o: FlyteDirectory,
        enum_status: Status,
    ):
        # Strict type checks for simple types
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"

        # Strict type checks for List[int]
        assert isinstance(e, list) and all(
            isinstance(i, int) for i in e
        ), "e is not List[int]"

        # Strict type checks for List[FlyteFile]
        assert isinstance(f, list) and all(
            isinstance(i, FlyteFile) for i in f
        ), "f is not List[FlyteFile]"

        # Strict type checks for List[List[int]]
        assert isinstance(g, list) and all(
            isinstance(i, list) and all(isinstance(j, int) for j in i) for i in g
        ), "g is not List[List[int]]"

        # Strict type checks for List[Dict[int, bool]]
        assert isinstance(h, list) and all(
            isinstance(i, dict)
            and all(isinstance(k, int) and isinstance(v, bool) for k, v in i.items())
            for i in h
        ), "h is not List[Dict[int, bool]]"

        # Strict type checks for Dict[int, bool]
        assert isinstance(i, dict) and all(
            isinstance(k, int) and isinstance(v, bool) for k, v in i.items()
        ), "i is not Dict[int, bool]"

        # Strict type checks for Dict[int, FlyteFile]
        assert isinstance(j, dict) and all(
            isinstance(k, int) and isinstance(v, FlyteFile) for k, v in j.items()
        ), "j is not Dict[int, FlyteFile]"

        # Strict type checks for Dict[int, List[int]]
        assert isinstance(k, dict) and all(
            isinstance(k, int)
            and isinstance(v, list)
            and all(isinstance(i, int) for i in v)
            for k, v in k.items()
        ), "k is not Dict[int, List[int]]"

        # Strict type checks for Dict[int, Dict[int, int]]
        assert isinstance(l, dict) and all(
            isinstance(k, int)
            and isinstance(v, dict)
            and all(
                isinstance(sub_k, int) and isinstance(sub_v, int)
                for sub_k, sub_v in v.items()
            )
            for k, v in l.items()
        ), "l is not Dict[int, Dict[int, int]]"

        # Strict type check for a generic dict
        assert isinstance(m, dict), "m is not dict"

        # Strict type check for FlyteFile
        assert isinstance(n, FlyteFile), "n is not FlyteFile"

        # Strict type check for FlyteDirectory
        assert isinstance(o, FlyteDirectory), "o is not FlyteDirectory"

        # Strict type check for Enum
        assert isinstance(enum_status, Status), "enum_status is not Status"

    # This is the old dataclass serialization behavior.
    # https://github.com/flyteorg/flytekit/blob/94786cfd4a5c2c3b23ac29dcd6f04d0553fa1beb/flytekit/core/type_engine.py#L702-L728
    dc = DC()
    DataclassTransformer()._make_dataclass_serializable(python_val=dc, python_type=DC)
    json_str = JSONEncoder(DC).encode(dc)
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct()))
    )

    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(), upstream_output, DC
    )
    t_inner(downstream_input.inner_dc)
    t_test_all_attributes(
        a=downstream_input.a,
        b=downstream_input.b,
        c=downstream_input.c,
        d=downstream_input.d,
        e=downstream_input.e,
        f=downstream_input.f,
        g=downstream_input.g,
        h=downstream_input.h,
        i=downstream_input.i,
        j=downstream_input.j,
        k=downstream_input.k,
        l=downstream_input.l,
        m=downstream_input.m,
        n=downstream_input.n,
        o=downstream_input.o,
        enum_status=downstream_input.enum_status,
    )
    t_test_all_attributes(
        a=downstream_input.inner_dc.a,
        b=downstream_input.inner_dc.b,
        c=downstream_input.inner_dc.c,
        d=downstream_input.inner_dc.d,
        e=downstream_input.inner_dc.e,
        f=downstream_input.inner_dc.f,
        g=downstream_input.inner_dc.g,
        h=downstream_input.inner_dc.h,
        i=downstream_input.inner_dc.i,
        j=downstream_input.inner_dc.j,
        k=downstream_input.inner_dc.k,
        l=downstream_input.inner_dc.l,
        m=downstream_input.inner_dc.m,
        n=downstream_input.inner_dc.n,
        o=downstream_input.inner_dc.o,
        enum_status=downstream_input.inner_dc.enum_status,
    )


def test_backward_compatible_with_untyped_dict_in_protobuf_struct():
    # This is the old dataclass serialization behavior.
    # https://github.com/flyteorg/flytekit/blob/94786cfd4a5c2c3b23ac29dcd6f04d0553fa1beb/flytekit/core/type_engine.py#L1699-L1720
    dict_input = {
        "a": 1.0,
        "b": "str",
        "c": False,
        "d": True,
        "e": [1.0, 2.0, -1.0, 0.0],
        "f": {"a": {"b": [1.0, -1.0]}},
    }

    upstream_output = Literal(
        scalar=Scalar(
            generic=_json_format.Parse(json.dumps(dict_input), _struct.Struct())
        ),
        metadata={"format": "json"},
    )

    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(), upstream_output, dict
    )
    assert dict_input == downstream_input


def test_flyte_console_input_with_typed_dict_with_flyte_types_in_dataclass_in_protobuf_struct(
    local_dummy_file, local_dummy_directory
):
    # TODO: We can add more nested cases for non-flyte types.
    """
    Handles the case where Flyte Console provides input as a protobuf struct.
    When resolving an attribute like 'dc.dict_int_ff', FlytePropeller retrieves a dictionary.
    Mashumaro's decoder can convert this dictionary to the expected Python object if the correct type is provided.
    Since Flyte Types handle their own deserialization, the dictionary is automatically converted to the expected Python object.

    Example Code:
    @dataclass
    class DC:
        dict_int_ff: Dict[int, FlyteFile]

    @workflow
    def wf(dc: DC):
        t_ff(dc.dict_int_ff)

    Life Cycle:
    json str            -> protobuf struct         -> resolved protobuf struct   -> dictionary                -> expected Python object
    (console user input)   (console output)           (propeller)                   (flytekit dict transformer)  (mashumaro decoder)

    Related PR:
    - Title: Override Dataclass Serialization/Deserialization Behavior for FlyteTypes via Mashumaro
    - Link: https://github.com/flyteorg/flytekit/pull/2554
    - Title: Binary IDL With MessagePack
    - Link: https://github.com/flyteorg/flytekit/pull/2760
    """

    dict_int_flyte_file = {"1": {"path": local_dummy_file}}
    json_str = json.dumps(dict_int_flyte_file)
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())),
        metadata={"format": "json"},
    )
    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(), upstream_output, Dict[int, FlyteFile]
    )
    assert downstream_input == {1: FlyteFile(local_dummy_file)}

    # FlyteConsole trims trailing ".0" when converting float-like strings
    dict_float_flyte_file = {"1": {"path": local_dummy_file}}
    json_str = json.dumps(dict_float_flyte_file)
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())),
        metadata={"format": "json"},
    )
    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(), upstream_output, Dict[float, FlyteFile]
    )
    assert downstream_input == {1.0: FlyteFile(local_dummy_file)}

    dict_float_flyte_file = {"1.0": {"path": local_dummy_file}}
    json_str = json.dumps(dict_float_flyte_file)
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())),
        metadata={"format": "json"},
    )
    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(), upstream_output, Dict[float, FlyteFile]
    )
    assert downstream_input == {1.0: FlyteFile(local_dummy_file)}

    dict_str_flyte_file = {"1": {"path": local_dummy_file}}
    json_str = json.dumps(dict_str_flyte_file)
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())),
        metadata={"format": "json"},
    )
    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(), upstream_output, Dict[str, FlyteFile]
    )
    assert downstream_input == {"1": FlyteFile(local_dummy_file)}

    dict_int_flyte_directory = {"1": {"path": local_dummy_directory}}
    json_str = json.dumps(dict_int_flyte_directory)
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())),
        metadata={"format": "json"},
    )
    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(),
        upstream_output,
        Dict[int, FlyteDirectory],
    )
    assert downstream_input == {1: FlyteDirectory(local_dummy_directory)}

    # FlyteConsole trims trailing ".0" when converting float-like strings
    dict_float_flyte_directory = {"1": {"path": local_dummy_directory}}
    json_str = json.dumps(dict_float_flyte_directory)
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())),
        metadata={"format": "json"},
    )
    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(),
        upstream_output,
        Dict[float, FlyteDirectory],
    )
    assert downstream_input == {1.0: FlyteDirectory(local_dummy_directory)}

    dict_float_flyte_directory = {"1.0": {"path": local_dummy_directory}}
    json_str = json.dumps(dict_float_flyte_directory)
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())),
        metadata={"format": "json"},
    )
    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(),
        upstream_output,
        Dict[float, FlyteDirectory],
    )
    assert downstream_input == {1.0: FlyteDirectory(local_dummy_directory)}

    dict_str_flyte_file = {"1": {"path": local_dummy_file}}
    json_str = json.dumps(dict_str_flyte_file)
    upstream_output = Literal(
        scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())),
        metadata={"format": "json"},
    )
    downstream_input = TypeEngine.to_python_value(
        FlyteContextManager.current_context(), upstream_output, Dict[str, FlyteFile]
    )
    assert downstream_input == {"1": FlyteFile(local_dummy_file)}


def test_all_types_with_optional_in_dataclass_wf(
    local_dummy_file, local_dummy_directory
):
    @dataclass
    class InnerDC:
        a: Optional[int] = -1
        b: Optional[float] = 2.1
        c: Optional[str] = "Hello, Flyte"
        d: Optional[bool] = False
        e: Optional[List[int]] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: Optional[List[FlyteFile]] = field(
            default_factory=lambda: [FlyteFile(local_dummy_file)]
        )
        g: Optional[List[List[int]]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: Optional[List[Dict[int, bool]]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Optional[Dict[int, bool]] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Optional[Dict[int, FlyteFile]] = field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Optional[Dict[int, List[int]]] = field(
            default_factory=lambda: {0: [0, 1, -1]}
        )
        l: Optional[Dict[int, Dict[int, int]]] = field(
            default_factory=lambda: {1: {-1: 0}}
        )
        m: Optional[dict] = field(default_factory=lambda: {"key": "value"})
        n: Optional[FlyteFile] = field(
            default_factory=lambda: FlyteFile(local_dummy_file)
        )
        o: Optional[FlyteDirectory] = field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        enum_status: Optional[Status] = field(default=Status.PENDING)

    @dataclass
    class DC:
        a: Optional[int] = -1
        b: Optional[float] = 2.1
        c: Optional[str] = "Hello, Flyte"
        d: Optional[bool] = False
        e: Optional[List[int]] = field(default_factory=lambda: [0, 1, 2, -1, -2])
        f: Optional[List[FlyteFile]] = field(
            default_factory=lambda: [FlyteFile(local_dummy_file)]
        )
        g: Optional[List[List[int]]] = field(default_factory=lambda: [[0], [1], [-1]])
        h: Optional[List[Dict[int, bool]]] = field(
            default_factory=lambda: [{0: False}, {1: True}, {-1: True}]
        )
        i: Optional[Dict[int, bool]] = field(
            default_factory=lambda: {0: False, 1: True, -1: False}
        )
        j: Optional[Dict[int, FlyteFile]] = field(
            default_factory=lambda: {
                0: FlyteFile(local_dummy_file),
                1: FlyteFile(local_dummy_file),
                -1: FlyteFile(local_dummy_file),
            }
        )
        k: Optional[Dict[int, List[int]]] = field(
            default_factory=lambda: {0: [0, 1, -1]}
        )
        l: Optional[Dict[int, Dict[int, int]]] = field(
            default_factory=lambda: {1: {-1: 0}}
        )
        m: Optional[dict] = field(default_factory=lambda: {"key": "value"})
        n: Optional[FlyteFile] = field(
            default_factory=lambda: FlyteFile(local_dummy_file)
        )
        o: Optional[FlyteDirectory] = field(
            default_factory=lambda: FlyteDirectory(local_dummy_directory)
        )
        inner_dc: Optional[InnerDC] = field(default_factory=lambda: InnerDC())
        enum_status: Optional[Status] = field(default=Status.PENDING)

    @task
    def t_inner(inner_dc: InnerDC):
        assert type(inner_dc) is InnerDC

        # f: List[FlyteFile]
        for ff in inner_dc.f:  # type: ignore
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # j: Dict[int, FlyteFile]
        for _, ff in inner_dc.j.items():  # type: ignore
            assert type(ff) is FlyteFile
            with open(ff, "r") as f:
                assert f.read() == "Hello FlyteFile"
        # n: FlyteFile
        assert type(inner_dc.n) is FlyteFile
        with open(inner_dc.n, "r") as f:
            assert f.read() == "Hello FlyteFile"
        # o: FlyteDirectory
        assert type(inner_dc.o) is FlyteDirectory
        assert not inner_dc.o.downloaded
        with open(os.path.join(inner_dc.o, "file"), "r") as fh:
            assert fh.read() == "Hello FlyteDirectory"
        assert inner_dc.o.downloaded

        # enum: Status
        assert inner_dc.enum_status == Status.PENDING

    @task
    def t_test_all_attributes(
        a: Optional[int],
        b: Optional[float],
        c: Optional[str],
        d: Optional[bool],
        e: Optional[List[int]],
        f: Optional[List[FlyteFile]],
        g: Optional[List[List[int]]],
        h: Optional[List[Dict[int, bool]]],
        i: Optional[Dict[int, bool]],
        j: Optional[Dict[int, FlyteFile]],
        k: Optional[Dict[int, List[int]]],
        l: Optional[Dict[int, Dict[int, int]]],
        m: Optional[dict],
        n: Optional[FlyteFile],
        o: Optional[FlyteDirectory],
        enum_status: Optional[Status],
    ):
        # Strict type checks for simple types
        assert isinstance(a, int), f"a is not int, it's {type(a)}"
        assert a == -1
        assert isinstance(b, float), f"b is not float, it's {type(b)}"
        assert isinstance(c, str), f"c is not str, it's {type(c)}"
        assert isinstance(d, bool), f"d is not bool, it's {type(d)}"

        # Strict type checks for List[int]
        assert isinstance(e, list) and all(
            isinstance(i, int) for i in e
        ), "e is not List[int]"

        # Strict type checks for List[FlyteFile]
        assert isinstance(f, list) and all(
            isinstance(i, FlyteFile) for i in f
        ), "f is not List[FlyteFile]"

        # Strict type checks for List[List[int]]
        assert isinstance(g, list) and all(
            isinstance(i, list) and all(isinstance(j, int) for j in i) for i in g
        ), "g is not List[List[int]]"

        # Strict type checks for List[Dict[int, bool]]
        assert isinstance(h, list) and all(
            isinstance(i, dict)
            and all(isinstance(k, int) and isinstance(v, bool) for k, v in i.items())
            for i in h
        ), "h is not List[Dict[int, bool]]"

        # Strict type checks for Dict[int, bool]
        assert isinstance(i, dict) and all(
            isinstance(k, int) and isinstance(v, bool) for k, v in i.items()
        ), "i is not Dict[int, bool]"

        # Strict type checks for Dict[int, FlyteFile]
        assert isinstance(j, dict) and all(
            isinstance(k, int) and isinstance(v, FlyteFile) for k, v in j.items()
        ), "j is not Dict[int, FlyteFile]"

        # Strict type checks for Dict[int, List[int]]
        assert isinstance(k, dict) and all(
            isinstance(k, int)
            and isinstance(v, list)
            and all(isinstance(i, int) for i in v)
            for k, v in k.items()
        ), "k is not Dict[int, List[int]]"

        # Strict type checks for Dict[int, Dict[int, int]]
        assert isinstance(l, dict) and all(
            isinstance(k, int)
            and isinstance(v, dict)
            and all(
                isinstance(sub_k, int) and isinstance(sub_v, int)
                for sub_k, sub_v in v.items()
            )
            for k, v in l.items()
        ), "l is not Dict[int, Dict[int, int]]"

        # Strict type check for a generic dict
        assert isinstance(m, dict), "m is not dict"

        # Strict type check for FlyteFile
        assert isinstance(n, FlyteFile), "n is not FlyteFile"

        # Strict type check for FlyteDirectory
        assert isinstance(o, FlyteDirectory), "o is not FlyteDirectory"

        # Strict type check for Enum
        assert isinstance(enum_status, Status), "enum_status is not Status"

    @workflow
    def wf(dc: DC):
        t_inner(dc.inner_dc)
        t_test_all_attributes(
            a=dc.a,
            b=dc.b,
            c=dc.c,
            d=dc.d,
            e=dc.e,
            f=dc.f,
            g=dc.g,
            h=dc.h,
            i=dc.i,
            j=dc.j,
            k=dc.k,
            l=dc.l,
            m=dc.m,
            n=dc.n,
            o=dc.o,
            enum_status=dc.enum_status,
        )

    wf(dc=DC())


def test_all_types_with_optional_and_none_in_dataclass_wf():
    @dataclass
    class InnerDC:
        a: Optional[int] = None
        b: Optional[float] = None
        c: Optional[str] = None
        d: Optional[bool] = None
        e: Optional[List[int]] = None
        f: Optional[List[FlyteFile]] = None
        g: Optional[List[List[int]]] = None
        h: Optional[List[Dict[int, bool]]] = None
        i: Optional[Dict[int, bool]] = None
        j: Optional[Dict[int, FlyteFile]] = None
        k: Optional[Dict[int, List[int]]] = None
        l: Optional[Dict[int, Dict[int, int]]] = None
        m: Optional[dict] = None
        n: Optional[FlyteFile] = None
        o: Optional[FlyteDirectory] = None
        enum_status: Optional[Status] = None

    @dataclass
    class DC:
        a: Optional[int] = None
        b: Optional[float] = None
        c: Optional[str] = None
        d: Optional[bool] = None
        e: Optional[List[int]] = None
        f: Optional[List[FlyteFile]] = None
        g: Optional[List[List[int]]] = None
        h: Optional[List[Dict[int, bool]]] = None
        i: Optional[Dict[int, bool]] = None
        j: Optional[Dict[int, FlyteFile]] = None
        k: Optional[Dict[int, List[int]]] = None
        l: Optional[Dict[int, Dict[int, int]]] = None
        m: Optional[dict] = None
        n: Optional[FlyteFile] = None
        o: Optional[FlyteDirectory] = None
        inner_dc: Optional[InnerDC] = None
        enum_status: Optional[Status] = None

    @task
    def t_inner(inner_dc: Optional[InnerDC]):
        return inner_dc

    @task
    def t_test_all_attributes(
        a: Optional[int],
        b: Optional[float],
        c: Optional[str],
        d: Optional[bool],
        e: Optional[List[int]],
        f: Optional[List[FlyteFile]],
        g: Optional[List[List[int]]],
        h: Optional[List[Dict[int, bool]]],
        i: Optional[Dict[int, bool]],
        j: Optional[Dict[int, FlyteFile]],
        k: Optional[Dict[int, List[int]]],
        l: Optional[Dict[int, Dict[int, int]]],
        m: Optional[dict],
        n: Optional[FlyteFile],
        o: Optional[FlyteDirectory],
        enum_status: Optional[Status],
    ):
        return

    @workflow
    def wf(dc: DC):
        t_inner(dc.inner_dc)
        t_test_all_attributes(
            a=dc.a,
            b=dc.b,
            c=dc.c,
            d=dc.d,
            e=dc.e,
            f=dc.f,
            g=dc.g,
            h=dc.h,
            i=dc.i,
            j=dc.j,
            k=dc.k,
            l=dc.l,
            m=dc.m,
            n=dc.n,
            o=dc.o,
            enum_status=dc.enum_status,
        )

    wf(dc=DC())


def test_union_in_dataclass_wf():
    @dataclass
    class DC:
        a: Union[int, bool, str, float]
        b: Union[int, bool, str, float]

    @task
    def add(
        a: Union[int, bool, str, float], b: Union[int, bool, str, float]
    ) -> Union[int, bool, str, float]:
        return a + b  # type: ignore

    @workflow
    def wf(dc: DC) -> Union[int, bool, str, float]:
        return add(dc.a, dc.b)

    assert wf(dc=DC(a=1, b=2)) == 3
    assert wf(dc=DC(a=True, b=False)) == True
    assert wf(dc=DC(a=False, b=False)) == False
    assert wf(dc=DC(a="hello", b="world")) == "helloworld"
    assert wf(dc=DC(a=1.0, b=2.0)) == 3.0

    @task
    def add(dc1: DC, dc2: DC) -> Union[int, bool, str, float]:
        return dc1.a + dc2.b  # type: ignore

    @workflow
    def wf(dc: DC) -> Union[int, bool, str, float]:
        return add(dc, dc)

    assert wf(dc=DC(a=1, b=2)) == 3

    @workflow
    def wf(dc: DC) -> DC:
        return dc

    assert wf(dc=DC(a=1, b=2)) == DC(a=1, b=2)
