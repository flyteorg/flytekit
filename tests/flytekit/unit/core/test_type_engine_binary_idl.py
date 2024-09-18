from datetime import datetime, date, timedelta

import msgpack
from mashumaro.codecs.msgpack import MessagePackEncoder

from flytekit.models.literals import Binary, Literal, Scalar
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine

def test_simple_type_transformer():
    ctx = FlyteContextManager.current_context()

    int_input = 20240918
    encoder = MessagePackEncoder(int)
    int_msgpack_bytes = encoder.encode(int_input)
    lv = Literal(scalar=Scalar(binary=Binary(value=int_msgpack_bytes, tag="msgpack")))
    int_output = TypeEngine.to_python_value(ctx, lv, int)
    assert int_input == int_output

    float_input = 2024.0918
    encoder = MessagePackEncoder(float)
    float_msgpack_bytes = encoder.encode(float_input)
    lv = Literal(scalar=Scalar(binary=Binary(value=float_msgpack_bytes, tag="msgpack")))
    float_output = TypeEngine.to_python_value(ctx, lv, float)
    assert float_input == float_output

    bool_input = True
    encoder = MessagePackEncoder(bool)
    bool_msgpack_bytes = encoder.encode(bool_input)
    lv = Literal(scalar=Scalar(binary=Binary(value=bool_msgpack_bytes, tag="msgpack")))
    bool_output = TypeEngine.to_python_value(ctx, lv, bool)
    assert bool_input == bool_output

    bool_input = False
    bool_msgpack_bytes = encoder.encode(bool_input)
    lv = Literal(scalar=Scalar(binary=Binary(value=bool_msgpack_bytes, tag="msgpack")))
    bool_output = TypeEngine.to_python_value(ctx, lv, bool)
    assert bool_input == bool_output

    str_input = "hello"
    encoder = MessagePackEncoder(str)
    str_msgpack_bytes = encoder.encode(str_input)
    lv = Literal(scalar=Scalar(binary=Binary(value=str_msgpack_bytes, tag="msgpack")))
    str_output = TypeEngine.to_python_value(ctx, lv, str)
    assert str_input == str_output

    datetime_input = datetime.now()
    encoder = MessagePackEncoder(datetime)
    datetime_msgpack_bytes = encoder.encode(datetime_input)
    lv = Literal(scalar=Scalar(binary=Binary(value=datetime_msgpack_bytes, tag="msgpack")))
    datetime_output = TypeEngine.to_python_value(ctx, lv, datetime)
    assert datetime_input == datetime_output

    date_input = date.today()
    encoder = MessagePackEncoder(date)
    date_msgpack_bytes = encoder.encode(date_input)
    lv = Literal(scalar=Scalar(binary=Binary(value=date_msgpack_bytes, tag="msgpack")))
    date_output = TypeEngine.to_python_value(ctx, lv, date)
    assert date_input == date_output

    timedelta_input = timedelta(days=1, seconds=1, microseconds=1, milliseconds=1, minutes=1, hours=1, weeks=1)
    encoder = MessagePackEncoder(timedelta)
    timedelta_msgpack_bytes = encoder.encode(timedelta_input)
    lv = Literal(scalar=Scalar(binary=Binary(value=timedelta_msgpack_bytes, tag="msgpack")))
    timedelta_output = TypeEngine.to_python_value(ctx, lv, timedelta)
    assert timedelta_input == timedelta_output

def test_untyped_dict():
    ctx = FlyteContextManager.current_context()

    dict_inputs = [
        # Basic key-value combinations with int, str, bool, float
        {1: "a", "key": 2.5, True: False, 3.14: 100},
        {"a": 1, 2: "b", 3.5: True, False: 3.1415},

        {
            1: [1, "a", 2.5, False],
            "key_list": ["str", 3.14, True, 42],
            True: [False, 2.718, "test"],
        },

        {
            "nested_dict": {1: 2, "key": "value", True: 3.14, False: "string"},
            3.14: {"pi": 3.14, "e": 2.718, 42: True},
        },

        {
            "list_in_dict": [
                {"inner_dict_1": [1, 2.5, "a"], "inner_dict_2": [True, False, 3.14]},
                [1, 2, 3, {"nested_list_dict": [False, "test"]}],
            ]
        },

        {
            "complex_nested": {
                1: {"nested_dict": {True: [1, "a", 2.5]}},
                "string_key": {False: {3.14: {"deep": [1, "deep_value"]}}},
            }
        },

        {
            "list_of_dicts": [{"a": 1, "b": 2}, {"key1": "value1", "key2": "value2"}],
            10: [{"nested_list": [1, "value", 3.14]}, {"another_list": [True, False]}],
        },

        # More nested combinations of list and dict
        {
            "outer_list": [
                [1, 2, 3],
                {"inner_dict": {"key1": [True, "string", 3.14], "key2": [1, 2.5]}},  # Dict inside list
            ],
            "another_dict": {"key1": {"subkey": [1, 2, "str"]}, "key2": [False, 3.14, "test"]},
        },
    ]

    for dict_input in dict_inputs:
        dict_msgpack_bytes = msgpack.dumps(dict_input)
        lv = Literal(scalar=Scalar(binary=Binary(value=dict_msgpack_bytes, tag="msgpack")))
        dict_output = TypeEngine.to_python_value(ctx, lv, dict)
        assert dict_input == dict_output
