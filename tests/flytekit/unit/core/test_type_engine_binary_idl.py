from datetime import datetime, date, timedelta
from mashumaro.codecs.msgpack import MessagePackEncoder

from flytekit.models.literals import Binary, Literal, Scalar
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine

def test_simple_type_transformer():
    ctx = FlyteContextManager.current_context()

    int_inputs = [1, 2, 20240918, -1, -2, -20240918]
    encoder = MessagePackEncoder(int)
    for int_input in int_inputs:
        int_msgpack_bytes = encoder.encode(int_input)
        lv = Literal(scalar=Scalar(binary=Binary(value=int_msgpack_bytes, tag="msgpack")))
        int_output = TypeEngine.to_python_value(ctx, lv, int)
        assert int_input == int_output

    float_inputs = [2024.0918, 5.0, -2024.0918, -5.0]
    encoder = MessagePackEncoder(float)
    for float_input in float_inputs:
        float_msgpack_bytes = encoder.encode(float_input)
        lv = Literal(scalar=Scalar(binary=Binary(value=float_msgpack_bytes, tag="msgpack")))
        float_output = TypeEngine.to_python_value(ctx, lv, float)
        assert float_input == float_output

    bool_inputs = [True, False]
    encoder = MessagePackEncoder(bool)
    for bool_input in bool_inputs:
        bool_msgpack_bytes = encoder.encode(bool_input)
        lv = Literal(scalar=Scalar(binary=Binary(value=bool_msgpack_bytes, tag="msgpack")))
        bool_output = TypeEngine.to_python_value(ctx, lv, bool)
        assert bool_input == bool_output

    str_inputs = ["hello", "world", "flyte", "kit", "is", "awesome"]
    encoder = MessagePackEncoder(str)
    for str_input in str_inputs:
        str_msgpack_bytes = encoder.encode(str_input)
        lv = Literal(scalar=Scalar(binary=Binary(value=str_msgpack_bytes, tag="msgpack")))
        str_output = TypeEngine.to_python_value(ctx, lv, str)
        assert str_input == str_output

    datetime_inputs = [datetime.now(),
                        datetime(2024, 9, 18),
                        datetime(2024, 9, 18, 1),
                        datetime(2024, 9, 18, 1, 1),
                        datetime(2024, 9, 18, 1, 1, 1),
                        datetime(2024, 9, 18, 1, 1, 1, 1)]
    encoder = MessagePackEncoder(datetime)
    for datetime_input in datetime_inputs:
        datetime_msgpack_bytes = encoder.encode(datetime_input)
        lv = Literal(scalar=Scalar(binary=Binary(value=datetime_msgpack_bytes, tag="msgpack")))
        datetime_output = TypeEngine.to_python_value(ctx, lv, datetime)
        assert datetime_input == datetime_output

    date_inputs = [date.today(),
                   date(2024, 9, 18)]
    encoder = MessagePackEncoder(date)
    for date_input in date_inputs:
        date_msgpack_bytes = encoder.encode(date_input)
        lv = Literal(scalar=Scalar(binary=Binary(value=date_msgpack_bytes, tag="msgpack")))
        date_output = TypeEngine.to_python_value(ctx, lv, date)
        assert date_input == date_output

    timedelta_inputs = [timedelta(days=1),
                        timedelta(days=1, seconds=1),
                        timedelta(days=1, seconds=1, microseconds=1),
                        timedelta(days=1, seconds=1, microseconds=1, milliseconds=1),
                        timedelta(days=1, seconds=1, microseconds=1, milliseconds=1, minutes=1),
                        timedelta(days=1, seconds=1, microseconds=1, milliseconds=1, minutes=1, hours=1),
                        timedelta(days=1, seconds=1, microseconds=1, milliseconds=1, minutes=1, hours=1, weeks=1),
                        timedelta(days=-1, seconds=-1, microseconds=-1, milliseconds=-1, minutes=-1, hours=-1, weeks=-1)]
    encoder = MessagePackEncoder(timedelta)
    for timedelta_input in timedelta_inputs:
        timedelta_msgpack_bytes = encoder.encode(timedelta_input)
        lv = Literal(scalar=Scalar(binary=Binary(value=timedelta_msgpack_bytes, tag="msgpack")))
        timedelta_output = TypeEngine.to_python_value(ctx, lv, timedelta)
        assert timedelta_input == timedelta_output
