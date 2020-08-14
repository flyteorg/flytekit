import datetime as _datetime

from flytekit.common.types import primitives as _primitives

notebook_types_map = {
    int: _primitives.Integer,
    bool: _primitives.Boolean,
    float: _primitives.Float,
    str: _primitives.String,
    _datetime.datetime: _primitives.Datetime,
    _datetime.timedelta: _primitives.Timedelta,
}
