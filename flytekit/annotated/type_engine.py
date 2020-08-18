import abc as _abc
import datetime as _datetime
import typing
from io import FileIO
from typing import Dict

import six as _six

from flytekit.common.types import primitives as _primitives
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types

# This is now in three different places. This here, the one that the notebook task uses, and the main in that meshes
# with the engine loader. I think we should get rid of the loader (can add back when/if we ever have more than one).
# All three should be merged into the existing one.
SIMPLE_TYPE_LOOKUP_TABLE: Dict[type, _type_models.LiteralType] = {
    int: _primitives.Integer.to_flyte_literal_type(),
    float: _primitives.Float.to_flyte_literal_type(),
    bool: _primitives.Boolean,
    _datetime.datetime: _primitives.Datetime.to_flyte_literal_type(),
    _datetime.timedelta: _primitives.Timedelta.to_flyte_literal_type(),
    str: _primitives.String.to_flyte_literal_type(),
    dict: _primitives.Generic.to_flyte_literal_type(),
    FileIO: _type_models.LiteralType(
        blob=_core_types.BlobType(
            format="",
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
        )
    )
}


def type_to_literal_type(t: type) -> _type_models.LiteralType:
    if t in SIMPLE_TYPE_LOOKUP_TABLE:
        return SIMPLE_TYPE_LOOKUP_TABLE[t]
    return _type_models.LiteralType(
        simple=_type_models.SimpleType.NONE
    )


def outputs(**kwargs) -> tuple:
    """
    Returns an outputs object that strongly binds the types of the outputs retruned by any executable unit (e.g. task,
    workflow).

    :param kwargs:
    :return:

    >>> @task
    >>> def my_task() -> outputs(a=int, b=str):
    >>>    pass
    """

    return typing.NamedTuple("Outputs", **kwargs)
