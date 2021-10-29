import pandas as pd
import pyarrow as pa
import typing
from datetime import datetime, timedelta
from pathlib import Path
from typing_extensions import Annotated

from flytekit import kwtypes
from flytekit.models.types import SchemaType
from flytekit.types.schema import FlyteSchema


class NewFlyteSchemaIDL():
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs


class NewFlyteSchemaLiteralMetadata():
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

########################################################################################################
# This section is all setup - think of this as user code
Path("/tmp/test/").mkdir(exist_ok=True)
now = datetime.now()
before = now - timedelta(seconds=5)

# Let's say the user returns this DF - used in multiple scenarios below
df = pd.DataFrame({
    'field0': [1, 2],
    'field1': ['a', 'b'],
    'field2': [now, before],
    'field3': [[2, 3], [4, 5]],
})

t1 = pa.int32()
t2 = pa.string()
t5 = pa.timestamp('ns')  # This test fails if it's 'ms' because we'd lose precision.
t6 = pa.list_(t1)

# not sure if this is possible, or if we should support it?
#  def t1() -> pa.Schema
my_arrow_schema = pa.schema([
    ('field0', t1),
    ('field1', t2),
    ('field2', t5),
    ('field3', t6)
])
########################################################################################################


def tt() -> pa.Table:
    return pa.Table.from_pandas(df)


# This doesn't pass mypy
# def tt2() -> ss:
#     return pa.Table.from_pandas(df)


# How can we specify a specific pyarrow schema as a return type?


T = typing.TypeVar("T")


class NewFlyteSchema(typing.Generic[T]):
    def __class_getitem__(cls, item: pa.Schema) -> typing.Type[NewFlyteSchema]:
        if item is None:
            return cls
        item_string = str(item)
        item_string = item_string.strip().lstrip("~").lstrip(".")
        if item == "":
            return cls

        class _SpecificFormatClass(NewFlyteSchema):
            # Get the type engine to see this as kind of a generic
            __origin__ = NewFlyteSchema

            @classmethod
            def extension(cls) -> str:
                return item_string

        return _SpecificFormatClass


# Specifying Arrow schema when you know it statically.
CustomSchema = Annotated[FlyteSchema, my_arrow_schema]
CustomPD = Annotated[pd.DataFrame, my_arrow_schema]


def tt3() -> CustomSchema:
    return FlyteSchema(df, external_bytes=serialized_ss, external_type="arrow", metadata={})

