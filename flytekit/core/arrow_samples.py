import pandas as pd
import pyarrow as pa
from datetime import datetime, timedelta
from pathlib import Path

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
ss = pa.schema([
    ('field0', t1),
    ('field1', t2),
    ('field2', t5),
    ('field3', t6)
])
########################################################################################################

def tt() -> pa.Schema:
    return pa.Table.from_pandas(df)

def tt2() -> ss:
    return pa.Table.from_pandas(df)



