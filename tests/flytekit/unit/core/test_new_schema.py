import pandas as pd
import pyarrow as pa
from datetime import datetime, timedelta
from pathlib import Path


class NewFlyteSchemaIDL():
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs


class NewFlyteSchemaLiteralMetadata():
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs


def test_scenario_one():
    """
    Output/to_literal - Scenario 1:
    User returns a some_lib.DataFrame (most likely pandas, but doesn't have to be)
    The different options below show manually what the code might do depending on the output signature
    """

    Path("/tmp/test/").mkdir(exist_ok=True)
    now = datetime.now()
    before = now - timedelta(seconds=5)
    df = pd.DataFrame({  # <- Let's say the user returns this DF
        'field0': [1, 2],
        'field1': ['a', 'b'],
        'field2': [now, before],
        'field3': [[2, 3], [4, 5]],
    })

    #
    # Option 1. If the user's return type is a pd.DataFrame
    #
    #   Turn DF into a parquet file as we do now, not involving Arrow.
    #   The format will be stored as parquet in the metadata.
    #   File will be uploaded to s3/gcs as we do today.

    #
    # Option 2. If the user's return type is an Arrow Schema instance
    #

    t1 = pa.int32()
    t2 = pa.string()
    t5 = pa.timestamp('ns')  # This test fails if it's 'ms' because we'd lose precision.
    t6 = pa.list_(t1)

    # This is the return signature, like
    #  def t1() -> ss
    # not sure if this is possible, or if we should support it?
    #  def t1() -> pa.Schema
    ss = pa.schema([
        ('field0', t1),
        ('field1', t2),
        ('field2', t5),
        ('field3', t6)
    ])
    # Assuming there's a schema involved, should we serialize the Arrow schema object?
    schema_bytes = ss.serialize()
    python_bytes = schema_bytes.to_pybytes()

    # Turn into a table to schema check
    # Warning: This can error easily - bad data and such. Can't have 0 values otherwise it'll be an 'object', etc.
    pa_table = pa.Table.from_pandas(df, ss)
    pa_record_batch = pa.record_batch(df, schema=ss)

    # pa.Table has a from_pandas but obviously it won't work for custom DF libraries. Users will have to provide, and
    # flytekit will have to pick up and use, code to convert from custom DFs to pa.Table in this case.

    # Write the data into a file - there's many, many ways to do this.
    with open("/tmp/test/pd_as_arrow_via_new_file", "wb") as fh:
        writer = pa.ipc.new_file(fh, ss)  # schema must be present here
        writer.write_batch(pa_record_batch)
        writer.close()

    with open("/tmp/test/pd_as_arrow_via_rbfw", "wb") as fh:
        writer = pa.ipc.RecordBatchFileWriter(fh, ss)
        writer.write(pa_table)
        writer.close()

    with open("/tmp/test/pd_as_arrow_via_rbfw_br", "wb") as fh:
        writer = pa.ipc.RecordBatchFileWriter(fh, ss)
        writer.write(pa_record_batch)
        writer.close()

    # Upload the file as normal to gcs/s3

    idl_literal_arrow_file = NewFlyteSchemaIDL(
        metadata=NewFlyteSchemaLiteralMetadata(format="pyarrow.file", arrow_schema=python_bytes),
        uri = "s3://blah",
        schema="xyz",  # where xyz is the pyarrow schema converted into a Flyte Schema.
    )

    # Arrow has two file formats - one for files, and one for streaming, but streaming can also be written to a file
    # Not sure if the other two options above also work, they probably do. not showing them.
    with open("/tmp/test/pd_as_arrow_via_rbsw", "wb") as fh:
        writer = pa.ipc.RecordBatchStreamWriter(fh, ss)
        writer.write(pa_table)
        writer.close()

    idl_literal_arrow_stream = NewFlyteSchemaIDL(
        metadata=NewFlyteSchemaLiteralMetadata(format="pyarrow.stream"),
        uri="s3://blah",
    )

    # If the user's return type is a FlyteSchema with columns

    # If the user's return type is a FlyteSchema without columns

