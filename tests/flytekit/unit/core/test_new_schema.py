import pandas as pd
import pyarrow as pa
from datetime import datetime, timedelta
from pathlib import Path

from flytekit import kwtypes
from flytekit.models.types import SchemaType
from flytekit.types.schema import FlyteSchema


class NewFlyteSchemaLiteral():
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

# This is the return signature, like
#  def t1() -> ss
# not sure if this is possible, or if we should support it?
#  def t1() -> pa.Schema
arrow_schema = pa.schema([
    ('field0', t1),
    ('field1', t2),
    ('field2', t5),
    ('field3', t6)
])
########################################################################################################

def tt() -> arrow_schema:
    return pa.Table.from_pandas(df)


def test_to_literal_scenario_1():
    """
    Output/to_literal - Scenario 1:

        def t1() -> (see options below):
            return some_lib.DataFrame()

    User returns a some_lib.DataFrame (most likely pandas, but doesn't have to be)
    The different options below show manually what the code might do depending on the output signature
    """
    #
    # Option 1. Return type -> pd.DataFrame
    #
    #   Turn DF into a parquet file as we do now, not involving Arrow.
    #   The format will be stored as parquet in the metadata.
    #   File will be uploaded to s3/gcs as we do today.

    #
    # Option 2 . Return type -> Annotated[StructuredData, arrow_schema]
    #
    # Since there's an Arrow schema involved, we'll serialize the Arrow schema as well.
    schema_bytes = arrow_schema.serialize()
    python_bytes = schema_bytes.to_pybytes()

    # serialize dataframe to a parquet file as we do now
    # upload parquet file

    idl_literal_arrow_file = NewFlyteSchemaLiteral(
        metadata=NewFlyteSchemaLiteralMetadata(format="parquet"),
        uri="s3://blah",
        schema="xyz",  # where xyz is the pyarrow schema converted into a Flyte Schema.
        external_schema=python_bytes,
        external_schema_type="arrow",
    )

    #
    # Option 3. Return type -> StructuredData[col_a=blah, etc.] (with or without columns)
    #

    # serialize dataframe to parquet file.
    # upload parquet file to default location
    # return Flyte literal leaving all arrow information empty.

    idl_literal_arrow_file = NewFlyteSchemaLiteral(
        metadata=NewFlyteSchemaLiteralMetadata(format="parquet"),
        uri="s3://blah",
        schema="some_flyte_schema",  # If cols were specified
    )

    #
    # Option 4. Return type -> Annotated[StructuredData[col_a=blah, etc.], arrow_schema]
    #

    # Handle this the same as 2 and 3 combined. Convert the columns into arrow schema, make sure
    # it matches, raise an exception otherwise. (or maybe the other way around actually.)


def test_to_literal_scenario_2():
    """
    Output/to_literal - Scenario 2:

        def t1() -> StructuredDataset:
            return StructuredDataset(pd, metadata=..., format="parquet", storage_driver="bq", use_uri="bq://blah")

    User returns a flytekit object that encapsulates additional data that instructs the engine on how to handle
    the returned dataframe.
    """
    #
    # Handling can vary depending on the metadata. Following is some code where we infer that we have to first
    # convert from the df to Arrow format data, and then write the file to GCS.
    #
    # Turn into a table to schema check
    # Warning: This can error easily - bad data and such. Can't have 0 values otherwise it'll be an 'object', etc.
    pa_table = pa.Table.from_pandas(df, arrow_schema)
    pa_record_batch = pa.record_batch(df, schema=arrow_schema)

    # pa.Table has a from_pandas but obviously it won't work for custom DF libraries. Users will have to
    # provide (and flytekit will have to pick up and use) code to convert from custom DFs
    # to some intermediate representation (maybe record batch, maybe pa.Table).

    # Write the data into a file - there's many, many ways to do this.
    with open("/tmp/test/pd_as_arrow_via_new_file", "wb") as fh:
        writer = pa.ipc.new_file(fh, arrow_schema)  # schema must be present here
        writer.write_batch(pa_record_batch)
        writer.close()

    with open("/tmp/test/pd_as_arrow_via_rbfw", "wb") as fh:
        writer = pa.ipc.RecordBatchFileWriter(fh, arrow_schema)
        writer.write(pa_table)
        writer.close()

    with open("/tmp/test/pd_as_arrow_via_rbfw_br", "wb") as fh:
        writer = pa.ipc.RecordBatchFileWriter(fh, arrow_schema)
        writer.write(pa_record_batch)
        writer.close()

    # Upload the file as normal to gcs/s3

    idl_literal_arrow_file = NewFlyteSchemaLiteral(
        metadata=NewFlyteSchemaLiteralMetadata(format="pyarrow.file", arrow_schema=python_bytes),
        uri="s3://blah",
        schema="xyz",  # where xyz is the pyarrow schema converted into a Flyte Schema.
    )

    # Arrow has two file formats - one for files, and one for streaming, but streaming can also be written to a file
    # Not sure if the other two options above also work, they probably do. not showing them.
    with open("/tmp/test/pd_as_arrow_via_rbsw", "wb") as fh:
        writer = pa.ipc.RecordBatchStreamWriter(fh, arrow_schema)
        writer.write(pa_table)
        writer.close()

    idl_literal_arrow_stream = NewFlyteSchemaLiteral(
        metadata=NewFlyteSchemaLiteralMetadata(format="pyarrow.stream"),
        uri="s3://blah",
    )


def test_to_python():
    """
    Input/to_python_value - Scenario 2:

        def t1(a: see options below):
            ...
    """
    # In all cases, first we need to extract from the incoming literal metadata -
    #  - format
    #  - uri and hence storage location
    #  - external library schema bytes (Arrow)

    # Option 1:
    # Input is a: some_lib.DataFrame

    # Option 2:
    # Input is a: Annotated[FlyteSchema, arrow_schema]

    # Option 3:
    # Input is a: FlyteSchema


def test_cdjskl():
    """
    export GOOGLE_APPLICATION_CREDENTIALS="/Users/ytong/.ssh/flyte-oauth-60544367078a.json"
    in order to be able to create the client.
    """
    from google.cloud import bigquery

    client = bigquery.Client()
    table_id = "flyte-oauth.taxis.your_table_name"
    pq_file = "/Users/ytong/playground/yellow.parquet"

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, autodetect=True)

    with open(pq_file, "rb") as source_file:
       job = client.load_table_from_file(source_file, table_id, job_config=job_config)


class ParquetToBQ(object):
    @classmethod
    def transform(cls, pq) -> NewFlyteSchemaLiteral:
        ...


class BQToDF(object):
    @classmethod
    def transform(cls, bq_client, bq_location: str) -> pd.DataFrame:
        ...


def test_cjdkjkl():
    """
    What would the handler selection process look like.

    serializers = {int: int_to_arrow}
    """

    metadata = {
        "storage": "bq",
    }




