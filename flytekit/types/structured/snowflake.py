import re
import typing

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from flytekit import FlyteContext
from flytekit.models import literals
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.structured_dataset import (
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetMetadata,
)

SNOWFLAKE = "snowflake"


def get_private_key():
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    import flytekit

    pk_string = flytekit.current_context().secrets.get("snowflake", "private_key.pem", encode_mode="rb")
    p_key = serialization.load_pem_private_key(pk_string, password=None, backend=default_backend())

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return pkb


def _write_to_sf(structured_dataset: StructuredDataset):
    if structured_dataset.uri is None:
        raise ValueError("structured_dataset.uri cannot be None.")

    uri = structured_dataset.uri
    _, user, account, warehouse, database, schema, table = re.split("\\/|://|:", uri)
    df = structured_dataset.dataframe

    conn = snowflake.connector.connect(
        user=user, account=account, private_key=get_private_key(), database=database, schema=schema, warehouse=warehouse
    )

    write_pandas(conn, df, table)


def _read_from_sf(
    flyte_value: literals.StructuredDataset, current_task_metadata: StructuredDatasetMetadata
) -> pd.DataFrame:
    if flyte_value.uri is None:
        raise ValueError("structured_dataset.uri cannot be None.")

    uri = flyte_value.uri
    _, user, account, warehouse, database, schema, table = re.split("\\/|://|:", uri)

    conn = snowflake.connector.connect(
        user=user, account=account, private_key=get_private_key(), database=database, schema=schema, warehouse=warehouse
    )

    cs = conn.cursor()
    cs.execute(f"select * from {table}")

    return cs.fetch_pandas_all()


class PandasToSnowflakeEncodingHandlers(StructuredDatasetEncoder):
    def __init__(self):
        super().__init__(pd.DataFrame, SNOWFLAKE, supported_format="", additional_protocols=["sf"])

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        _write_to_sf(structured_dataset)
        return literals.StructuredDataset(
            uri=typing.cast(str, structured_dataset.uri), metadata=StructuredDatasetMetadata(structured_dataset_type)
        )


class SnowflakeToPandasDecodingHandler(StructuredDatasetDecoder):
    def __init__(self):
        super().__init__(pd.DataFrame, SNOWFLAKE, supported_format="", additional_protocols=["sf"])

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pd.DataFrame:
        return _read_from_sf(flyte_value, current_task_metadata)
