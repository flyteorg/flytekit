import os
import re
from typing import Any, Optional

import pandas as pd
import pyarrow as pa
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import LoadJob, LoadJobConfig
from google.cloud.bigquery_storage_v1 import types

from flytekit.types.structured.structured_dataset import (
    FLYTE_DATASET_TRANSFORMER,
    DatasetDecodingHandler,
    DatasetEncodingHandler,
    DatasetFormat,
)


class PandasToBQEncodingHandlers(DatasetEncodingHandler):
    def encode(self, df: Optional[pd.DataFrame] = None, path: Optional[os.PathLike] = None) -> LoadJob:
        table_id = path.split("://", 1)[1].replace(":", ".")
        client = bigquery.Client()
        return client.load_table_from_dataframe(df, table_id)


class BQToPandasDecodingHandler(DatasetDecodingHandler):
    def decode(self, df: Optional[Any] = None, path: Optional[os.PathLike] = None) -> pd.DataFrame:
        # path will be like bq://photo-313016:flyte.new_table1
        _, project_id, dataset_id, table_id = re.split("\\.|://|:", path)
        client = bigquery_storage.BigQueryReadClient()
        table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        parent = "projects/{}".format(project_id)

        requested_session = types.ReadSession(
            table=table,
            data_format=types.DataFormat.ARROW,
        )
        read_session = client.create_read_session(parent=parent, read_session=requested_session)

        stream = read_session.streams[0]
        reader = client.read_rows(stream.name)
        frames = []
        for message in reader.rows().pages:
            frames.append(message.to_dataframe())
        return pd.concat(frames)


class ArrowToBQEncodingHandlers(DatasetEncodingHandler):
    def encode(self, df: Optional[pa.Table] = None, path: Optional[os.PathLike] = None) -> LoadJob:
        return PandasToBQEncodingHandlers().encode(df.to_pandas(), path)


class BQToArrowDecodingHandler(DatasetDecodingHandler):
    def decode(self, df: Optional[Any] = None, path: Optional[os.PathLike] = None) -> pa.Table:
        pd_dataframe = BQToPandasDecodingHandler().decode(path)
        return pa.Table.from_pandas(pd.concat(pd_dataframe))


FLYTE_DATASET_TRANSFORMER.register_handler(pd.DataFrame, DatasetFormat.BIGQUERY, PandasToBQEncodingHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.BIGQUERY, pd.DataFrame, BQToPandasDecodingHandler())
FLYTE_DATASET_TRANSFORMER.register_handler(pa.Table, DatasetFormat.BIGQUERY, ArrowToBQEncodingHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.BIGQUERY, pa.Table, BQToArrowDecodingHandler())
