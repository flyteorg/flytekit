import os
import re
import typing
from abc import ABC

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import LoadJob, LoadJobConfig
from google.cloud.bigquery_storage_v1 import types

from flytekit.types.structured.structured_dataset import (
    FLYTE_DATASET_TRANSFORMER,
    DatasetFormat,
    DatasetPersistenceHandler,
    DatasetRetrievalHandler,
)


class ArrowToParquetPersistenceHandlers(DatasetPersistenceHandler, ABC):
    def persist(
        self,
        table: pa.Table,
        to_file: os.PathLike,
    ):
        pq.write_table(table, to_file)


class ArrowToBQPersistenceHandlers(DatasetPersistenceHandler, ABC):
    def persist(self, table: pa.Table, path: str, job_config: typing.Optional[LoadJobConfig] = None) -> LoadJob:
        table_id = path.split("://", 1)[1].replace(":", ".")
        client = bigquery.Client()
        return client.load_table_from_dataframe(table.to_pandas(), table_id, job_config=job_config)


class ParquetToArrowRetrievalHandler(DatasetRetrievalHandler, ABC):
    def retrieve(self, path: str) -> pa.Table:
        return pq.read_table(path)


class BQToArrowRetrievalHandler(DatasetRetrievalHandler, ABC):
    def retrieve(self, path: str, **kwargs) -> pa.Table:
        # path will be like bq://photo-313016:flyte.new_table1
        _, project_id, dataset_id, table_id = re.split("\.|://|:", path)
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
        return pa.Table.from_pandas(pd.concat(frames))


FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.PARQUET, pa.Table, ParquetToArrowRetrievalHandler())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.BIGQUERY, pa.Table, BQToArrowRetrievalHandler())

FLYTE_DATASET_TRANSFORMER.register_handler(pa.Table, DatasetFormat.PARQUET, ArrowToParquetPersistenceHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(pa.Table, DatasetFormat.BIGQUERY, ArrowToBQPersistenceHandlers())
