import os
import typing

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud.bigquery import LoadJob, LoadJobConfig

from flytekit.types.structured.structured_dataset import (
    FLYTE_DATASET_TRANSFORMER,
    DatasetFormat,
    DatasetPersistenceHandler,
    DatasetRetrievalHandler,
)


class ArrowToParquetPersistenceHandlers(DatasetPersistenceHandler):
    def persist(
        self,
        table: pa.Table,
        to_file: os.PathLike,
    ):
        pq.write_table(table, to_file)


class ArrowToBQPersistenceHandlers(DatasetPersistenceHandler):
    def persist(self, table: pa.Table, path: str, job_config: typing.Optional[LoadJobConfig] = None) -> LoadJob:
        from flytekit.types.structured import PandasToBQPersistenceHandlers

        return PandasToBQPersistenceHandlers().persist(table.to_pandas(), path, job_config)


class ParquetToArrowRetrievalHandler(DatasetRetrievalHandler):
    def retrieve(self, path: str) -> pa.Table:
        return pq.read_table(path)


class BQToArrowRetrievalHandler(DatasetRetrievalHandler):
    def retrieve(self, path: str, **kwargs) -> pa.Table:
        from flytekit.types.structured import BQToPandasRetrievalHandler

        pd_dataframe = BQToPandasRetrievalHandler().retrieve(path)
        return pa.Table.from_pandas(pd.concat(pd_dataframe))


FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.PARQUET, pa.Table, ParquetToArrowRetrievalHandler())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.BIGQUERY, pa.Table, BQToArrowRetrievalHandler())

FLYTE_DATASET_TRANSFORMER.register_handler(pa.Table, DatasetFormat.PARQUET, ArrowToParquetPersistenceHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(pa.Table, DatasetFormat.BIGQUERY, ArrowToBQPersistenceHandlers())
