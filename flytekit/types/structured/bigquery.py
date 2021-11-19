import re
import typing

import pandas
import pandas as pd
import pyarrow as pa
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import LoadJob, LoadJobConfig
from google.cloud.bigquery_storage_v1 import types

from flytekit.types.structured.structured_dataset import (
    FLYTE_DATASET_TRANSFORMER,
    DatasetFormat,
    DatasetPersistenceHandler,
    DatasetRetrievalHandler,
)


class PandasToBQPersistenceHandlers(DatasetPersistenceHandler):
    def persist(self, df: pandas.DataFrame, path: str, job_config: typing.Optional[LoadJobConfig] = None) -> LoadJob:
        table_id = path.split("://", 1)[1].replace(":", ".")
        client = bigquery.Client()
        return client.load_table_from_dataframe(df, table_id, job_config=job_config)


class BQToPandasRetrievalHandler(DatasetRetrievalHandler):
    def retrieve(self, path: str, **kwargs) -> pd.DataFrame:
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


class ArrowToBQPersistenceHandlers(DatasetPersistenceHandler):
    def persist(self, table: pa.Table, path: str, job_config: typing.Optional[LoadJobConfig] = None) -> LoadJob:
        return PandasToBQPersistenceHandlers().persist(table.to_pandas(), path, job_config)


class BQToArrowRetrievalHandler(DatasetRetrievalHandler):
    def retrieve(self, path: str, **kwargs) -> pa.Table:
        pd_dataframe = BQToPandasRetrievalHandler().retrieve(path)
        return pa.Table.from_pandas(pd.concat(pd_dataframe))


FLYTE_DATASET_TRANSFORMER.register_handler(pd.DataFrame, DatasetFormat.BIGQUERY, PandasToBQPersistenceHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.BIGQUERY, pd.DataFrame, BQToPandasRetrievalHandler())
FLYTE_DATASET_TRANSFORMER.register_handler(pa.Table, DatasetFormat.BIGQUERY, ArrowToBQPersistenceHandlers())
FLYTE_DATASET_TRANSFORMER.register_handler(DatasetFormat.BIGQUERY, pa.Table, BQToArrowRetrievalHandler())
