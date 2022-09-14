import os
import typing
from pathlib import Path

import pandas as pd
from botocore.exceptions import NoCredentialsError
from flytekitplugins.fsspec.persist import FSSpecPersistence, s3_setup_args

from flytekit import FlyteContext, logger
from flytekit.configuration import DataConfig
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.structured_dataset import (
    PARQUET,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
)


def get_storage_options(cfg: DataConfig, uri: str) -> typing.Optional[typing.Dict]:
    protocol = FSSpecPersistence.get_protocol(uri)
    if protocol == "s3":
        kwargs = s3_setup_args(cfg.s3)
        if kwargs:
            return kwargs
    return None


class PandasToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, protocol: str):
        super().__init__(pd.DataFrame, protocol, PARQUET)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        uri = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_directory()
        if not ctx.file_access.is_remote(uri):
            Path(uri).mkdir(parents=True, exist_ok=True)
        path = os.path.join(uri, f"{0:05}")
        df = typing.cast(pd.DataFrame, structured_dataset.dataframe)
        df.to_parquet(
            path,
            coerce_timestamps="us",
            allow_truncated_timestamps=False,
            storage_options=get_storage_options(ctx.file_access.data_config, path),
        )
        structured_dataset_type.format = PARQUET
        return literals.StructuredDataset(uri=uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToPandasDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(pd.DataFrame, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pd.DataFrame:
        uri = flyte_value.uri
        columns = None
        kwargs = get_storage_options(ctx.file_access.data_config, uri)
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
        try:
            return pd.read_parquet(uri, columns=columns, storage_options=kwargs)
        except NoCredentialsError:
            logger.debug("S3 source detected, attempting anonymous S3 access")
            kwargs["anon"] = True
            return pd.read_parquet(uri, columns=columns, storage_options=kwargs)
