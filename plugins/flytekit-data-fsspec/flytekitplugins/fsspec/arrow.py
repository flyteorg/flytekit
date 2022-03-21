import os
import typing
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import NoCredentialsError
from flytekitplugins.fsspec.persist import FSSpecPersistence
from fsspec.core import split_protocol, strip_protocol

from flytekit import FlyteContext, logger
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.structured_dataset import (
    PARQUET,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
)


class ArrowToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, protocol: str):
        super().__init__(pa.Table, protocol, PARQUET)

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
        fp = FSSpecPersistence(data_config=ctx.file_access.data_config)
        filesystem = fp.get_filesystem(path)
        pq.write_table(structured_dataset.dataframe, strip_protocol(path), filesystem=filesystem)
        return literals.StructuredDataset(uri=uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToArrowDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(pa.Table, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pa.Table:
        uri = flyte_value.uri
        if not ctx.file_access.is_remote(uri):
            Path(uri).parent.mkdir(parents=True, exist_ok=True)
        _, path = split_protocol(uri)

        columns = None
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
        try:
            fp = FSSpecPersistence(data_config=ctx.file_access.data_config)
            fs = fp.get_filesystem(uri)
            return pq.read_table(path, filesystem=fs, columns=columns)
        except NoCredentialsError as e:
            logger.debug("S3 source detected, attempting anonymous S3 access")
            fs = FSSpecPersistence.get_anonymous_filesystem(uri)
            if fs is not None:
                return pq.read_table(path, filesystem=fs, columns=columns)
            raise e
