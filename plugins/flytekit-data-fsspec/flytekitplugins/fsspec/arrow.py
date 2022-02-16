import os
import typing
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from flytekitplugins.fsspec.persist import FSSpecPersistence
from fsspec.core import strip_protocol

from flytekit import FlyteContext
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
        filesystem = FSSpecPersistence._get_filesystem(path)
        pq.write_table(structured_dataset.dataframe, strip_protocol(path), filesystem=filesystem)
        return literals.StructuredDataset(uri=uri, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToArrowDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(pa.Table, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> pa.Table:
        uri = flyte_value.uri
        if not ctx.file_access.is_remote(uri):
            Path(uri).parent.mkdir(parents=True, exist_ok=True)
        filesystem = FSSpecPersistence._get_filesystem(uri)
        if flyte_value.metadata.structured_dataset_type.columns:
            columns = []
            for c in flyte_value.metadata.structured_dataset_type.columns:
                columns.append(c.name)
            return pq.read_table(uri, filesystem=filesystem, columns=columns)
        return pq.read_table(uri, filesystem=filesystem)
