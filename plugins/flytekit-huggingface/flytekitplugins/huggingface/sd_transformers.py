import os
import typing

import datasets

from flytekit import FlyteContext
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.structured_dataset import (
    PARQUET,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)


class HuggingFaceDatasetRenderer:
    """
    The datasets.Dataset printable representation is saved to HTML.
    """

    def to_html(self, df: datasets.Dataset) -> str:
        assert isinstance(df, datasets.Dataset)
        return str(df).replace("\n", "<br>")


class HuggingFaceDatasetToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self):
        super().__init__(datasets.Dataset, None, PARQUET)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        df = typing.cast(datasets.Dataset, structured_dataset.dataframe)

        local_dir = ctx.file_access.get_random_local_directory()
        local_path = f"{local_dir}/00000"

        df.to_parquet(local_path)

        remote_dir = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_directory()
        ctx.file_access.upload_directory(local_dir, remote_dir)
        return literals.StructuredDataset(uri=remote_dir, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToHuggingFaceDatasetDecodingHandler(StructuredDatasetDecoder):
    def __init__(self):
        super().__init__(datasets.Dataset, None, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> datasets.Dataset:
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.get_data(flyte_value.uri, local_dir, is_multipart=True)
        files = [item.path for item in os.scandir(local_dir)]
        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
            return datasets.Dataset.from_parquet(files, columns=columns)
        return datasets.Dataset.from_parquet(files)


StructuredDatasetTransformerEngine.register(HuggingFaceDatasetToParquetEncodingHandler())
StructuredDatasetTransformerEngine.register(ParquetToHuggingFaceDatasetDecodingHandler())
StructuredDatasetTransformerEngine.register_renderer(datasets.Dataset, HuggingFaceDatasetRenderer())
