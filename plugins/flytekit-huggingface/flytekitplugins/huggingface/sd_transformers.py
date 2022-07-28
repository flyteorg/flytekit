import typing

from datasets.arrow_dataset import Dataset

from flytekit import FlyteContext
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured.structured_dataset import (
    GCS,
    LOCAL,
    PARQUET,
    S3,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)


class HuggingFaceDatasetToParquetEncodingHandler(StructuredDatasetEncoder):
    def __init__(self, protocol: str):
        super().__init__(Dataset, protocol, PARQUET)

    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        df = typing.cast(Dataset, structured_dataset.dataframe)

        local_dir = ctx.file_access.get_random_local_directory()
        local_path = f"{local_dir}/00000"

        df.to_parquet(local_path)

        remote_dir = typing.cast(str, structured_dataset.uri) or ctx.file_access.get_random_remote_directory()
        ctx.file_access.upload_directory(local_dir, remote_dir)
        return literals.StructuredDataset(uri=remote_dir, metadata=StructuredDatasetMetadata(structured_dataset_type))


class ParquetToHuggingFaceDatasetDecodingHandler(StructuredDatasetDecoder):
    def __init__(self, protocol: str):
        super().__init__(Dataset, protocol, PARQUET)

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> Dataset:
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.get_data(flyte_value.uri, local_dir, is_multipart=True)
        path = f"{local_dir}/00000"

        if current_task_metadata.structured_dataset_type and current_task_metadata.structured_dataset_type.columns:
            columns = [c.name for c in current_task_metadata.structured_dataset_type.columns]
            return Dataset.from_parquet(path, columns=columns)
        return Dataset.from_parquet(path)


for protocol in [LOCAL, S3]:
    StructuredDatasetTransformerEngine.register(
        HuggingFaceDatasetToParquetEncodingHandler(protocol), default_for_type=True
    )
    StructuredDatasetTransformerEngine.register(
        ParquetToHuggingFaceDatasetDecodingHandler(protocol), default_for_type=True
    )
StructuredDatasetTransformerEngine.register(HuggingFaceDatasetToParquetEncodingHandler(GCS), default_for_type=False)
StructuredDatasetTransformerEngine.register(ParquetToHuggingFaceDatasetDecodingHandler(GCS), default_for_type=False)
