from typing import Dict

from flytekit.core.context_manager import FlyteContextManager
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar, Schema
from flytekit.types.directory import FlyteDirectory, FlyteDirToMultipartBlobTransformer
from flytekit.types.file import FlyteFile, FlyteFilePathTransformer
from flytekit.types.schema import FlyteSchema, FlyteSchemaTransformer
from flytekit.types.structured import (
    StructuredDataset,
    StructuredDatasetMetadata,
    StructuredDatasetTransformerEngine,
    StructuredDatasetType,
)
from pydantic import model_serializer, model_validator


@model_serializer
def serialize_flyte_file(self) -> Dict[str, str]:
    lv = FlyteFilePathTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    return {"path": lv.scalar.blob.uri}


@model_validator(mode="after")
def deserialize_flyte_file(self) -> FlyteFile:
    pv = FlyteFilePathTransformer().to_python_value(
        FlyteContextManager.current_context(),
        Literal(
            scalar=Scalar(
                blob=Blob(
                    metadata=BlobMetadata(
                        type=_core_types.BlobType(
                            format="", dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
                        )
                    ),
                    uri=self.path,
                )
            )
        ),
        type(self),
    )
    pv._remote_path = None
    return pv


@model_serializer
def serialize_flyte_dir(self) -> Dict[str, str]:
    lv = FlyteDirToMultipartBlobTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    return {"path": lv.scalar.blob.uri}


@model_validator(mode="after")
def deserialize_flyte_dir(self) -> FlyteDirectory:
    pv = FlyteDirToMultipartBlobTransformer().to_python_value(
        FlyteContextManager.current_context(),
        Literal(
            scalar=Scalar(
                blob=Blob(
                    metadata=BlobMetadata(
                        type=_core_types.BlobType(
                            format="", dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART
                        )
                    ),
                    uri=self.path,
                )
            )
        ),
        type(self),
    )
    pv._remote_directory = None
    return pv


@model_serializer
def serialize_flyte_schema(self) -> Dict[str, str]:
    FlyteSchemaTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    return {"remote_path": self.remote_path}


@model_validator(mode="after")
def deserialize_flyte_schema(self) -> FlyteSchema:
    # If we call the method to_python_value, FlyteSchemaTransformer will overwrite the argument _local_path,
    # which will lose our data.
    # If this data is from an existed FlyteSchema, _local_path will be None.

    if hasattr(self, "_local_path"):
        return self

    t = FlyteSchemaTransformer()
    return t.to_python_value(
        FlyteContextManager.current_context(),
        Literal(scalar=Scalar(schema=Schema(self.remote_path, t._get_schema_type(type(self))))),
        type(self),
    )


@model_serializer
def serialize_structured_dataset(self) -> Dict[str, str]:
    lv = StructuredDatasetTransformerEngine().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    sd = StructuredDataset(uri=lv.scalar.structured_dataset.uri)
    sd.file_format = lv.scalar.structured_dataset.metadata.structured_dataset_type.format
    return {
        "uri": sd.uri,
        "file_format": sd.file_format,
    }


@model_validator(mode="after")
def deserialize_structured_dataset(self) -> StructuredDataset:
    # If we call the method to_python_value, StructuredDatasetTransformerEngine will overwrite the argument '_dataframe',
    # which will lose our data.
    # If this data is from an existed StructuredDataset, '_dataframe' will be None.

    if hasattr(self, "_dataframe"):
        return self

    return StructuredDatasetTransformerEngine().to_python_value(
        FlyteContextManager.current_context(),
        Literal(
            scalar=Scalar(
                structured_dataset=StructuredDataset(
                    metadata=StructuredDatasetMetadata(
                        structured_dataset_type=StructuredDatasetType(format=self.file_format)
                    ),
                    uri=self.uri,
                )
            )
        ),
        type(self),
    )
