from typing import Dict
from pydantic import model_serializer, model_validator

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


# Conditional import for Pydantic model_serializer and model_validator
# Serialize and Deserialize functions
@model_serializer
def serialize_flyte_file(self) -> Dict[str, str]:
    lv = FlyteFilePathTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    return {"path": lv.scalar.blob.uri}


@model_validator(mode="after")
def deserialize_flyte_file(self, info) -> FlyteFile:
    if info.context is None or info.context.get("deserialize") is not True:
        return self

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
    return pv


@model_serializer
def serialize_flyte_dir(self) -> Dict[str, str]:
    lv = FlyteDirToMultipartBlobTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    return {"path": lv.scalar.blob.uri}


@model_validator(mode="after")
def deserialize_flyte_dir(self, info) -> FlyteDirectory:
    if info.context is None or info.context.get("deserialize") is not True:
        return self

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
    return pv


@model_serializer
def serialize_flyte_schema(self) -> Dict[str, str]:
    FlyteSchemaTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    return {"remote_path": self.remote_path}


@model_validator(mode="after")
def deserialize_flyte_schema(self, info) -> FlyteSchema:
    if info.context is None or info.context.get("deserialize") is not True:
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
def deserialize_structured_dataset(self, info) -> StructuredDataset:
    if info.context is None or info.context.get("deserialize") is not True:
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


setattr(FlyteFile, "serialize_flyte_file", serialize_flyte_file)
setattr(FlyteFile, "deserialize_flyte_file", deserialize_flyte_file)
setattr(FlyteDirectory, "serialize_flyte_dir", serialize_flyte_dir)
setattr(FlyteDirectory, "deserialize_flyte_dir", deserialize_flyte_dir)
setattr(FlyteSchema, "serialize_flyte_schema", serialize_flyte_schema)
setattr(FlyteSchema, "deserialize_flyte_schema", deserialize_flyte_schema)
setattr(StructuredDataset, "serialize_structured_dataset", serialize_structured_dataset)
setattr(StructuredDataset, "deserialize_structured_dataset", deserialize_structured_dataset)
