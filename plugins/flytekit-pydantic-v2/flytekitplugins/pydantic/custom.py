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
def deserialize_flyte_file(self, info) -> FlyteFile:
    """
    Pydantic calls validator in two cases:
    1. When using the constructor, e.g., BM(). In this case, we do not want to deserialize Flyte types.
    2. When calling the basemodel_type.model_validate_json() method. In this case, we do want to deserialize Flyte types.
    Therefore, Flyte type deserialization should only occur when model_validate_json() is called.
    """
    if info.context is None or info.context["deserialize"] is not True:
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
    """
    Pydantic calls validator in two cases:
    1. When using the constructor, e.g., BM(). In this case, we do not want to deserialize Flyte types.
    2. When calling the basemodel_type.model_validate_json() method. In this case, we do want to deserialize Flyte types.
    Therefore, Flyte type deserialization should only occur when model_validate_json() is called.
    """
    if info.context is None or info.context["deserialize"] is not True:
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
    """
    Pydantic calls validator in two cases:
    1. When using the constructor, e.g., BM(). In this case, we do not want to deserialize Flyte types.
    2. When calling the basemodel_type.model_validate_json() method. In this case, we do want to deserialize Flyte types.
    Therefore, Flyte type deserialization should only occur when model_validate_json() is called.
    """

    if info.context is None or info.context["deserialize"] is not True:
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
    """
    Pydantic calls validator in two cases:
    1. When using the constructor, e.g., BM(). In this case, we do not want to deserialize Flyte types.
    2. When calling the basemodel_type.model_validate_json() method. In this case, we do want to deserialize Flyte types.
    Therefore, Flyte type deserialization should only occur when model_validate_json() is called.
    """

    if info.context is None or info.context["deserialize"] is not True:
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
