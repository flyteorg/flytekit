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
    """
    When constructing a Pydantic BaseModel with a FlyteDirectory, the 'deserialize_flyte_dir' method
    is called because it is registered as a validator for this BaseModel.

    FlyteDirToMultipartBlobTransformer().to_python_value will set the '_remote_directory' attribute to `False`
    if the FlyteDirectory represents a local path (i.e., the directory is local).

    Later, when we need to upload this directory to remote storage during serialization using the
    'serialize_flyte_dir' method, which is our BaseModel serializer, the upload process will be skipped
    if '_remote_directory' is set to `False`.

    This is why we need to customize the deserialization logic here, to ensure that the proper behavior
    for remote directories is maintained.

    Related Code:
    - https://github.com/flyteorg/flytekit/blob/6944406e1e3f09aedafb2270c67c532c9ddb98f3/flytekit/types/directory/types.py#L554-L555
    """
    pv._remote_directory = None
    return pv


@model_serializer
def serialize_flyte_schema(self) -> Dict[str, str]:
    FlyteSchemaTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    return {"remote_path": self.remote_path}


@model_validator(mode="after")
def deserialize_flyte_schema(self, info) -> FlyteSchema:
    # If we call the method to_python_value, FlyteSchemaTransformer will overwrite the argument _local_path,
    # which will lose our data.
    # If this data is from an existed FlyteSchema, _local_path will be None.

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
    # If we call the method to_python_value, StructuredDatasetTransformerEngine will overwrite the argument '_dataframe',
    # which will lose our data.
    # If this data is from an existed StructuredDataset, '_dataframe' will be None.

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
