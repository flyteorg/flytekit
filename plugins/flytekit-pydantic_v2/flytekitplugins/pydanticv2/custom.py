from typing import Dict

from pydantic import model_serializer, model_validator

from flytekit.core.context_manager import FlyteContextManager
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar, Schema
from flytekit.types.directory import FlyteDirectory, FlyteDirToMultipartBlobTransformer
from flytekit.types.file import FlyteFile, FlyteFilePathTransformer
from flytekit.types.schema import FlyteSchema, FlyteSchemaTransformer, SchemaOpenMode


@model_serializer
def serialize_flyte_file(self) -> Dict[str, str]:
    lv = FlyteFilePathTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    return {"path": lv.scalar.blob.uri}


@model_validator(mode="after")
def deserialize_flyte_file(self) -> FlyteFile:
    return FlyteFilePathTransformer().to_python_value(
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


@model_serializer
def serialize_flyte_dir(self) -> Dict[str, str]:
    lv = FlyteDirToMultipartBlobTransformer().to_literal(FlyteContextManager.current_context(), self, self, None)
    return {"path": lv.scalar.blob.uri}


@model_validator(mode="after")
def deserialize_flyte_dir(self) -> FlyteDirectory:
    return FlyteDirToMultipartBlobTransformer().to_python_value(
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


@model_serializer
def serialize_flyte_schema(self) -> Dict[str, str]:
    self._supported_mode = SchemaOpenMode.WRITE
    lv = FlyteSchemaTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    assert lv.scalar.schema.uri == self.remote_path
    return {"remote_path": self.remote_path}

@model_validator(mode="after")
def deserialize_flyte_schema(self) -> FlyteSchema:
    if hasattr(self, "_local_path"):
        return self

    # if self.local_path is not None:
    #     return self
    # self._supported_mode = SchemaOpenMode.WRITE
    t = FlyteSchemaTransformer()
    print("=========DESERIALIZING======")
    # print(self.local_path)
    # print(self.remote_path)
    pv = t.to_python_value(
        FlyteContextManager.current_context(),
        Literal(scalar=Scalar(schema=Schema(self.remote_path, t._get_schema_type(type(self))))),
        type(self),
    )
    # print(pv.local_path)
    # print(pv.remote_path)
    print("=========DESERIALIZING======")
    return pv
