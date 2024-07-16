from pydantic import BaseModel, model_serializer, model_validator
from flytekit.types.file import FlyteFile, FlyteFilePathTransformer
from flytekit.types.directory import FlyteDirectory, FlyteDirToMultipartBlobTransformer
from flytekit.core.context_manager import FlyteContextManager
from typing import Dict, Type, Any
from flytekit.models.core import types as _core_types
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType

@model_serializer
def serialize_flyte_file(self) -> Dict[str, Any]:
    lv = FlyteFilePathTransformer().to_literal(FlyteContextManager.current_context(), self, type(self), None)
    return {"path": lv.scalar.blob.uri}


@model_validator(mode='after')
def deserialize_flyte_file(self):
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
def serialize_flyte_dir(self) -> Dict[str, Any]:
    lv = FlyteDirToMultipartBlobTransformer().to_literal(
        FlyteContextManager.current_context(), self, type(self), None
    )
    return {"path": lv.scalar.blob.uri}


@model_validator(mode='after')
def deserialize_flyte_dir(self):
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

setattr(FlyteFile, "serialize_flyte_file", serialize_flyte_file)
setattr(FlyteFile, "deserialize_flyte_file", deserialize_flyte_file)
setattr(FlyteDirectory, "serialize_flyte_dir", serialize_flyte_dir)
setattr(FlyteDirectory, "deserialize_flyte_dir", deserialize_flyte_dir)
