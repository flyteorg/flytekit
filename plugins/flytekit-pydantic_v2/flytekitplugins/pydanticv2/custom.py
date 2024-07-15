from pydantic import BaseModel, model_serializer, model_validator
from flytekit.types.file import FlyteFile, FlyteFilePathTransformer
from flytekit.core.context_manager import FlyteContextManager
from typing import Dict, Type, Any
from flytekit.models.core import types as _core_types
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType


@model_serializer
def ser_flyte_file(self) -> Dict[str, Any]:
    lv = FlyteFilePathTransformer().to_literal(FlyteContextManager.current_context(), self, FlyteFile, None)
    return {"path": lv.scalar.blob.uri, "does serialization works?": "Yes it is!"}


@model_validator(mode='after')
def deser_flyte_file(self):
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

setattr(FlyteFile, "ser_flyte_file", ser_flyte_file)
setattr(FlyteFile, "deser_flyte_file", deser_flyte_file)

