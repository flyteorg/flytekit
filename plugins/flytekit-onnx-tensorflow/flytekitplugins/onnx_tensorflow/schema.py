from dataclasses import dataclass, field
from typing import Any, Tuple, Type, Union

import numpy as np
import tensorflow as tf
import tf2onnx
from dataclasses_json import dataclass_json
from typing_extensions import Annotated, get_args, get_origin

from flytekit import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType
from flytekit.types.file import FlyteFile


@dataclass_json
@dataclass
class TensorFlow2ONNXConfig:
    input_signature: Union[tf.TensorSpec, np.ndarray]
    opset: int = 13


class TensorFlow2ONNX:
    model: tf.keras = field(default=None)

    def __init__(self, model: tf.keras):
        self._model = model

    @property
    def model(self) -> Type[Any]:
        return self._model


def extract_config(t: Type[TensorFlow2ONNX]) -> Tuple[Type[TensorFlow2ONNX], TensorFlow2ONNXConfig]:
    config = None
    if get_origin(t) is Annotated:
        base_type, config = get_args(t)
        if isinstance(config, TensorFlow2ONNXConfig):
            return base_type, config
        else:
            raise TypeTransformerFailedError(f"{t}'s config isn't of type TensorFlow2ONNX")
    return t, config


def to_onnx(ctx, model, config):
    local_path = ctx.file_access.get_random_local_path()

    tf2onnx.convert.from_keras(
        model, input_signature=config.input_signature, opset=config.opset, output_path=local_path
    )

    return local_path


class TensorFlow2ONNXTransformer(TypeTransformer[TensorFlow2ONNX]):
    ONNX_FORMAT = "onnx"

    def __init__(self):
        super().__init__(name="TensorFlow ONNX Transformer", t=TensorFlow2ONNX)

    def get_literal_type(self, t: Type[TensorFlow2ONNX]) -> LiteralType:
        return LiteralType(blob=BlobType(format=self.ONNX_FORMAT, dimensionality=BlobType.BlobDimensionality.SINGLE))

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: TensorFlow2ONNX,
        python_type: Type[TensorFlow2ONNX],
        expected: LiteralType,
    ) -> Literal:
        python_type, config = extract_config(python_type)
        remote_path = ctx.file_access.get_random_remote_path()

        if config:
            local_path = to_onnx(ctx, python_val.model, config)
            ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        else:
            raise TypeTransformerFailedError(f"{python_type}'s config is None")

        return Literal(
            scalar=Scalar(
                blob=Blob(
                    uri=remote_path,
                    metadata=BlobMetadata(
                        type=BlobType(format=self.ONNX_FORMAT, dimensionality=BlobType.BlobDimensionality.SINGLE)
                    ),
                )
            )
        )

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Type[FlyteFile],
    ) -> FlyteFile:
        if not lv.scalar.blob.uri:
            raise TypeTransformerFailedError(f"ONNX isn't of the expected type {expected_python_type}")

        return FlyteFile[self.ONNX_FORMAT](path=lv.scalar.blob.uri)

    def guess_python_type(self, literal_type: LiteralType) -> Type[TensorFlow2ONNX]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.ONNX_FORMAT
        ):
            return TensorFlow2ONNX

        raise TypeTransformerFailedError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(TensorFlow2ONNXTransformer())
