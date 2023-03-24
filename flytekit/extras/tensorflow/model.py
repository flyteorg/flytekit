import pathlib
from typing import Generic, Type, TypeVar

import tensorflow as tf

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType

T = TypeVar("T")


class TensorFlowModelTransformerBase(TypeTransformer, Generic[T]):
    TENSORFLOW_FORMAT = "TensorFlowModel"

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.TENSORFLOW_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,
            )
        )

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: T,
        python_type: Type[T],
        expected: LiteralType,
    ) -> Literal:
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.TENSORFLOW_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,
            )
        )

        local_path = ctx.file_access.get_random_local_path()
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save model in SavedModel format
        tf.keras.models.save_model(python_val, local_path)

        remote_path = ctx.file_access.get_random_remote_path()
        ctx.file_access.put_data(local_path, remote_path, is_multipart=True)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=True)

        # load model
        return tf.keras.models.load_model(local_path)

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.MULTIPART
            and literal_type.blob.format == self.TENSORFLOW_FORMAT
        ):
            return T

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class TensorFlowModelTransformer(TensorFlowModelTransformerBase[tf.keras.Model]):
    def __init__(self):
        super().__init__(name="TensorFlow Model", t=tf.keras.Model)


# class TensorFlowSequentialTransformer(TensorFlowModelTransformerBase[tf.keras.Sequential]):
#     def __init__(self):
#         super().__init__(name="TensorFlow Sequential Model", t=tf.keras.Sequential)


TypeEngine.register(TensorFlowModelTransformer())
# TypeEngine.register(TensorFlowSequentialTransformer())
