import os
from typing import Type

import tensorflow as tf

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType


class TensorFlowTensorTransformer(TypeTransformer[tf.Tensor]):
    """
    TypeTransformer that supports tf.tensor as a native type
    """

    TENSORFLOW_FORMAT = "TensorFlowTensor"

    def __init__(self):
        super().__init__(name="TensorFlow Tensor", t=tf.Tensor)

    def get_literal_type(self, t: Type[tf.Tensor]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.TENSORFLOW_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: tf.Tensor,
        python_type: Type[tf.Tensor],
        expected: LiteralType,
    ) -> Literal:
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=python_val.dtype.name,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

        local_path = ctx.file_access.get_random_local_path()

        # Save `tf.tensor` to a file
        local_path = os.path.join(local_path, "tensor_data")
        tf.io.write_file(local_path, tf.io.serialize_tensor(python_val))

        remote_path = ctx.file_access.get_random_remote_path(local_path)
        ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[tf.Tensor]
    ) -> tf.Tensor:  # Set 'expected_python_type' as an optional argument
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=False)
        tensor_dtype = tf.dtypes.as_dtype(lv.scalar.blob.metadata.type.format)

        serialized_tensor = tf.io.read_file(local_path)

        return tf.io.parse_tensor(serialized_tensor, out_type=tensor_dtype)

    def guess_python_type(self, literal_type: LiteralType) -> Type[tf.Tensor]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.TENSORFLOW_FORMAT
        ):
            return tf.Tensor

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(TensorFlowTensorTransformer())
