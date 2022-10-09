import pathlib
from typing import Generic, Type, TypeVar

import tensorflow as tf

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType

T = TypeVar("T")


class TensorflowTypeTransformer(TypeTransformer, Generic[T]):
    def get_literal_type(self, t: Type[T]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.TENSORFLOW_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
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
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".tf"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        ds = tf.data.Dataset.from_tensor_slices(python_val)
        # Serialize the tensors
        ds_bytes = ds.map(tf.io.serialize_tensor)
        # save to TFRecord
        writer = tf.data.experimental.TFRecordWriter(local_path)
        writer.write(ds_bytes)

        remote_path = ctx.file_access.get_random_remote_path(local_path)
        ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=False)

        # Read TFRecord file
        ds_bytes = tf.data.TFRecordDataset(local_path)
        ds = ds_bytes.map(lambda x: tf.io.parse_tensor(x, out_type=tf.int32))
        return ds.get_single_element()

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.TENSORFLOW_FORMAT
        ):
            return T

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class TensorflowRecordTransformer(TensorflowTypeTransformer[tf.Tensor]):
    TENSORFLOW_FORMAT = "TensorflowTensor"

    def __init__(self):
        super().__init__(name="Tensorflow Tensor", t=tf.Tensor)


TypeEngine.register(TensorflowRecordTransformer())
