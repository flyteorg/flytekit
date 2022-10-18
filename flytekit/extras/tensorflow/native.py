import pathlib
from typing import Generic, Type, TypeVar

import tensorflow as tf

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType

T = TypeVar("T")


class TensorflowRecordTransformer(TypeTransformer, Generic[T]):
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

        local_path = ctx.file_access.get_random_local_path() + ".tfrecord"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # Write the `tf.train.Example` observations to the file.
        filename = "test.tfrecord"
        with tf.io.TFRecordWriter(filename) as writer:
            writer.write(python_val.SerializeToString())
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

        # Read TFRecord file and return Example format
        raw_dataset = tf.data.TFRecordDataset(local_path)
        example = tf.train.Example()
        for raw_record in raw_dataset:
            example.ParseFromString(raw_record.numpy())
        return example

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.TENSORFLOW_FORMAT
        ):
            return T

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class TensorflowRecordTransformer(TensorflowRecordTransformer[tf.train.Example]):
    TENSORFLOW_FORMAT = "TensorflowTensorRecord"

    def __init__(self):
        super().__init__(name="Tensorflow TensorRecord", t=tf.train.Example)


TypeEngine.register(TensorflowRecordTransformer())
