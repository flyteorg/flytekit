import os
from dataclasses import dataclass
from typing import Generic, List, Optional, Tuple, Type, TypeVar, Union

import tensorflow as tf
from dataclasses_json import dataclass_json
from tensorflow.core.example import example_pb2
from tensorflow.python.data.ops.readers import TFRecordDatasetV2
from typing_extensions import Annotated, get_args, get_origin

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType
from flytekit.types.directory import TFRecordsDirectory
from flytekit.types.file import TFRecordFile

T = TypeVar("T")


@dataclass_json
@dataclass
class TFRecordDatasetConfig:
    """
    TFRecordDatasetConfig can be used while creating tf.data.TFRecordDataset comprising
    record of one or more TFRecord files.

    Args:
      compression_type: A scalar evaluating to one of "" (no compression), "ZLIB", or "GZIP".
      buffer_size: The number of bytes in the read buffer. If None, a sensible default for both local and remote file systems is used.
      num_parallel_reads: The number of files to read in parallel. If greater than one, the records of files read in parallel are outputted in an interleaved order.
      name: A name for the operation.
    """

    compression_type: Optional[str] = None
    buffer_size: Optional[int] = None
    num_parallel_reads: Optional[int] = None
    name: Optional[str] = None


def extract_metadata(t: Type[T]) -> Tuple[T, TFRecordDatasetConfig]:
    metadata = TFRecordDatasetConfig()
    if get_origin(t) is Annotated:
        base_type, metadata = get_args(t)
        if isinstance(metadata, TFRecordDatasetConfig):
            return base_type, metadata
        else:
            raise TypeTransformerFailedError(f"{t}'s metadata needs to be of type TFRecordDatasetConfig")
    return t, metadata


class TensorflowRecordTransformerBase(TypeTransformer, Generic[T]):
    """
    TypeTransformer that supports serialising and deserialising to and from TFRecord file.
    https://www.tensorflow.org/tutorials/load_data/tfrecord
    """

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.TENSORFLOW_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

    def to_literal(
        self, ctx: FlyteContext, python_val: Union[T, List[T]], python_type: Type[T], expected: LiteralType
    ) -> Literal:
        local_dir = ctx.file_access.get_random_local_directory()
        remote_path = ctx.file_access.get_random_remote_directory()
        if isinstance(python_val, List):
            dimensionality = _core_types.BlobType.BlobDimensionality.MULTIPART
            if all(isinstance(x, example_pb2.Example) for x in python_val):
                for i, val in enumerate(python_val):
                    local_path = f"{local_dir}/part_{i}"
                    with tf.io.TFRecordWriter(local_path) as writer:
                        writer.write(val.SerializeToString())
        elif isinstance(python_val, example_pb2.Example):
            dimensionality = _core_types.BlobType.BlobDimensionality.SINGLE
            local_path = os.path.join(local_dir, "0000.tfrecord")
            with tf.io.TFRecordWriter(local_path) as writer:
                writer.write(python_val.SerializeToString())
        else:
            raise AssertionError(
                f"TensorflowRecordsTransformer can only return TFRecordFile or TFRecordsDirectory types from a task, "
                f"returned object type provided is {type(python_val)}"
            )
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.TENSORFLOW_FORMAT,
                dimensionality=dimensionality,
            )
        )
        ctx.file_access.upload_directory(local_dir, remote_path)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[TFRecordDatasetV2]
    ) -> TFRecordDatasetV2:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        _, metadata = extract_metadata(expected_python_type)
        local_dir = ctx.file_access.get_random_local_directory()
        files = os.scandir(uri)
        filenames = [f.name for f in files if os.path.getsize(f) > 0]
        ctx.file_access.get_data(uri, local_dir, is_multipart=True if len(filenames) > 1 else False)
        # load .tfrecord into tf.data.TFRecordDataset
        return tf.data.TFRecordDataset(
            filenames=filenames,
            compression_type=metadata.compression_type,
            buffer_size=metadata.buffer_size,
            num_parallel_reads=metadata.num_parallel_read,
            name=metadata.name,
        )

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.TENSORFLOW_FORMAT
        ):
            return T

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class TensorflowRecordsDirTransformer(TensorflowRecordTransformerBase[TFRecordsDirectory]):
    TENSORFLOW_FORMAT = "TensorflowRecord"

    def __init__(self):
        super().__init__(name="Tensorflow Record", t=TFRecordsDirectory)


class TensorflowRecordFileTransformer(TensorflowRecordTransformerBase[TFRecordFile]):
    TENSORFLOW_FORMAT = "TensorflowRecord"

    def __init__(self):
        super().__init__(name="Tensorflow Record", t=TFRecordFile)


TypeEngine.register(TensorflowRecordsDirTransformer())
TypeEngine.register(TensorflowRecordFileTransformer())
