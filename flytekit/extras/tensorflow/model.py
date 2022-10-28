import pathlib
from typing import Type

import tensorflow as tf

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType


class TensorflowModelTransformer(TypeTransformer[tf.keras.Model]):
    TENSORFLOW_FORMAT = "TensorflowModel"

    def __init__(
        self,
    ):
        super().__init__(name="Tensorflow Model", t=tf.keras.Model)

    def get_literal_type(self, t: Type[tf.keras.Model]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.TENSORFLOW_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: tf.keras.Model,
        python_type: Type[tf.keras.Model],
        expected: LiteralType,
    ) -> Literal:
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.TENSORFLOW_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".h5"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save tensorflow model to a folder in SavedModel format
        tf.keras.models.save_model(python_val, local_path)

        remote_path = ctx.file_access.get_random_remote_path(local_path)
        ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[tf.keras.Model]
    ) -> tf.keras.Model:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=False)

        # load tensorflow model from the SavedModel folder
        return tf.keras.models.load_model(local_path)

    def guess_python_type(self, literal_type: LiteralType) -> Type[tf.keras.Model]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.TENSORFLOW_FORMAT
        ):
            return tf.keras.Model

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(TensorflowModelTransformer())
