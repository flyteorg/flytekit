import pathlib
import typing
from typing import Type

import numpy as np

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType


class NumpyArrayTransformer(TypeTransformer[np.ndarray]):
    """
    TypeTransformer that supports np.ndarray as a native type.
    """

    NUMPY_ARRAY_FORMAT = "NumpyArray"

    def __init__(self):
        super().__init__(name="Numpy Array", t=np.ndarray)

    def get_literal_type(self, t: Type[np.ndarray]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.NUMPY_ARRAY_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )

    def to_literal(
        self, ctx: FlyteContext, python_val: np.ndarray, python_type: Type[np.ndarray], expected: LiteralType
    ) -> Literal:
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.NUMPY_ARRAY_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".npy"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save numpy array to a file
        # allow_pickle=False prevents numpy from trying to save object arrays (dtype=object) using pickle
        np.save(file=local_path, arr=python_val, allow_pickle=False)

        remote_path = ctx.file_access.get_random_remote_path(local_path)
        ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[np.ndarray]) -> np.ndarray:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=False)

        # load numpy array from a file
        return np.load(file=local_path)

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[np.ndarray]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.NUMPY_ARRAY_FORMAT
        ):
            return np.ndarray

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(NumpyArrayTransformer())
