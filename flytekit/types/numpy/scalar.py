
import pathlib
import typing
from typing import Type

import numpy as np
import joblib

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType


class NumpyScalarTransformer(TypeTransformer[np.generic]):
    """
    TypeTransformer that supports numpy scalar types as a native type.
    """

    NUMPY_FORMAT = "NumpyScalar"

    def __init__(self):
        super().__init__(name="Numpy Scalar", t=np.generic)

    def get_literal_type(self, t: Type[np.generic]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.NUMPY_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )

    def to_literal(
        self, ctx: FlyteContext, python_val: np.generic, python_type: Type[np.generic], expected: LiteralType
    ) -> Literal:
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.NUMPY_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".pkl"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save numpy scalar value to a pickle file
        joblib.dump(python_val, local_path)

        remote_path = ctx.file_access.get_random_remote_path(local_path)
        ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[np.generic]) -> np.generic:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=False)

        # load numpy scalar from a file
        return joblib.load(local_path)

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[np.generic]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.NUMPY_FORMAT
        ):
            return np.generic

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(NumpyScalarTransformer())
