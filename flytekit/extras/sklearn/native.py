import pathlib
from typing import Generic, Type, TypeVar

import joblib
import sklearn
from flyteidl.core import types_pb2
from flyteidl.core.literals_pb2 import Blob, BlobMetadata, Literal, Scalar
from flyteidl.core.types_pb2 import LiteralType

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError

T = TypeVar("T")


class SklearnTypeTransformer(TypeTransformer, Generic[T]):
    def get_literal_type(self, t: Type[T]) -> LiteralType:
        return LiteralType(
            blob=types_pb2.BlobType(
                format=self.SKLEARN_FORMAT,
                dimensionality=types_pb2.BlobType.SINGLE,
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
            type=types_pb2.BlobType(
                format=self.SKLEARN_FORMAT,
                dimensionality=types_pb2.BlobType.SINGLE,
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".joblib"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save sklearn estimator to a file
        joblib.dump(python_val, local_path)

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

        # load sklearn estimator from a file
        return joblib.load(local_path)

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == types_pb2.BlobType.SINGLE
            and literal_type.blob.format == self.SKLEARN_FORMAT
        ):
            return T

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class SklearnEstimatorTransformer(SklearnTypeTransformer[sklearn.base.BaseEstimator]):
    SKLEARN_FORMAT = "SklearnEstimator"

    def __init__(self):
        super().__init__(name="Sklearn Estimator", t=sklearn.base.BaseEstimator)


TypeEngine.register(SklearnEstimatorTransformer())
