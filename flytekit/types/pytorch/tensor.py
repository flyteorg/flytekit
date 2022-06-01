import pathlib
import typing
from typing import Type

import torch

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType


class PyTorchTensorTransformer(TypeTransformer[torch.Tensor]):
    """
    TypeTransformer that supports torch.Tensor as a native type.
    """

    PYTORCH_TENSOR_FORMAT = "PyTorchTensor"

    def __init__(self):
        super().__init__(name="PyTorch Tensor", t=torch.Tensor)

    def get_literal_type(self, t: Type[torch.Tensor]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.PYTORCH_TENSOR_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )

    def to_literal(
        self, ctx: FlyteContext, python_val: torch.Tensor, python_type: Type[torch.Tensor], expected: LiteralType
    ) -> Literal:
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.PYTORCH_TENSOR_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".pt"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save pytorch tensor to a file
        torch.save(python_val, local_path)

        remote_path = ctx.file_access.get_random_remote_path(local_path)
        ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[torch.Tensor]) -> torch.Tensor:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=False)

        # load pytorch tensor from a file
        return torch.load(local_path)

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[torch.Tensor]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.PYTORCH_TENSOR_FORMAT
        ):
            return torch.Tensor

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(PyTorchTensorTransformer())
