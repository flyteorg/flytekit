import inspect
import pathlib
from typing import Type, TypeVar

import torch

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType

T = TypeVar("T")


class PyTorchTypeTransformer(TypeTransformer[T]):
    # Extra kwargs forwarded to torch.load. Tensors load fine with torch's safe default.
    TORCH_LOAD_KWARGS: dict = {}

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.PYTORCH_FORMAT,
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
        if not isinstance(python_val, torch.Tensor) and not isinstance(python_val, torch.nn.Module):
            raise TypeTransformerFailedError("Expected a torch.Tensor or nn.Module")

        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.PYTORCH_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".pt"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save pytorch tensor/module to a file
        torch.save(python_val, local_path)

        remote_path = ctx.file_access.put_raw_data(local_path)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=False)

        # cpu <-> gpu conversion
        if torch.cuda.is_available():
            map_location = "cuda:0"
        else:
            map_location = torch.device("cpu")

        # load pytorch tensor/module from a file
        return torch.load(local_path, map_location=map_location, **self.TORCH_LOAD_KWARGS)


class PyTorchTensorTransformer(PyTorchTypeTransformer[torch.Tensor]):
    PYTORCH_FORMAT = "PyTorchTensor"

    def __init__(self):
        super().__init__(name="PyTorch Tensor", t=torch.Tensor)

    def guess_python_type(self, literal_type: LiteralType) -> Type[torch.Tensor]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.PYTORCH_FORMAT
        ):
            return torch.Tensor

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class PyTorchModuleTransformer(PyTorchTypeTransformer[torch.nn.Module]):
    PYTORCH_FORMAT = "PyTorchModule"
    # A whole nn.Module is pickled, so it cannot be loaded with weights_only=True (the torch>=2.6 default).
    # torch<1.13 has no weights_only kwarg at all, so only pass it when it exists.
    TORCH_LOAD_KWARGS = {"weights_only": False} if "weights_only" in inspect.signature(torch.load).parameters else {}

    def __init__(self):
        super().__init__(name="PyTorch Module", t=torch.nn.Module)

    def guess_python_type(self, literal_type: LiteralType) -> Type[torch.nn.Module]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.PYTORCH_FORMAT
        ):
            return torch.nn.Module

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(PyTorchTensorTransformer())
TypeEngine.register(PyTorchModuleTransformer())
