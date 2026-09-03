import inspect
import pathlib
from typing import Any, Optional, Type, TypeVar

import torch

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType

T = TypeVar("T")


def load_torch_object(path: str, map_location: Any, weights_only: Optional[bool] = None) -> Any:
    """
    Wrapper around ``torch.load``.

    ``weights_only=None`` keeps torch's own default, which is ``True`` since torch 2.6: only tensors, containers and
    primitives are unpickled. ``weights_only=False`` unpickles arbitrary objects and therefore executes pickle code
    from the blob. flytekit needs that for ``nn.Module`` and checkpoint objects; the trust model is the same as for
    ``FlytePickle``, the data was written by flytekit itself in an upstream task. The keyword does not exist before
    torch 1.13, in which case it is omitted.
    """
    if weights_only is not None and "weights_only" in inspect.signature(torch.load).parameters:
        return torch.load(path, map_location=map_location, weights_only=weights_only)
    return torch.load(path, map_location=map_location)


class PyTorchTypeTransformer(TypeTransformer[T]):
    # Passed to ``load_torch_object``; ``None`` keeps torch's default.
    WEIGHTS_ONLY: Optional[bool] = None

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
        return load_torch_object(local_path, map_location=map_location, weights_only=self.WEIGHTS_ONLY)


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
    # A whole ``nn.Module`` object is stored, which torch's weights-only loader refuses to unpickle.
    WEIGHTS_ONLY = False

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
