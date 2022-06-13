from __future__ import annotations

import functools
import pathlib
import types
import typing
from typing import Type

import torch

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType

for item in [
    (torch.Tensor, "PyTorchTensorTransformer", "TENSOR"),
    (torch.nn.Module, "PyTorchModuleTransformer", "MODULE"),
]:
    """
    TypeTransformers that support torch.Tensor & torch.nn.Module as native types.
    """
    entity = None

    def clsexec(ns):
        # define class variables
        ns[f"PYTORCH_{item[2]}_FORMAT"] = f"PyTorch{item[2].title()}"
        ns["entity"] = item
        return ns

    # define class
    new_class = types.new_class(item[1], bases=(TypeTransformer[item[0]],), exec_body=clsexec)

    def __init__(self):
        super(type(self), self).__init__(name=f"PyTorch {self.entity[2].title()}", t=self.entity[0])

    def get_literal_type(self, t: Type[entity[0]]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=getattr(self, f"PYTORCH_{self.entity[2]}_FORMAT"),
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: entity[0],
        python_type: Type[entity[0]],
        expected: LiteralType,
    ) -> Literal:
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=getattr(self, f"PYTORCH_{self.entity[2]}_FORMAT"),
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".pt"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save pytorch tensor to a file
        torch.save(python_val, local_path)

        remote_path = ctx.file_access.get_random_remote_path(local_path)
        ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[entity[0]]) -> entity[0]:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=False)

        # load pytorch tensor from a file
        return torch.load(local_path)

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[entity[0]]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == getattr(self, f"PYTORCH_{self.entity[2]}_FORMAT")
        ):
            return self.entity[0]

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")

    def copy_func(f):
        """Create real copy of the function."""
        g = types.FunctionType(
            f.__code__, f.__globals__, name=f.__name__, argdefs=f.__defaults__, closure=f.__closure__
        )
        g = functools.update_wrapper(g, f)
        g.__kwdefaults__ = f.__kwdefaults__
        return g

    # assign methods to new class
    new_class.__init__ = copy_func(__init__)
    new_class.get_literal_type = copy_func(get_literal_type)
    new_class.to_literal = copy_func(to_literal)
    new_class.to_python_value = copy_func(to_python_value)

    TypeEngine.register(new_class())

    # initialize class to import in __init__.py
    globals().update({item[1]: new_class})
