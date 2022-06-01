import pathlib
import typing
from dataclasses import dataclass
from typing import Type

import torch
from dataclasses_json import dataclass_json

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType


@dataclass_json
@dataclass
class PyTorchStateDict(object):
    """
    This class is helpful to store a PyTorch module's state_dict.
    This type pertains to saving and loading PyTorch modules/models.

    Why state_dict?
    When saving a model for inference, it is only necessary to save the trained model's learned parameters.
    Saving the model's state_dict will give the most flexibility for restoring the model later,
    which is why it is the recommended method for saving models.
    """

    module: typing.Optional[torch.nn.Module] = None


class PyTorchModuleTransformer(TypeTransformer[PyTorchStateDict]):
    """
    TypeTransformer that supports serializing and deserializing PyTorch modules' state_dicts.
    """

    PYTORCH_STATEDICT_FORMAT = "PyTorchStateDict"

    def __init__(self):
        super().__init__(name="PyTorch StateDict", t=PyTorchStateDict)

    def get_literal_type(self, t: Type[PyTorchStateDict]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.PYTORCH_STATEDICT_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: PyTorchStateDict,
        python_type: Type[PyTorchStateDict],
        expected: LiteralType,
    ) -> Literal:
        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.PYTORCH_STATEDICT_FORMAT, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".pt"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save state_dict to a file
        torch.save(python_val.module.state_dict(), local_path)

        remote_path = ctx.file_access.get_random_remote_path(local_path)
        ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[PyTorchStateDict]
    ) -> PyTorchStateDict:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(uri, local_path, is_multipart=False)

        # load state_dict from a file
        return typing.cast(PyTorchStateDict, torch.load(local_path))

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[PyTorchStateDict]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.PYTORCH_STATEDICT_FORMAT
        ):
            return PyTorchStateDict

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(PyTorchModuleTransformer())
