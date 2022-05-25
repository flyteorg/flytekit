from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, Type, Union

import torch
from dataclasses_json import dataclass_json
from typing_extensions import Annotated, get_args, get_origin

from flytekit import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType
from flytekit.types.file import FlyteFile


@dataclass_json
@dataclass
class PyTorch2ONNXConfig:
    args: Union[Tuple, torch.Tensor]
    export_params: bool = True
    verbose: bool = False
    opset_version: int = 9
    input_names: List[str] = field(default_factory=list)
    output_names: List[str] = field(default_factory=list)
    do_constant_folding: bool = False
    dynamic_axes: Union[Dict[str, Dict[int, str]], Dict[str, List[int]]] = field(default_factory=dict)
    keep_initializers_as_inputs: bool = None
    custom_opsets: Dict[str, int] = field(default_factory=dict)


@dataclass_json
@dataclass
class PyTorch2ONNX:
    model: Union[torch.nn.Module, torch.jit.ScriptModule, torch.jit.ScriptFunction] = field(default=None)

    def __init__(self, model: Union[torch.nn.Module, torch.jit.ScriptModule, torch.jit.ScriptFunction]):
        self._model = model

    @property
    def model(self) -> Type[Any]:
        return self._model


def extract_config(t: Type[PyTorch2ONNX]) -> Tuple[Type[PyTorch2ONNX], PyTorch2ONNXConfig]:
    config = None
    if get_origin(t) is Annotated:
        base_type, config = get_args(t)
        if isinstance(config, PyTorch2ONNXConfig):
            return base_type, config
        else:
            raise TypeTransformerFailedError(f"{t}'s config isn't of type PyTorch2ONNXConfig")
    return t, config


def to_onnx(ctx, model, config):
    local_path = ctx.file_access.get_random_local_path()

    torch.onnx.export(
        model,
        args=config.args,
        export_params=config.export_params,
        verbose=config.verbose,
        opset_version=config.opset_version,
        do_constant_folding=config.do_constant_folding,
        dynamic_axes=config.dynamic_axes,
        keep_initializers_as_inputs=config.keep_initializers_as_inputs,
        custom_opsets=config.custom_opsets,
        f=local_path,
    )

    return local_path


class PyTorch2ONNXTransformer(TypeTransformer[PyTorch2ONNX]):
    ONNX_FORMAT = "onnx"

    def __init__(self):
        super().__init__(name="PyTorch ONNX Transformer", t=PyTorch2ONNX)

    def get_literal_type(self, t: Type[PyTorch2ONNX]) -> LiteralType:
        return LiteralType(blob=BlobType(format=self.ONNX_FORMAT, dimensionality=BlobType.BlobDimensionality.SINGLE))

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: PyTorch2ONNX,
        python_type: Type[PyTorch2ONNX],
        expected: LiteralType,
    ) -> Literal:
        python_type, config = extract_config(python_type)
        remote_path = ctx.file_access.get_random_remote_path()

        if config:
            local_path = to_onnx(ctx, python_val.model, config)
            ctx.file_access.put_data(local_path, remote_path, is_multipart=False)
        else:
            raise TypeTransformerFailedError(f"{python_type}'s config is None")

        return Literal(
            scalar=Scalar(
                blob=Blob(
                    uri=remote_path,
                    metadata=BlobMetadata(
                        type=BlobType(format=self.ONNX_FORMAT, dimensionality=BlobType.BlobDimensionality.SINGLE)
                    ),
                )
            )
        )

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Type[FlyteFile],
    ) -> FlyteFile:
        if not lv.scalar.blob.uri:
            raise TypeTransformerFailedError(f"ONNX isn't of the expected type {expected_python_type}")

        return FlyteFile[self.ONNX_FORMAT](path=lv.scalar.blob.uri)

    def guess_python_type(self, literal_type: LiteralType) -> Type[PyTorch2ONNX]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.ONNX_FORMAT
        ):
            return PyTorch2ONNX

        raise TypeTransformerFailedError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(PyTorch2ONNXTransformer())
