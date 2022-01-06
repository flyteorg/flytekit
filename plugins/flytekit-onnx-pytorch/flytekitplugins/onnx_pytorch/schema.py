from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple, Type, Union

import joblib
import torch
from dataclasses_json import dataclass_json

import flytekit
from flytekit import FlyteContext
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models.literals import Literal
from flytekit.models.types import LiteralType
from flytekit.types.file import JoblibSerializedFile
from flytekit.types.file.file import FlyteFile, FlyteFilePathTransformer


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


class PyTorch2ONNX:
    @classmethod
    def config(cls) -> PyTorch2ONNXConfig:
        return PyTorch2ONNXConfig(args=torch.Tensor([1]))

    def __class_getitem__(cls, config: PyTorch2ONNXConfig) -> Any:
        class _PyTorch2ONNXTypeClass(PyTorch2ONNX):
            __origin__ = PyTorch2ONNX

            @classmethod
            def config(cls) -> PyTorch2ONNXConfig:
                return config

        return _PyTorch2ONNXTypeClass


class PyTorch2ONNXTransformer(TypeTransformer[PyTorch2ONNX]):
    def __init__(self):
        super().__init__(name="PyTorch ONNX Transformer", t=PyTorch2ONNX)

    @staticmethod
    def get_config(t: Type[PyTorch2ONNX]) -> PyTorch2ONNXConfig:
        return t.config()

    def get_literal_type(self, t: Type[PyTorch2ONNX]) -> LiteralType:
        return FlyteFilePathTransformer().get_literal_type(JoblibSerializedFile)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: JoblibSerializedFile,
        python_type: Type[PyTorch2ONNX],
        expected: LiteralType,
    ) -> Literal:
        return FlyteFilePathTransformer().to_literal(ctx, python_val, JoblibSerializedFile, expected)

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Type[PyTorch2ONNX],
    ) -> PyTorch2ONNX:
        if not (lv.scalar.blob.uri and lv.scalar.blob.metadata.type.format == "joblib"):
            raise AssertionError("Can only validate a literal JoblibSerializedFile")

        config = PyTorch2ONNXTransformer.get_config(expected_python_type)

        onnx_fname = Path(flytekit.current_context().working_directory) / "pytorch_model.onnx"

        torch.onnx.export(
            joblib.load(lv.scalar.blob.uri),
            args=config.args,
            export_params=config.export_params,
            verbose=config.verbose,
            opset_version=config.opset_version,
            do_constant_folding=config.do_constant_folding,
            dynamic_axes=config.dynamic_axes,
            keep_initializers_as_inputs=config.keep_initializers_as_inputs,
            custom_opsets=config.custom_opsets,
            f=onnx_fname,
        )

        PyTorch2ONNX.onnx = FlyteFile(path=str(onnx_fname))
        PyTorch2ONNX.model = lv.scalar.blob.uri

        return PyTorch2ONNX


TypeEngine.register(PyTorch2ONNXTransformer())
