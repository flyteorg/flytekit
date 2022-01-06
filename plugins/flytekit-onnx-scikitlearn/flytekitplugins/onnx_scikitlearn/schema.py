import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Tuple, Type

import joblib
import skl2onnx
import skl2onnx.common.data_types
from dataclasses_json import dataclass_json
from skl2onnx import convert_sklearn

import flytekit
from flytekit import FlyteContext
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models.literals import Literal
from flytekit.models.types import LiteralType
from flytekit.types.file import JoblibSerializedFile
from flytekit.types.file.file import FlyteFile, FlyteFilePathTransformer


@dataclass_json
@dataclass
class ScikitLearn2ONNXConfig:
    initial_types: List[Tuple[str, Type]]
    name: str = None
    doc_string: str = ""
    target_opset: int = None
    verbose: int = 0
    final_types: List[Tuple[str, Type]] = None

    def __post_init__(self):
        validate_initial_types = [
            True for item in self.initial_types if item in inspect.getmembers("skl2onnx.common.data_types")
        ]
        if not all(validate_initial_types):
            raise ValueError("All types in initial_types must be in skl2onnx.common.data_types")

        if self.final_types:
            validate_final_types = [
                True for item in self.final_types if item in inspect.getmembers("skl2onnx.common.data_types")
            ]
            if not all(validate_final_types):
                raise ValueError("All types in final_types must be in skl2onnx.common.data_types")


class ScikitLearn2ONNX:
    @classmethod
    def config(cls) -> ScikitLearn2ONNXConfig:
        return ScikitLearn2ONNXConfig(initial_types=[("float_input", skl2onnx.common.data_types.FloatTensorType)])

    def __class_getitem__(cls, config: ScikitLearn2ONNXConfig) -> Any:
        class _ScikitLearn2ONNXTypeClass(ScikitLearn2ONNX):
            __origin__ = ScikitLearn2ONNX

            @classmethod
            def config(cls) -> ScikitLearn2ONNXConfig:
                return config

        return _ScikitLearn2ONNXTypeClass


class ScikitLearn2ONNXTransformer(TypeTransformer[ScikitLearn2ONNX]):
    def __init__(self):
        super().__init__(name="ScikitLearn ONNX Transformer", t=ScikitLearn2ONNX)

    @staticmethod
    def get_config(t: Type[ScikitLearn2ONNX]) -> ScikitLearn2ONNXConfig:
        return t.config()

    def get_literal_type(self, t: Type[ScikitLearn2ONNX]) -> LiteralType:
        return FlyteFilePathTransformer().get_literal_type(JoblibSerializedFile)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: JoblibSerializedFile,
        python_type: Type[ScikitLearn2ONNX],
        expected: LiteralType,
    ) -> Literal:
        return FlyteFilePathTransformer().to_literal(ctx, python_val, JoblibSerializedFile, expected)

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Type[ScikitLearn2ONNX],
    ) -> ScikitLearn2ONNX:
        if not (lv.scalar.blob.uri and lv.scalar.blob.metadata.type.format == "joblib"):
            raise AssertionError("Can only validate a literal JoblibSerializedFile")

        config = ScikitLearn2ONNXTransformer.get_config(expected_python_type).to_dict()

        onx = convert_sklearn(
            joblib.load(lv.scalar.blob.uri),
            **config,
        )

        onnx_fname = Path(flytekit.current_context().working_directory) / "scikitlearn_model.onnx"

        with open(onnx_fname, "wb") as f:
            f.write(onx.SerializeToString())

        ScikitLearn2ONNX.onnx = FlyteFile(path=str(onnx_fname))
        ScikitLearn2ONNX.model = lv.scalar.blob.uri

        return ScikitLearn2ONNX


TypeEngine.register(ScikitLearn2ONNXTransformer())
