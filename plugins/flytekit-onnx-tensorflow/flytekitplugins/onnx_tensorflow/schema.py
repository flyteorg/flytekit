from dataclasses import dataclass
from pathlib import Path
from typing import Any, Type, Union

import joblib
import numpy as np
import tensorflow as tf
import tf2onnx
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
class TensorFlow2ONNXConfig:
    input_signature: Union[tf.TensorSpec, np.ndarray]
    opset: int = None


class TensorFlow2ONNX:
    @classmethod
    def config(cls) -> TensorFlow2ONNXConfig:
        return TensorFlow2ONNXConfig(input_signature=tf.TensorSpec(shape=(1,), dtype=tf.float32))

    def __class_getitem__(cls, config: TensorFlow2ONNXConfig) -> Any:
        class _TensorFlow2ONNXTypeClass(TensorFlow2ONNX):
            __origin__ = TensorFlow2ONNX

            @classmethod
            def config(cls) -> TensorFlow2ONNXConfig:
                return config

        return _TensorFlow2ONNXTypeClass


class TensorFlow2ONNXTransformer(TypeTransformer[TensorFlow2ONNX]):
    def __init__(self):
        super().__init__(name="TensorFlow ONNX Transformer", t=TensorFlow2ONNX)

    @staticmethod
    def get_config(t: Type[TensorFlow2ONNX]) -> TensorFlow2ONNXConfig:
        return t.config()

    def get_literal_type(self, t: Type[TensorFlow2ONNX]) -> LiteralType:
        return FlyteFilePathTransformer().get_literal_type(JoblibSerializedFile)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: JoblibSerializedFile,
        python_type: Type[TensorFlow2ONNX],
        expected: LiteralType,
    ) -> Literal:
        return FlyteFilePathTransformer().to_literal(ctx, python_val, JoblibSerializedFile, expected)

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Type[TensorFlow2ONNX],
    ) -> TensorFlow2ONNX:
        if not (lv.scalar.blob.uri and lv.scalar.blob.metadata.type.format == "joblib"):
            raise AssertionError("Can only validate a literal JoblibSerializedFile")

        config = TensorFlow2ONNXTransformer.get_config(expected_python_type).to_dict()

        onnx_fname = Path(flytekit.current_context().working_directory) / "tensorflow_model.onnx"

        tf2onnx.convert.from_keras(joblib.load(lv.scalar.blob.uri), **config, output_path=onnx_fname)

        TensorFlow2ONNX.onnx = FlyteFile(path=str(onnx_fname))
        TensorFlow2ONNX.model = lv.scalar.blob.uri

        return TensorFlow2ONNX


TypeEngine.register(TensorFlow2ONNXTransformer())
