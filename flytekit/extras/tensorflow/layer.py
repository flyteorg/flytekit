import json
from typing import Type

import tensorflow as tf

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.literals import Literal, Primitive, Scalar
from flytekit.models.types import LiteralType, SimpleType


class TensorFlowLayerTransformer(TypeTransformer[tf.keras.layers.Layer]):
    def __init__(self):
        super().__init__(name="TensorFlow Layer", t=tf.keras.layers.Layer)

    def get_literal_type(self, t: Type[tf.keras.layers.Layer]) -> LiteralType:
        return LiteralType(simple=SimpleType.STRING)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: tf.keras.layers.Layer,
        python_type: Type[tf.keras.layers.Layer],
        expected: LiteralType,
    ) -> Literal:
        layer_config = tf.keras.layers.serialize(python_val)

        return Literal(Scalar(primitive=Primitive(string_value=json.dumps(layer_config))))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[tf.keras.layers.Layer]
    ) -> tf.keras.layers.Layer:
        if not (lv and lv.scalar and lv.scalar.primitive):
            raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        layer_config = json.loads(lv.scalar.primitive.string_value)
        return tf.keras.layers.deserialize(layer_config)

    def guess_python_type(self, literal_type: LiteralType) -> Type[tf.keras.layers.Layer]:
        if literal_type is not None and literal_type.simple == SimpleType.STRING:
            return tf.keras.layers.Layer

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(TensorFlowLayerTransformer())
