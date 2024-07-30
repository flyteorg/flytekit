import importlib
from typing import Type, TypeVar

from flyteidl.core.literals_pb2 import Literal as PB_Literal
from flytekitplugins.omegaconf.type_information import extract_node_type
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct

import omegaconf
from flytekit import FlyteContext
from flytekit.core.type_engine import TypeTransformerFailedError
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models.literals import Literal, Primitive, Scalar
from flytekit.models.types import LiteralType, SimpleType
from omegaconf import ListConfig, OmegaConf

T = TypeVar("T")


class ListConfigTransformer(TypeTransformer[ListConfig]):
    def __init__(self):
        """Construct ListConfigTransformer."""
        super().__init__(name="OmegaConf ListConfig", t=ListConfig)

    def get_literal_type(self, t: Type[ListConfig]) -> LiteralType:
        """
        Provide type hint for Flytekit type system.

        To support the multivariate typing of nodes in a ListConfig, we encode them as binaries (no introspection)
        with multiple files.
        """
        return LiteralType(simple=SimpleType.STRUCT)

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        """
        Convert from given python type object ``ListConfig`` to the Literal representation.

        Since the ListConfig type does not offer additional type hints for its nodes, typing information is stored
        within the literal itself rather than the Flyte LiteralType.
        """
        # instead of raising TypeError, raising AssertError so that flytekit can catch it in
        # https://github.com/flyteorg/flytekit/blob/60c982e4b065fdb3aba0b957e506f652a2674c00/flytekit/core/
        # type_engine.py#L1222
        assert isinstance(python_val, ListConfig), f"Invalid type {type(python_val)}, can only serialise ListConfigs"

        type_list = []
        value_list = []
        for idx in range(len(python_val)):
            if OmegaConf.is_missing(python_val, idx):
                type_list.append("MISSING")
                value_list.append(
                    MessageToDict(Literal(scalar=Scalar(primitive=Primitive(string_value="MISSING"))).to_flyte_idl())
                )
            else:
                node_type, type_name = extract_node_type(python_val, idx)
                type_list.append(type_name)

                transformer = TypeEngine.get_transformer(node_type)
                literal_type = transformer.get_literal_type(node_type)
                value_list.append(
                    MessageToDict(transformer.to_literal(ctx, python_val[idx], node_type, literal_type).to_flyte_idl())
                )

        wrapper = Struct()
        wrapper.update({"types": type_list, "values": value_list})
        return Literal(scalar=Scalar(generic=wrapper))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[ListConfig]) -> ListConfig:
        """Re-hydrate the custom object from Flyte Literal value."""
        if lv and lv.scalar is not None:
            MessageToDict(lv.scalar.generic)

            type_list = MessageToDict(lv.scalar.generic)["types"]
            value_list = MessageToDict(lv.scalar.generic)["values"]
            cfg_literal = []
            for i, type_name in enumerate(type_list):
                if type_name == "MISSING":
                    cfg_literal.append(omegaconf.MISSING)
                else:
                    module_name, class_name = type_name.rsplit(".", 1)
                    node_type = importlib.import_module(module_name).__getattribute__(class_name)

                    value_literal = Literal.from_flyte_idl(ParseDict(value_list[i], PB_Literal()))

                    transformer = TypeEngine.get_transformer(node_type)
                    cfg_literal.append(transformer.to_python_value(ctx, value_literal, node_type))

            return OmegaConf.create(cfg_literal)
        raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")


TypeEngine.register(ListConfigTransformer())
