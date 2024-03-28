import importlib
import logging
import traceback
from typing import Optional, Type, TypeVar

import omegaconf
from flyteidl.core.literals_pb2 import Literal as PB_Literal
from flytekitplugins.hydra.config import OmegaConfTransformerMode, SharedConfig
from flytekitplugins.hydra.flytekit_patch import iterate_get_transformers
from flytekitplugins.hydra.type_information import extract_node_type
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct
from omegaconf import ListConfig, OmegaConf

from flytekit import FlyteContext
from flytekit.core.type_engine import TypeTransformerFailedError
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models.literals import Literal, Primitive, Scalar
from flytekit.models.types import LiteralType, SimpleType

T = TypeVar("T")
logger = logging.getLogger(__name__)


class ListConfigTransformer(TypeTransformer[ListConfig]):
    def __init__(self, mode: Optional[OmegaConfTransformerMode] = None):
        """Construct ListConfigTransformer."""
        super().__init__(name="OmegaConf ListConfig", t=ListConfig)
        self.mode = mode

    @property
    def mode(self) -> OmegaConfTransformerMode:
        """Serialisation mode for omegaconf objects defined in shared config."""
        return SharedConfig.get_mode()

    @mode.setter
    def mode(self, new_mode: OmegaConfTransformerMode) -> None:
        """Updates the central shared config with a new serialisation mode."""
        SharedConfig.set_mode(new_mode)

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

                transformation_logs = ""
                for transformer in iterate_get_transformers(node_type):
                    try:
                        literal_type = transformer.get_literal_type(node_type)
                        value_list.append(
                            MessageToDict(
                                transformer.to_literal(ctx, python_val[idx], node_type, literal_type).to_flyte_idl()
                            )
                        )
                        break
                    except Exception:
                        transformation_logs += (
                            f"Serialisation with transformer {type(transformer)} failed:\n"
                            f"{traceback.format_exc()}\n\n"
                        )

                if len(type_list) != len(value_list):
                    raise ValueError(
                        f"Could not identify matching transformer for object {python_val[idx]} of type "
                        f"{type(python_val[idx])}. This may either indicate that no such transformer "
                        "exists or the appropriate transformer cannot serialise this object. Attempted the following "
                        f"transformers:\n{transformation_logs}"
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

                    transformation_logs = ""
                    for transformer in iterate_get_transformers(node_type):
                        try:
                            cfg_literal.append(transformer.to_python_value(ctx, value_literal, node_type))
                            break
                        except Exception:
                            transformation_logs += (
                                f"Deserialisation with transformer {type(transformer)} failed:\n"
                                f"{traceback.format_exc()}\n\n"
                            )

                    if len(cfg_literal) != i + 1:
                        raise ValueError(
                            f"Could not identify matching transformer for object {value_literal[i]} of proposed type "
                            f"{node_type}. This may either indicate that no such transformer exists or the appropriate "
                            f"transformer cannot deserialise this object. Attempted the following "
                            f"transformers:\n{transformation_logs}"
                        )

            return OmegaConf.create(cfg_literal)
        raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")


TypeEngine.register(ListConfigTransformer())
