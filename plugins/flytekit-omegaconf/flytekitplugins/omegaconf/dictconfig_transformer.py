import importlib
import re
from typing import Type, TypeVar

import flatten_dict
import flytekitplugins.omegaconf
from flyteidl.core.literals_pb2 import Literal as PB_Literal
from flytekitplugins.omegaconf.config import OmegaConfTransformerMode
from flytekitplugins.omegaconf.type_information import extract_node_type
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct

import omegaconf
from flytekit import FlyteContext
from flytekit.core.type_engine import TypeTransformerFailedError
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.loggers import logger
from flytekit.models.literals import Literal, Scalar
from flytekit.models.types import LiteralType, SimpleType
from omegaconf import DictConfig, OmegaConf

T = TypeVar("T")
NoneType = type(None)


def is_flattenable(d: DictConfig) -> bool:
    """Check if a DictConfig can be properly flattened and unflattened, i.e. keys do not contain dots."""
    return all(
        isinstance(k, str)  # keys are strings ...
        and "." not in k  # ... and do not contain dots
        and (
            OmegaConf.is_missing(d, k)  # values are either MISSING ...
            or not isinstance(d[k], DictConfig)  # ... not nested Dictionaries ...
            or is_flattenable(d[k])
        )  # or flatable themselves
        for k in d.keys()
    )


class DictConfigTransformer(TypeTransformer[DictConfig]):
    def __init__(self):
        """Construct DictConfigTransformer."""
        super().__init__(name="OmegaConf DictConfig", t=DictConfig)

    def get_literal_type(self, t: Type[DictConfig]) -> LiteralType:
        """
        Provide type hint for Flytekit type system.

        To support the multivariate typing of nodes in a DictConfig, we encode them as binaries (no introspection)
        with multiple files.
        """
        return LiteralType(simple=SimpleType.STRUCT)

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        """
        Convert from given python type object ``DictConfig`` to the Literal representation.

        Since the DictConfig type does not offer additional type hints for its nodes, typing information is stored
        within the literal itself rather than the Flyte LiteralType.
        """
        # instead of raising TypeError, raising ValueError so that flytekit can catch it in
        # https://github.com/flyteorg/flytekit/blob/60c982e4b065fdb3aba0b957e506f652a2674c00/flytekit/core/
        # type_engine.py#L1222
        if not isinstance(python_val, DictConfig):
            raise ValueError(f"Invalid type {type(python_val)}, can only serialise DictConfigs")
        if not is_flattenable(python_val):
            raise ValueError(
                f"{python_val} cannot be flattened as it contains non-string keys or keys containing dots."
            )

        base_config = OmegaConf.get_type(python_val)
        type_map = {}
        value_map = {}
        for key in python_val.keys():
            if OmegaConf.is_missing(python_val, key):
                type_map[key] = "MISSING"
            else:
                node_type, type_name = extract_node_type(python_val, key)
                type_map[key] = type_name

                transformer = TypeEngine.get_transformer(node_type)
                literal_type = transformer.get_literal_type(node_type)

                value_map[key] = MessageToDict(
                    transformer.to_literal(ctx, python_val[key], node_type, literal_type).to_flyte_idl()
                )

        wrapper = Struct()
        wrapper.update(
            flatten_dict.flatten(
                {
                    "types": type_map,
                    "values": value_map,
                    "base_dataclass": f"{base_config.__module__}.{base_config.__name__}",
                },
                reducer="dot",
                keep_empty_types=(dict,),
            )
        )
        return Literal(scalar=Scalar(generic=wrapper))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[DictConfig]) -> DictConfig:
        """Re-hydrate the custom object from Flyte Literal value."""
        if lv and lv.scalar is not None:
            nested_dict = flatten_dict.unflatten(MessageToDict(lv.scalar.generic), splitter="dot")
            cfg_dict = {}
            for key, type_desc in nested_dict["types"].items():
                if type_desc == "MISSING":
                    cfg_dict[key] = omegaconf.MISSING
                else:
                    if re.match(r".+\[.*]", type_desc):
                        origin_module, origin_type = type_desc.split("[")[0].rsplit(".", 1)
                        origin = importlib.import_module(origin_module).__getattribute__(origin_type)
                        sub_types = type_desc.split("[")[1][:-1].split(", ")
                        for i, t in enumerate(sub_types):
                            if t != "NoneType":
                                module_name, class_name = t.rsplit(".", 1)
                                sub_types[i] = importlib.import_module(module_name).__getattribute__(class_name)
                            else:
                                sub_types[i] = type(None)
                        node_type = origin[tuple(sub_types)]
                    else:
                        module_name, class_name = type_desc.rsplit(".", 1)
                        node_type = importlib.import_module(module_name).__getattribute__(class_name)

                    transformer = TypeEngine.get_transformer(node_type)
                    value_literal = Literal.from_flyte_idl(ParseDict(nested_dict["values"][key], PB_Literal()))
                    cfg_dict[key] = transformer.to_python_value(ctx, value_literal, node_type)

            if (
                nested_dict["base_dataclass"] != "builtins.dict"
                and flytekitplugins.omegaconf.get_transformer_mode() != OmegaConfTransformerMode.DictConfig
            ):
                # Explicitly instantiate dataclass and create DictConfig from there in order to have typing information
                module_name, class_name = nested_dict["base_dataclass"].rsplit(".", 1)
                try:
                    return OmegaConf.structured(
                        importlib.import_module(module_name).__getattribute__(class_name)(**cfg_dict)
                    )
                except (ModuleNotFoundError, AttributeError) as e:
                    logger.error(
                        f"Could not import module {module_name}. If you want to deserialise to DictConfig, "
                        f"set the mode to DictConfigTransformerMode.DictConfig."
                    )
                    if flytekitplugins.omegaconf.get_transformer_mode() == OmegaConfTransformerMode.DataClass:
                        raise e
            return OmegaConf.create(cfg_dict)
        raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")


TypeEngine.register(DictConfigTransformer())
