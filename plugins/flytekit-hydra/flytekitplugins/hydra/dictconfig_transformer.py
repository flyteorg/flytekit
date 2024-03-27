import importlib
import logging
import re
import traceback
import typing
from typing import Type, TypeVar

import flatten_dict
import omegaconf
from flyteidl.core.literals_pb2 import Literal as PB_Literal
from flytekit import FlyteContext
from flytekit.core.type_engine import TypeTransformerFailedError
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models.literals import Literal, Scalar
from flytekit.models.types import LiteralType, SimpleType
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.struct_pb2 import Struct
from omegaconf import DictConfig, OmegaConf

from flytekitplugins.hydra.config import OmegaConfTransformerMode, SharedConfig
from flytekitplugins.hydra.flytekit_patch import iterate_get_transformers
from flytekitplugins.hydra.type_information import extract_node_type

logger = logging.getLogger(__name__)

T = TypeVar("T")
NoneType = type(None)


def is_flatable(d: typing.Union[DictConfig]) -> bool:
    """Check if a DictConfig can be properly flattened and unflattened, i.e. keys do not contain dots."""
    return all(
        isinstance(k, str)  # keys are strings ...
        and "." not in k  # ... and do not contain dots
        and (
            OmegaConf.is_missing(d, k)  # values are either MISSING ...
            or not isinstance(d[k], DictConfig)  # ... not nested Dictionaries ...
            or is_flatable(d[k])
        )  # or flatable themselves
        for k in d.keys()
    )


class DictConfigTransformer(TypeTransformer[DictConfig]):
    def __init__(self, mode: typing.Optional[OmegaConfTransformerMode] = None):
        """Construct DictConfigTransformer."""
        super().__init__(name="OmegaConf DictConfig", t=DictConfig)
        self.mode = mode

    @property
    def mode(self) -> OmegaConfTransformerMode:
        """Serialisation mode for omegaconf objects defined in shared config."""
        return SharedConfig.get_mode()

    @mode.setter
    def mode(self, new_mode: OmegaConfTransformerMode) -> None:
        """Updates the central shared config with a new serialisation mode."""
        SharedConfig.set_mode(new_mode)

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
        # instead of raising TypeError, raising AssertError so that flytekit can catch it in
        # https://github.com/flyteorg/flytekit/blob/60c982e4b065fdb3aba0b957e506f652a2674c00/flytekit/core/
        # type_engine.py#L1222
        assert isinstance(python_val, DictConfig), f"Invalid type {type(python_val)}, can only serialise DictConfigs"
        assert is_flatable(
            python_val
        ), f"{python_val} cannot be flattened as it contains non-string keys or keys containing dots."

        base_config = OmegaConf.get_type(python_val)
        type_map = {}
        value_map = {}
        for key in python_val.keys():
            if OmegaConf.is_missing(python_val, key):
                type_map[key] = "MISSING"
            else:
                node_type, type_name = extract_node_type(python_val, key)
                type_map[key] = type_name

                transformation_logs = ""
                for transformer in iterate_get_transformers(node_type):
                    try:
                        literal_type = transformer.get_literal_type(node_type)

                        value_map[key] = MessageToDict(
                            transformer.to_literal(ctx, python_val[key], node_type, literal_type).to_flyte_idl()
                        )
                        break
                    except Exception:
                        transformation_logs += (
                            f"Serialisation with transformer {type(transformer)} failed:\n"
                            f"{traceback.format_exc()}\n\n"
                        )

                if key not in value_map.keys():
                    raise ValueError(
                        f"Could not identify matching transformer for object {python_val[key]} of type "
                        f"{type(python_val[key])}. This may either indicate that no such transformer "
                        "exists or the appropriate transformer cannot serialise this object. Attempted the following "
                        f"transformers:\n{transformation_logs}"
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

                    transformation_logs = ""
                    for transformer in iterate_get_transformers(node_type):
                        try:
                            value_literal = Literal.from_flyte_idl(ParseDict(nested_dict["values"][key], PB_Literal()))
                            cfg_dict[key] = transformer.to_python_value(ctx, value_literal, node_type)
                            break
                        except Exception:
                            logger.debug(
                                f"Serialisation with transformer {type(transformer)} failed:\n"
                                f"{traceback.format_exc()}\n\n"
                            )
                            transformation_logs += (
                                f"Deserialisation with transformer {type(transformer)} failed:\n"
                                f"{traceback.format_exc()}\n\n"
                            )

                    if key not in cfg_dict.keys():
                        raise ValueError(
                            f"Could not identify matching transformer for object {nested_dict['values'][key]} of "
                            f"proposed type {node_type}. This may either indicate that no such transformer "
                            "exists or the appropriate transformer cannot deserialise this object. Attempted the "
                            f"following transformers:\n{transformation_logs}"
                        )
            if nested_dict["base_dataclass"] != "builtins.dict" and self.mode != OmegaConfTransformerMode.DictConfig:
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
                    if self.mode == OmegaConfTransformerMode.DataClass:
                        raise e
            return OmegaConf.create(cfg_dict)
        raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")


TypeEngine.register(DictConfigTransformer())
