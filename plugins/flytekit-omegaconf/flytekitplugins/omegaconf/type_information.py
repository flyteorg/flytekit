import dataclasses
import typing
from collections import ChainMap

from dataclasses_json import DataClassJsonMixin

from flytekit.loggers import logger
from omegaconf import DictConfig, ListConfig, OmegaConf

NoneType = type(None)


def substitute_types(t: typing.Type) -> typing.Type:
    """
    Provides a substitute type hint to use when selecting transformers for serialisation.

    :param t: Original type
    :return: A corrected typehint
    """
    if hasattr(t, "__origin__"):
        # Only encode generic type and let appropriate transformer handle the rest
        if t.__origin__ in [dict, typing.Dict]:
            t = DictConfig
        elif t.__origin__ in [list, typing.List]:
            t = ListConfig
        else:
            return t.__origin__
    return t


def all_annotations(cls: typing.Type) -> ChainMap:
    """
    Returns a dictionary-like ChainMap that includes annotations for all
    attributes defined in cls or inherited from superclasses.
    """
    return ChainMap(*(c.__annotations__ for c in cls.__mro__ if "__annotations__" in c.__dict__))


def extract_node_type(
    python_val: typing.Union[DictConfig, ListConfig], key: typing.Union[str, int]
) -> typing.Tuple[type, str]:
    """
    Provides typing information about DictConfig nodes

    :param python_val: A DictConfig
    :param key: Key of the node to analyze
    :return:
        - Type - The extracted type
        - str - String representation for (de-)serialisation
    """
    assert isinstance(python_val, DictConfig) or isinstance(
        python_val, ListConfig
    ), "Can only extract type information from omegaconf objects"

    python_val_node_type = OmegaConf.get_type(python_val)
    python_val_annotations = all_annotations(python_val_node_type)

    # Check if type annotations are available
    if hasattr(python_val_node_type, "__annotations__"):
        if key not in python_val_annotations:
            raise ValueError(
                f"Key '{key}' not found in type annotations {python_val_annotations}. "
                "Check your DictConfig object for invalid subtrees not covered by your structured config."
            )

        if typing.get_origin(python_val_annotations[key]) is not None:
            # Abstract types
            origin = typing.get_origin(python_val_annotations[key])
            if getattr(origin, "__name__", None) is not None:
                origin_name = f"{origin.__module__}.{origin.__name__}"
            elif getattr(origin, "_name", None) is not None:
                origin_name = f"{origin.__module__}.{origin._name}"
            else:
                raise ValueError(f"Could not extract name from origin type {origin}")

            # Replace list and dict with omegaconf types
            if origin_name in ["builtins.list", "typing.List"]:
                return ListConfig, "omegaconf.listconfig.ListConfig"
            elif origin_name in ["builtins.dict", "typing.Dict"]:
                return DictConfig, "omegaconf.dictconfig.DictConfig"

            sub_types = []
            sub_type_names = []
            for sub_type in typing.get_args(python_val_annotations[key]):
                if sub_type == NoneType:  # NoneType gets special treatment as no import exists
                    sub_types.append(NoneType)
                    sub_type_names.append("NoneType")
                elif dataclasses.is_dataclass(sub_type) and not issubclass(sub_type, DataClassJsonMixin):
                    # Dataclasses have no matching transformers and get replaced by DictConfig
                    # alternatively, dataclasses can use dataclass_json decorator
                    sub_types.append(DictConfig)
                    sub_type_names.append("omegaconf.dictconfig.DictConfig")
                else:
                    sub_type = substitute_types(sub_type)
                    sub_types.append(sub_type)
                    sub_type_names.append(f"{sub_type.__module__}.{sub_type.__name__}")
            return origin[tuple(sub_types)], f"{origin_name}[{', '.join(sub_type_names)}]"
        elif dataclasses.is_dataclass(python_val_annotations[key]):
            # Dataclasses have no matching transformers and get replaced by DictConfig
            # alternatively, dataclasses can use dataclass_json decorator
            return DictConfig, "omegaconf.dictconfig.DictConfig"
        elif python_val_annotations[key] != typing.Any:
            # Use (cleaned) annotation if it is meaningful
            node_type = substitute_types(python_val_annotations[key])
            type_name = f"{node_type.__module__}.{node_type.__name__}"
            return node_type, type_name

    logger.debug(
        f"Inferring type information directly from runtime object {python_val[key]} for serialisation purposes. "
        "For more stable type resolution and serialisation provide explicit type hints."
    )
    node_type = type(python_val[key])
    type_name = f"{node_type.__module__}.{node_type.__name__}"
    return node_type, type_name
