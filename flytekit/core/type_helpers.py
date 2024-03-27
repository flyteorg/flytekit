import importlib
import sys
import typing

T = typing.TypeVar("T")

if sys.version_info >= (3, 10):
    from types import UnionType as UnionTypePep604
else:
    UnionTypePep604 = typing.Union


def load_type_from_tag(tag: str) -> typing.Type[T]:
    """
    Loads python type from tag
    """

    if "." not in tag:
        raise ValueError(
            f"Protobuf tag must include at least one '.' to delineate package and object name got {tag}",
        )

    module, name = tag.rsplit(".", 1)
    try:
        pb_module = importlib.import_module(module)
    except ImportError:
        raise ValueError(f"Could not resolve the protobuf definition @ {module}.  Is the protobuf library installed?")

    if not hasattr(pb_module, name):
        raise ValueError(f"Could not find the protobuf named: {name} @ {module}.")

    return getattr(pb_module, name)


def is_pep604_union_type(type_: typing.Any) -> bool:
    origin = typing.get_origin(type_)
    args = typing.get_args(type_)
    return origin is UnionTypePep604 and len(args) > 1


def convert_pep604_union_type(type_: typing.Any) -> typing.Any:
    """Convert PEP604 UnionType to a typing.Union type."""
    return typing.Union[type_.__args__]  # type: ignore
