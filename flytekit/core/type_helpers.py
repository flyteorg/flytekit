import importlib
import typing

T = typing.TypeVar("T")


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


def is_namedtuple(t: typing.Type[T]) -> bool:
    # This is namedtuple
    if hasattr(t, "__bases__") and (
        isinstance(t, typing.Type) or isinstance(t, typing.TypeVar)  # type: ignore
    ):
        bases = t.__bases__
        if len(bases) == 1 and bases[0] == tuple and hasattr(t, "_fields"):
            return True

    # This is original tuple
    if getattr(t, "__origin__", None) is tuple:
        if is_univariate_tuple(t):
            raise ValueError("Univariate tuple types are not supported yet.")
        return False

    # Not a tuple, should never happen
    raise ValueError(f"Type {t} is not a tuple or NamedTuple")


def is_univariate_tuple(t: typing.Type) -> bool:
    if getattr(t, "__origin__", None) is tuple:
        args = getattr(t, "__args__", None)
        return args is not None and len(args) == 2 and args[1] is Ellipsis
    return False
