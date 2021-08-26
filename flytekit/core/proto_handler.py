import importlib
import typing

from flytekit.common.exceptions import user as user_exceptions

T = typing.TypeVar("T")


def load_type_from_tag(tag: str) -> typing.Type[T]:
    """
    Loads python type from tag
    """

    if "." not in tag:
        raise user_exceptions.FlyteValueException(
            tag,
            "Protobuf tag must include at least one '.' to delineate package and object name.",
        )

    module, name = tag.rsplit(".", 1)
    try:
        pb_module = importlib.import_module(module)
    except ImportError:
        raise user_exceptions.FlyteAssertion(
            "Could not resolve the protobuf definition @ {}.  Is the protobuf library installed?".format(module)
        )

    if not hasattr(pb_module, name):
        raise user_exceptions.FlyteAssertion("Could not find the protobuf named: {} @ {}.".format(name, module))

    return getattr(pb_module, name)
