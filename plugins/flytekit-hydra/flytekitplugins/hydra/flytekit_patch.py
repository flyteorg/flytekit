import dataclasses
import inspect
import logging
from typing import get_args, Type

from flytekit.core.type_engine import is_annotated, TypeEngine, TypeTransformer

logger = logging.getLogger()


def iterate_get_transformers(python_type: Type) -> TypeTransformer:
    """
    This is a copy of flytekit.core.type_engine.TypeEngine.get_transformer. Instead of simply returning the first
    appropriate Transformer, it yields all admissible candidates and leaves identification of the correct one and error
    handling to the caller of the transformer itself.
    """
    TypeEngine.lazy_import_transformers()
    # Step 1
    if is_annotated(python_type):
        args = get_args(python_type)
        for annotation in args:
            if isinstance(annotation, TypeTransformer):
                yield annotation

        python_type = args[0]

    if python_type in TypeEngine._REGISTRY:
        yield TypeEngine._REGISTRY[python_type]

    # Step 2
    if hasattr(python_type, "__origin__"):
        # Handling of annotated generics, eg:
        # Annotated[typing.List[int], 'foo']
        if is_annotated(python_type):
            yield TypeEngine.get_transformer(get_args(python_type)[0])

        if python_type.__origin__ in TypeEngine._REGISTRY:
            yield TypeEngine._REGISTRY[python_type.__origin__]

        raise ValueError(
            f"Could not find suitable transformer for generic type {python_type.__origin__}."
            f"Please check the (info-)logs for caught exceptions in serialisation attempts using existing transformers."
            f"If no suitable transformer was chosen, this type may not currently be supported in Flytekit."
        )

    # Step 3
    # To facilitate cases where users may specify one transformer for multiple types that all inherit from one
    # parent.
    for base_type in TypeEngine._REGISTRY.keys():
        if base_type is None:
            continue  # None is actually one of the keys, but isinstance/issubclass doesn't work on it
        try:
            if isinstance(python_type, base_type) or (
                inspect.isclass(python_type) and issubclass(python_type, base_type)
            ):
                yield TypeEngine._REGISTRY[base_type]
        except TypeError:
            # As of python 3.9, calls to isinstance raise a TypeError if the base type is not a valid type, which
            # is the case for one of the restricted types, namely NamedTuple.
            logger.debug(f"Invalid base type {base_type} in call to isinstance", exc_info=True)

    # Step 4
    if dataclasses.is_dataclass(python_type):
        yield TypeEngine._DATACLASS_TRANSFORMER
