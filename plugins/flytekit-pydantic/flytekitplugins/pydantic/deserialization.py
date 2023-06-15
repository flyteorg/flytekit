from typing import Any, Callable, Dict, Iterator, List, Type, TypeVar, Union

import pydantic

from flytekit.types import directory, file

from . import object_store

# this field is used by pydantic to get the validator method
PYDANTIC_VALIDATOR_METHOD_NAME = pydantic.BaseModel.__get_validators__.__name__
PythonType = TypeVar("PythonType")  # target type of the deserialization
Serializable = TypeVar("Serializable")  # flyte object type


def set_validators_on_supported_flyte_types() -> None:
    """
    Set validator on the pydantic model for the type that is being (de-)serialized
    """
    for flyte_type in object_store.PYDANTIC_SUPPORTED_FLYTE_TYPES:
        setattr(flyte_type, PYDANTIC_VALIDATOR_METHOD_NAME, make_validators_for_type(flyte_type))


def make_validators_for_type(
    flyte_obj_type: Type[Serializable],
) -> Callable[[Any], Iterator[Callable[[Any], Serializable]]]:
    """
    Returns a validator that can be used by pydantic to deserialize the object
    """

    def validator(object_uid_maybe: Union[object_store.LiteralObjID, Any]) -> Union[Serializable, Any]:
        """partial of deserialize_flyte_literal with the object_type fixed"""
        if not isinstance(object_uid_maybe, str):
            return object_uid_maybe  # this validator should only trigger for the placholders
        if object_uid_maybe not in object_store.FlyteObjectStore.get_literal_store():
            return object_uid_maybe  # if not in the store pass to the next validator to resolve
        return object_store.FlyteObjectStore.get_python_object(object_uid_maybe, flyte_obj_type)

    def validator_generator(*args, **kwags) -> Iterator[Callable[[Any], Serializable]]:
        """Generator that returns the validator"""
        yield validator
        yield from additional_flytetype_validators.get(flyte_obj_type, [])

    return validator_generator


def validate_flytefile(flytefile: Union[str, file.FlyteFile]) -> file.FlyteFile:
    """validator for flytefile (i.e. deserializer)"""
    if isinstance(flytefile, file.FlyteFile):
        return flytefile
    if isinstance(flytefile, str):  # when e.g. initializing from config
        return file.FlyteFile(flytefile)
    else:
        raise ValueError(f"Invalid type for flytefile: {type(flytefile)}")


def validate_flytedir(flytedir: Union[str, directory.FlyteDirectory]) -> directory.FlyteDirectory:
    """validator for flytedir (i.e. deserializer)"""
    if isinstance(flytedir, directory.FlyteDirectory):
        return flytedir
    if isinstance(flytedir, str):  # when e.g. initializing from config
        return directory.FlyteDirectory(flytedir)
    else:
        raise ValueError(f"Invalid type for flytedir: {type(flytedir)}")


additional_flytetype_validators: Dict[Type, List[Callable[[Any], Any]]] = {
    file.FlyteFile: [validate_flytefile],
    directory.FlyteDirectory: [validate_flytedir],
}
