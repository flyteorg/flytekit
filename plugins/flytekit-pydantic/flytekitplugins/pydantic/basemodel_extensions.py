import abc
from typing import Any, Dict, Generic, Optional, Type, TypeVar, Union, cast

from typing_extensions import TypedDict

from flytekit.types.directory import types as flyte_directory_types
from flytekit.types.file import file as flyte_file

from . import flytepath_creation

## ====================================================================================
# Base class
## ====================================================================================


Serializable = TypeVar("Serializable")
SerializedFlyteType = TypeVar("SerializedFlyteType")


def get_pydantic_flyteobject_config() -> Dict[str, Any]:
    """
    Returns the pydantic config for the serializers/deserializers
    """
    return {
        "json_encoders": {
            serializer_deserializer()._t: serializer_deserializer().serialize  # type: ignore
            for serializer_deserializer in PydanticSerializerDeserializerBase.__subclasses__()
        },
    }


class PydanticSerializerDeserializerBase(
    abc.ABC,
    Generic[Serializable, SerializedFlyteType],
):
    """Base class for object serializers/deserializers"""

    @abc.abstractmethod
    def serialize(self, obj: Serializable) -> SerializedFlyteType:
        pass

    @abc.abstractmethod
    def deserialize(self, obj: SerializedFlyteType) -> Serializable:
        pass

    def __init__(self, t: Type[Serializable]):
        self._t = t
        self._set_validator_on_serializeable(t)

    def _set_validator_on_serializeable(self, serializeable: Type[Serializable]) -> None:
        """
        Sets the validator on the pydantic model for the
        type that is being serialized/deserialized
        """
        setattr(serializeable, "__get_validators__", lambda *_: (self.deserialize,))


## ====================================================================================
#  FlyteDir
## ====================================================================================


class FlyteDirJsonEncoded(TypedDict):
    """JSON representation of a FlyteDirectory"""

    remote_source: str


FLYTEDIR_DESERIALIZABLE_TYPES = Union[flyte_directory_types.FlyteDirectory, str, FlyteDirJsonEncoded]


class FlyteDirSerializerDeserializer(
    PydanticSerializerDeserializerBase[flyte_directory_types.FlyteDirectory, FlyteDirJsonEncoded]
):
    def __init__(
        self,
        t: Type[flyte_directory_types.FlyteDirectory] = flyte_directory_types.FlyteDirectory,
    ):
        super().__init__(t)

    def serialize(self, obj: flyte_directory_types.FlyteDirectory) -> FlyteDirJsonEncoded:
        return {"remote_source": obj.remote_source}

    def deserialize(self, obj: FLYTEDIR_DESERIALIZABLE_TYPES) -> flyte_directory_types.FlyteDirectory:
        flytedir = validate_flytedir(obj)
        if flytedir is None:
            raise ValueError(f"Could not deserialize {obj} to FlyteDirectory")
        return flytedir


def validate_flytedir(
    flytedir: FLYTEDIR_DESERIALIZABLE_TYPES,
) -> Optional[flyte_directory_types.FlyteDirectory]:
    """validator for flytedir (i.e. deserializer)"""
    if isinstance(flytedir, dict):  # this is a json encoded flytedir
        flytedir = cast(FlyteDirJsonEncoded, flytedir)
        path = flytedir["remote_source"]
        return flytepath_creation.make_flytepath(path, flyte_directory_types.FlyteDirectory)
    elif isinstance(flytedir, str):  # when e.g. initializing from config
        return flytepath_creation.make_flytepath(flytedir, flyte_directory_types.FlyteDirectory)
    elif isinstance(flytedir, flyte_directory_types.FlyteDirectory):
        return flytedir
    else:
        raise ValueError(f"Invalid type for flytedir: {type(flytedir)}")


## ====================================================================================
#  FlyteFile
## ====================================================================================


class FlyteFileJsonEncoded(TypedDict):
    """JSON representation of a FlyteFile"""

    remote_source: str


FLYTEFILE_DESERIALIZABLE_TYPES = Union[flyte_directory_types.FlyteFile, str, FlyteFileJsonEncoded]


class FlyteFileSerializerDeserializer(PydanticSerializerDeserializerBase[flyte_file.FlyteFile, FlyteFileJsonEncoded]):
    def __init__(self, t: Type[flyte_file.FlyteFile] = flyte_file.FlyteFile):
        super().__init__(t)

    def serialize(self, obj: flyte_file.FlyteFile) -> FlyteFileJsonEncoded:
        return {"remote_source": obj.remote_source}

    def deserialize(self, obj: FLYTEFILE_DESERIALIZABLE_TYPES) -> flyte_file.FlyteFile:
        flyte_file = validate_flytefile(obj)
        if flyte_file is None:
            raise ValueError(f"Could not deserialize {obj} to FlyteFile")
        return flyte_file


def validate_flytefile(
    flytedir: FLYTEFILE_DESERIALIZABLE_TYPES,
) -> Optional[flyte_file.FlyteFile]:
    """validator for flytedir (i.e. deserializer)"""
    if isinstance(flytedir, dict):  # this is a json encoded flytedir
        flytedir = cast(FlyteFileJsonEncoded, flytedir)
        path = flytedir["remote_source"]
        return flytepath_creation.make_flytepath(path, flyte_file.FlyteFile)
    elif isinstance(flytedir, str):  # when e.g. initializing from config
        return flytepath_creation.make_flytepath(flytedir, flyte_file.FlyteFile)
    elif isinstance(flytedir, flyte_directory_types.FlyteFile):
        return flytedir
    else:
        raise ValueError(f"Invalid type for flytedir: {type(flytedir)}")


# add these to your basemodel config to enable serialization/deserialization of flyte objects.
pydantic_flyteobject_config = get_pydantic_flyteobject_config()
flyteobject_json_encoders = pydantic_flyteobject_config["json_encoders"]
