import uuid
from typing import Any, Dict, Type, TypeVar, cast

import pandas as pd
import torch.nn as nn
from typing_extensions import Annotated, NewType

from flytekit.core import context_manager, type_engine
from flytekit.models import literals
from flytekit.types import directory
from flytekit.types.file import file

PYDANTIC_SUPPORTED_FLYTE_TYPES = (
    nn.Module,
    pd.DataFrame,
    file.FlyteFile,
    directory.FlyteDirectory,
    # TODO - add all supported types
)

LiteralObjID = Annotated[str, "Key for unique object in literal map."]
PythonType = TypeVar("PythonType")  # target type of the deserialization


class FlyteObjectStore:
    """
    This class is an intermediate store for python objects that are being serialized/deserialized.

    On serialization of a basemodel, flyte objects are serialized and stored in this object store.
    On deserialization of a basemodel, flyte objects are deserialized from this object store.
    """

    _literal_store: Dict[LiteralObjID, literals.Literal] = {}

    def __setattr__(self, name: str, value: Any) -> None:
        raise Exception("Attributes should not be set on the FlyteObjectStore.")

    def __init__(self) -> None:
        raise Exception("This should not be instantiated, it is a singleton object store.")

    def __contains__(self, item: LiteralObjID) -> bool:
        return item in self.get_literal_store()

    @classmethod
    def get_literal_store(cls):
        """Accessor to the class variable"""
        return cls._literal_store

    @classmethod
    def register_python_object(cls, python_object: object) -> LiteralObjID:
        """serializes to literal and returns a unique identifier"""
        serialized_item = serialize_to_flyte_literal(python_object)
        identifier = make_identifier(python_object)
        cls.get_literal_store()[identifier] = serialized_item
        return identifier

    @classmethod
    def get_python_object(cls, identifier: LiteralObjID, expected_type: Type[PythonType]) -> PythonType:
        """deserializes a literal and returns the python object"""
        literal = cls.get_literal_store()[identifier]
        python_object = deserialize_flyte_literal(literal, expected_type)
        return python_object

    @classmethod
    def as_literalmap(cls) -> literals.LiteralMap:
        """
        Converts the object store to a literal map
        """
        return literals.LiteralMap(literals=cls.get_literal_store())

    @classmethod
    def read_literalmap(cls, literal_map: literals.LiteralMap) -> None:
        """
        Reads a literal map and populates the object store from it
        """
        literal_store = cls.get_literal_store()
        literal_store.update(literal_map.literals)


def deserialize_flyte_literal(flyteobj_literal: literals.Literal, python_type: Type[PythonType]) -> PythonType:
    """
    Deserializes a Flyte Literal into the python object instance.
    """
    ctx = context_manager.FlyteContext.current_context()
    transformer = type_engine.TypeEngine.get_transformer(python_type)
    python_obj = transformer.to_python_value(ctx, flyteobj_literal, python_type)
    return cast(PythonType, python_obj)


def serialize_to_flyte_literal(python_obj) -> literals.Literal:
    """
    Use the Flyte TypeEngine to serialize a python object to a Flyte Literal.
    """
    python_type = type(python_obj)
    ctx = context_manager.FlyteContextManager().current_context()
    literal_type = type_engine.TypeEngine.to_literal_type(python_type)
    literal_obj = type_engine.TypeEngine.to_literal(ctx, python_obj, python_type, literal_type)
    return literal_obj


def make_identifier(python_type: object) -> LiteralObjID:
    """
    Create a unique identifier for a python object.
    """
    # TODO - human readable way to identify the object
    unique_id = f"{type(python_type).__name__}_{uuid.uuid4().hex}"
    return cast(LiteralObjID, unique_id)
