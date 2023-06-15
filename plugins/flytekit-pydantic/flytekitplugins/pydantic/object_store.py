import builtins
import contextlib
import datetime
import typing
import uuid
from typing import Any, Dict, Generator, Optional, Type, TypeVar, cast

from typing_extensions import Annotated

import pydantic

from flytekit.core import context_manager, type_engine
from flytekit.models import literals

MODULES_TO_EXLCLUDE_FROM_FLYTE_TYPES = {m.__name__ for m in [builtins, typing, datetime]}


def include_in_flyte_types(t: type) -> bool:
    if t is None:
        return False
    if t.__module__ in MODULES_TO_EXLCLUDE_FROM_FLYTE_TYPES:
        return False
    return True


PYDANTIC_SUPPORTED_FLYTE_TYPES = tuple(
    filter(include_in_flyte_types, type_engine.TypeEngine.get_available_transformers())
)
ObjectStoreID = Annotated[str, "Key for unique literalmap of a serialized basemodel."]
LiteralObjID = Annotated[str, "Key for unique object in literal map."]
LiteralStore = Annotated[Dict[LiteralObjID, literals.Literal], "uid to literals for a serialized BaseModel"]
PythonType = TypeVar("PythonType")  # target type of the deserialization


class PydanticTransformerLiteralStore:
    """
    This class is an intermediate store for python objects that are being serialized/deserialized.

    On serialization of a basemodel, flyte objects are serialized and stored in this object store.
    On deserialization of a basemodel, flyte objects are deserialized from this object store.
    """

    _literal_stores: Dict[ObjectStoreID, LiteralStore] = {}  # for each basemodel, one literal store is kept
    _attached_literalstore: Optional[LiteralStore] = None  # when deserializing we attach to this with a context manager

    ### When serializing, we instantiate a new literal store for each basemodel
    def __init__(self, uid: ObjectStoreID) -> None:
        self._basemodel_uid = uid

    def as_literalmap(self) -> literals.LiteralMap:
        """
        Converts the object store to a literal map
        """
        return literals.LiteralMap(literals=self._get_literal_store())

    def register_python_object(self, python_object: object) -> LiteralObjID:
        """Serialize to literal and return a unique identifier."""
        serialized_item = serialize_to_flyte_literal(python_object)
        identifier = make_identifier_for_serializeable(python_object)
        self._get_literal_store()[identifier] = serialized_item
        return identifier

    @classmethod
    def from_basemodel(cls, basemodel: pydantic.BaseModel) -> "PydanticTransformerLiteralStore":
        """Attach to a BaseModel to write to the literal store"""
        internal_basemodel_uid = cls._basemodel_uid = make_identifier_for_basemodel(basemodel)
        assert internal_basemodel_uid not in cls._literal_stores, "Every serialization must have unique basemodel uid"
        cls._literal_stores[internal_basemodel_uid] = {}
        return cls(internal_basemodel_uid)

    ## When deserializing, we attach to the literal store

    @classmethod
    @contextlib.contextmanager
    def attach_to_literalmap(cls, literal_map: literals.LiteralMap) -> Generator[None, None, None]:
        """
        Read a literal map and populate the object store from it
        """
        # TODO make thread safe?
        try:
            cls._attached_literalstore = literal_map.literals
            yield
        finally:
            cls._attached_literalstore = None

    @classmethod
    def is_attached(cls) -> bool:
        return cls._attached_literalstore is not None

    @classmethod
    def get_python_object(cls, identifier: LiteralObjID, expected_type: Type[PythonType]) -> Optional[PythonType]:
        """Deserialize a literal and return the python object"""
        literal = cls._get_literal_store()[identifier]
        python_object = deserialize_flyte_literal(literal, expected_type)
        return python_object

    ## Private methods
    @classmethod
    def _get_literal_store(cls) -> LiteralStore:
        if cls.is_attached():
            return cls._attached_literalstore  # type: ignore there is always a literal store when attached
        else:
            return cls._literal_stores[cls._basemodel_uid]


def deserialize_flyte_literal(
    flyteobj_literal: literals.Literal, python_type: Type[PythonType]
) -> Optional[PythonType]:
    """Deserialize a Flyte Literal into the python object instance."""
    ctx = context_manager.FlyteContext.current_context()
    transformer = type_engine.TypeEngine.get_transformer(python_type)
    python_obj = transformer.to_python_value(ctx, flyteobj_literal, python_type)
    return python_obj


def serialize_to_flyte_literal(python_obj) -> literals.Literal:
    """
    Use the Flyte TypeEngine to serialize a python object to a Flyte Literal.
    """
    python_type = type(python_obj)
    ctx = context_manager.FlyteContextManager().current_context()
    literal_type = type_engine.TypeEngine.to_literal_type(python_type)
    literal_obj = type_engine.TypeEngine.to_literal(ctx, python_obj, python_type, literal_type)
    return literal_obj


def make_identifier_for_basemodel(basemodel: pydantic.BaseModel) -> ObjectStoreID:
    """
    Create a unique identifier for a basemodel.
    """
    unique_id = f"{basemodel.__class__.__name__}_{uuid.uuid4().hex}"
    return cast(ObjectStoreID, unique_id)


def make_identifier_for_serializeable(python_type: object) -> LiteralObjID:
    """
    Create a unique identifier for a python object.
    """
    unique_id = f"{type(python_type).__name__}_{uuid.uuid4().hex}"
    return cast(LiteralObjID, unique_id)
