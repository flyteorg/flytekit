from typing import Optional, Tuple

from diskcache import Cache
from flyteidl.core.literals_pb2 import LiteralMap

from flytekit import lazy_module
from flytekit.models.literals import Literal, LiteralCollection
from flytekit.models.literals import LiteralMap as ModelLiteralMap

joblib = lazy_module("joblib")

# Location on the filesystem where serialized objects will be stored
# TODO: read from config
CACHE_LOCATION = "~/.flyte/local-cache"


def _recursive_hash_placement(literal: Literal) -> Literal:
    # Base case, hash gets passed through always if set
    if literal.hash is not None:
        return Literal(hash=literal.hash)
    elif literal.collection is not None:
        literals = [_recursive_hash_placement(lit) for lit in literal.collection.literals]
        return Literal(collection=LiteralCollection(literals=literals))
    elif literal.map is not None:
        literal_map = {}
        for key, literal_value in literal.map.literals.items():
            literal_map[key] = _recursive_hash_placement(literal_value)
        return Literal(map=ModelLiteralMap(literal_map))
    else:
        return literal


def _calculate_cache_key(
    task_name: str,
    cache_version: str,
    input_literal_map: ModelLiteralMap,
    cache_ignore_input_vars: Tuple[str, ...] = (),
) -> str:
    # Traverse the literals and replace the literal with a new literal that only contains the hash
    literal_map_overridden = {}
    for key, literal in input_literal_map.literals.items():
        if key in cache_ignore_input_vars:
            continue
        literal_map_overridden[key] = _recursive_hash_placement(literal)

    # Generate a stable representation of the underlying protobuf by passing `deterministic=True` to the
    # protobuf library.
    hashed_inputs = ModelLiteralMap(literal_map_overridden).to_flyte_idl().SerializeToString(deterministic=True)
    # Use joblib to hash the string representation of the literal into a fixed length string
    return f"{task_name}-{cache_version}-{joblib.hash(hashed_inputs)}"


class LocalTaskCache(object):
    """
    This class implements a persistent store able to cache the result of local task executions.
    """

    _cache: Cache
    _initialized: bool = False

    @staticmethod
    def initialize():
        LocalTaskCache._cache = Cache(CACHE_LOCATION)
        LocalTaskCache._initialized = True

    @staticmethod
    def clear():
        if not LocalTaskCache._initialized:
            LocalTaskCache.initialize()
        LocalTaskCache._cache.clear()

    @staticmethod
    def get(
        task_name: str, cache_version: str, input_literal_map: ModelLiteralMap, cache_ignore_input_vars: Tuple[str, ...]
    ) -> Optional[ModelLiteralMap]:
        if not LocalTaskCache._initialized:
            LocalTaskCache.initialize()
        serialized_obj = LocalTaskCache._cache.get(
            _calculate_cache_key(task_name, cache_version, input_literal_map, cache_ignore_input_vars)
        )

        if serialized_obj is None:
            return None

        # If the serialized object is a model file, first convert it back to a proto object (which will force it to
        # use the installed flyteidl proto messages) and then convert it to a model object. This will guarantee
        # that the object is in the correct format.
        if isinstance(serialized_obj, ModelLiteralMap):
            return ModelLiteralMap.from_flyte_idl(ModelLiteralMap.to_flyte_idl(serialized_obj))
        elif isinstance(serialized_obj, bytes):
            # If it is a bytes object, then it is a serialized proto object.
            # We need to convert it to a model object first.o
            pb_literal_map = LiteralMap()
            pb_literal_map.ParseFromString(serialized_obj)
            return ModelLiteralMap.from_flyte_idl(pb_literal_map)
        else:
            raise ValueError(f"Unexpected object type {type(serialized_obj)}")

    @staticmethod
    def set(
        task_name: str,
        cache_version: str,
        input_literal_map: ModelLiteralMap,
        cache_ignore_input_vars: Tuple[str, ...],
        value: ModelLiteralMap,
    ) -> None:
        if not LocalTaskCache._initialized:
            LocalTaskCache.initialize()
        LocalTaskCache._cache.set(
            _calculate_cache_key(
                task_name,
                cache_version,
                input_literal_map,
                cache_ignore_input_vars,
            ),
            value.to_flyte_idl().SerializeToString(),
        )
