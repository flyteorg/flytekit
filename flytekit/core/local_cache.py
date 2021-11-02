from typing import Optional

from diskcache import Cache

from flytekit.models.literals import LiteralMap

# Location on the filesystem where serialized objects will be stored
# TODO: read from config
CACHE_LOCATION = "~/.flyte/local-cache"


def _calculate_cache_key(task_name: str, cache_version: str, input_literal_map: LiteralMap) -> str:
    return f"{task_name}-{cache_version}-{input_literal_map}"


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
    def get(task_name: str, cache_version: str, input_literal_map: LiteralMap) -> Optional[LiteralMap]:
        if not LocalTaskCache._initialized:
            LocalTaskCache.initialize()
        return LocalTaskCache._cache.get(_calculate_cache_key(task_name, cache_version, input_literal_map))

    @staticmethod
    def set(task_name: str, cache_version: str, input_literal_map: LiteralMap, value: LiteralMap) -> None:
        if not LocalTaskCache._initialized:
            LocalTaskCache.initialize()
        LocalTaskCache._cache.add(_calculate_cache_key(task_name, cache_version, input_literal_map), value)
