from typing import Callable, List, Optional

from joblib import Memory

# Location in the file system where serialized objects will be stored
# TODO: read from config
CACHE_LOCATION = "~/.flyte/local-cache"


class LocalCache(object):
    _memory: Memory
    _initialized: bool = False

    @staticmethod
    def initialize():
        LocalCache._memory = Memory(CACHE_LOCATION, verbose=5)
        LocalCache._initialized = True

    @staticmethod
    def cache(func: Callable, ignore: Optional[List[str]] = None):
        if not LocalCache._initialized:
            LocalCache.initialize()
        return LocalCache._memory.cache(func, ignore=ignore)

    @staticmethod
    def clear():
        if not LocalCache._initialized:
            LocalCache.initialize()
        LocalCache._memory.clear()
