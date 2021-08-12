from typing import Optional

from joblib import Memory

# TODO: read from config
# Location in the file system where serialized objects will be stored
CACHE_LOCATION = "/tmp/cache-location"


class LocalCache(object):
    _memory: Optional[Memory] = None
    _initialized = False

    @staticmethod
    def initialize():
        LocalCache._memory = Memory(CACHE_LOCATION, verbose=5)
        LocalCache._initialized = True

    # TODO: type this properly
    @staticmethod
    def cache(func, ignore=None):
        if not LocalCache._initialized:
            LocalCache.initialize()
        return LocalCache._memory.cache(func, ignore=ignore)

    # TODO: type this properly
    @staticmethod
    def clear():
        LocalCache._memory.clear()
