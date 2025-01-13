from typing import List

from flytekit.core.auto_cache import AutoCache, VersionParameters


class CachePolicy:
    """
    A class that combines multiple caching mechanisms to generate a version hash.

    Args:
        auto_cache_policies: A list of AutoCache instances (optional).
        salt: Optional salt string to add uniqueness to the hash.
    """

    def __init__(
        self,
        auto_cache_policies: List[AutoCache] = None,
        salt: str = "",
    ) -> None:
        self.auto_cache_policies = auto_cache_policies or []  # Use an empty list if None is provided
        self.salt = salt

    def get_version(self, params: VersionParameters) -> str:
        """
        Generate a version hash using all cache objects. If the user passes a version, it takes precedence over auto_cache_policies.

        Args:
            params (VersionParameters): Parameters to use for hash generation.

        Returns:
            str: The combined hash from all cache objects.
        """
        if self.cache_version:
            return self.cache_version

        if self.auto_cache_policies:
            task_hash = ""
            for cache_instance in self.auto_cache_policies:
                # Apply the policy's salt to each cache instance
                cache_instance.salt = self.salt
                task_hash += cache_instance.get_version(params)

            # Generate SHA-256 hash
            import hashlib

            hash_obj = hashlib.sha256(task_hash.encode())
            return hash_obj.hexdigest()

        return None
