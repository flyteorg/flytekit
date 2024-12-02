from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Protocol, Tuple, Union, runtime_checkable

from flytekit.image_spec.image_spec import ImageSpec


@dataclass
class VersionParameters:
    """
    Parameters used for version hash generation.

    Args:
        func (Optional[Callable]): The function to generate a version for
        container_image (Optional[Union[str, ImageSpec]]): The container image to generate a version for
    """

    func: Optional[Callable[..., Any]] = None
    container_image: Optional[Union[str, ImageSpec]] = None


@runtime_checkable
class AutoCache(Protocol):
    """
    A protocol that defines the interface for a caching mechanism
    that generates a version hash of a function based on its source code.
    """

    salt: str

    def get_version(self, params: VersionParameters) -> str:
        """
        Generate a version hash based on the provided parameters.

        Args:
            params (VersionParameters): Parameters to use for hash generation.

        Returns:
            str: The generated version hash.
        """
        ...


class CachePolicy:
    """
    A class that combines multiple caching mechanisms to generate a version hash.

    Args:
        auto_cache_policies: A list of AutoCache instances (optional).
        salt: Optional salt string to add uniqueness to the hash.
        cache_serialize: Boolean to indicate if serialization should be used.
        cache_version: A version string for the cache.
        cache_ignore_input_vars: Tuple of input variable names to ignore.
    """

    def __init__(
        self,
        auto_cache_policies: List["AutoCache"] = None,
        salt: str = "",
        cache_serialize: bool = False,
        cache_version: str = "",
        cache_ignore_input_vars: Tuple[str, ...] = (),
    ) -> None:
        self.auto_cache_policies = auto_cache_policies or []  # Use an empty list if None is provided
        self.salt = salt
        self.cache_serialize = cache_serialize
        self.cache_version = cache_version
        self.cache_ignore_input_vars = cache_ignore_input_vars

    def get_version(self, params: "VersionParameters") -> str:
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
