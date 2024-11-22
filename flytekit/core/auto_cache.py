from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, Union, runtime_checkable

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
        *cache_objects: Variable number of AutoCache instances
        salt: Optional salt string to add uniqueness to the hash
    """

    def __init__(self, *cache_objects: AutoCache, salt: str = "") -> None:
        self.cache_objects = cache_objects
        self.salt = salt

    def get_version(self, params: VersionParameters) -> str:
        """
        Generate a version hash using all cache objects.

        Args:
            params (VersionParameters): Parameters to use for hash generation.

        Returns:
            str: The combined hash from all cache objects.
        """
        task_hash = ""
        for cache_instance in self.cache_objects:
            # Apply the policy's salt to each cache instance
            cache_instance.salt = self.salt
            task_hash += cache_instance.get_version(params)

        # Generate SHA-256 hash
        import hashlib

        hash_obj = hashlib.sha256(task_hash.encode())
        return hash_obj.hexdigest()
