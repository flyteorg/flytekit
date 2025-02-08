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
