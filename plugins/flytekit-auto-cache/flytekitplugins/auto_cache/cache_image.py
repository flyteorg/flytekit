import hashlib

from flytekit.core.auto_cache import VersionParameters
from flytekit.image_spec.image_spec import ImageSpec


class CacheImage:
    """
    A class that generates a version hash given a container image.

    Attributes:
        salt (str): A string used to add uniqueness to the generated hash. Defaults to an empty string.

    Methods:
        get_version(params: VersionParameters) -> str:
            Given a VersionParameters object, generates a version hash based on the container_image and the salt.
    """

    def __init__(self, salt: str = ""):
        """
        Initialize the CacheImage instance with a salt value.

        Args:
            salt (str): A string to be used as the salt in the hashing process. Defaults to an empty string.
        """
        self.salt = salt

    def get_version(self, params: VersionParameters) -> str:
        """
        Generates a version hash for the container image specified in the VersionParameters object.

        Args:
            params (VersionParameters): An object containing the container_image parameter.

        Returns:
            str: The SHA-256 hash of the container_image combined with the salt.

        Raises:
            ValueError: If the container_image parameter is None.
        """
        if params.container_image is None:
            raise ValueError("Image-based cache requires a container_image parameter")

        # If the image is an ImageSpec, combine tag with salt
        if isinstance(params.container_image, ImageSpec):
            combined = params.container_image.tag + self.salt
            return hashlib.sha256(combined.encode("utf-8")).hexdigest()

        # If the image is a string, combine with salt
        combined = params.container_image + self.salt
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()
