import hashlib

from flytekit.core.auto_cache import VersionParameters
from flytekit.image_spec.image_spec import ImageSpec


class CacheImage:
    def __init__(self, salt: str):
        self.salt = salt

    def get_version(self, params: VersionParameters) -> str:
        if params.container_image is None:
            raise ValueError("Image-based cache requires a container_image parameter")

        # If the image is an ImageSpec, combine tag with salt
        if isinstance(params.container_image, ImageSpec):
            combined = params.container_image.tag + self.salt
            return hashlib.sha256(combined.encode("utf-8")).hexdigest()

        # If the image is a string, combine with salt
        combined = params.container_image + self.salt
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()
