import sys
import typing

class ImageNotFoundError(ValueError):
    pass


class DefaultImages(object):
    """
    We may want to load the default images from remote - maybe s3 location etc?
    """

    PYTHON_3_7 = (3, 7)
    PYTHON_3_8 = (3, 8)
    PYTHON_3_9 = (3, 9)
    PYTHON_3_10 = (3, 10)
    _DEFAULT_IMAGE_PREFIXES = {
        PYTHON_3_7: "ghcr.io/flyteorg/flytekit:py3.7-",
        PYTHON_3_8: "ghcr.io/flyteorg/flytekit:py3.8-",
        PYTHON_3_9: "ghcr.io/flyteorg/flytekit:py3.9-",
        PYTHON_3_10: "ghcr.io/flyteorg/flytekit:py3.10-",
    }

    @property
    def default_image(self) -> str:
        return DefaultImages.find_image_for()

    @staticmethod
    def find_image_for(python_version: typing.Optional[typing.Tuple[int, int]] = None) -> str:
        from flytekit import __version__
        if not __version__ or __version__ == "0.0.0+develop":
            version_suffix = "latest"
        else:
            version_suffix = __version__
        if python_version is None:
            python_version = (sys.version_info.major, sys.version_info.minor)
        if python_version not in DefaultImages._DEFAULT_IMAGE_PREFIXES:
            raise ImageNotFoundError(f"Default image not found for Python version: {python_version}")
        return DefaultImages._DEFAULT_IMAGE_PREFIXES[python_version] + version_suffix
