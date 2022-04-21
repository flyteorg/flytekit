import enum
import sys
import typing


class PythonVersion(enum.Enum):
    PYTHON_3_7 = (3, 7)
    PYTHON_3_8 = (3, 8)
    PYTHON_3_9 = (3, 9)
    PYTHON_3_10 = (3, 10)


class DefaultImages(object):
    """
    We may want to load the default images from remote - maybe s3 location etc?
    """

    _DEFAULT_IMAGE_PREFIXES = {
        PythonVersion.PYTHON_3_7: "ghcr.io/flyteorg/flytekit:py3.7-",
        PythonVersion.PYTHON_3_8: "ghcr.io/flyteorg/flytekit:py3.8-",
        PythonVersion.PYTHON_3_9: "ghcr.io/flyteorg/flytekit:py3.9-",
        PythonVersion.PYTHON_3_10: "ghcr.io/flyteorg/flytekit:py3.10-",
    }

    @classmethod
    def default_image(cls) -> str:
        return cls.find_image_for()

    @classmethod
    def find_image_for(
        cls, python_version: typing.Optional[PythonVersion] = None, flytekit_version: typing.Optional[str] = None
    ) -> str:
        from flytekit import __version__

        if not __version__ or __version__ == "0.0.0+develop":
            version_suffix = "latest"
        else:
            version_suffix = __version__
        if python_version is None:
            python_version = PythonVersion((sys.version_info.major, sys.version_info.minor))
        return cls._DEFAULT_IMAGE_PREFIXES[python_version] + (
            flytekit_version.replace("v", "") if flytekit_version else version_suffix
        )
