class ImageNotFoundError(ValueError):
    pass


class DefaultImages(object):
    """
    We may want to load the default images from remote - maybe s3 location etc?
    """

    PYTHON_3_9 = "py3.9"
    _DEFAULT_IMAGES = {
        PYTHON_3_9: "ghcr.io/flyteorg/flytekit:py3.9-latest",
    }

    @staticmethod
    def find_image_for(flavor: str) -> str:
        if flavor not in DefaultImages._DEFAULT_IMAGES:
            raise ImageNotFoundError(f"Default image not found for flavor: {flavor}")
        return DefaultImages._DEFAULT_IMAGES[flavor]
