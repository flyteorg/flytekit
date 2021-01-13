import configparser
import typing

from flytekit.configuration import common as _config_common


def get_specified_images() -> typing.Dict[str, str]:
    """
    This section should contain options, where the option name is the friendly name of the image and the corresponding
    value is actual FQN of the image. Example of how the section is structured
    [images]
    my_image1=docker.io/flyte:tag
    # Note that the tag is optional. If not specified it will be the default version identifier specified
    my_image2=docker.io/flyte

    :returns a dictionary of name: image<fqn+version> Version is optional
    """
    try:
        image_names = _config_common.CONFIGURATION_SINGLETON.config.options("images")
    except configparser.NoSectionError:
        print("No images specified, will use the default image")
        image_names = None
    images: typing.Dict[str, str] = {}
    if image_names:
        for i in image_names:
            images[str(i)] = _config_common.FlyteStringConfigurationEntry("images", i).get()
    return images
