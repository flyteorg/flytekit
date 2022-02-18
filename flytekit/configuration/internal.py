import re

_IMAGE_VERSION_REGEX = ".*:(.+)"


def look_up_version_from_image_tag(tag):
    """
    Looks up the image tag from environment variable (should be set from the Dockerfile).
        FLYTE_INTERNAL_IMAGE should be the environment variable.

    This function is used when registering tasks/workflows with Admin.
    When using the canonical Python-based development cycle, the version that is used to register workflows
    and tasks with Admin should be the version of the image itself, which should ideally be something unique
    like the sha of the latest commit.

    :param Text tag: e.g. somedocker.com/myimage:someversion123
    :rtype: Text
    """
    if tag is None or tag == "":
        raise Exception("Bad input for image tag {}".format(tag))
    m = re.match(_IMAGE_VERSION_REGEX, tag)
    if m is not None:
        return m.group(1)

    raise Exception("Could not parse image version from configuration. Did you set it in the" "Dockerfile?")
