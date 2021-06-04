import re

from flytekit.configuration import common as _common_config

IMAGE = _common_config.FlyteStringConfigurationEntry("internal", "image")
# This configuration option specifies the path to the file that holds the configuration options.  Don't worry,
# there will not be cycles because the parsing of the configuration file intentionally will not read and settings
# in the [internal] section.
# The default, if you want to use it, should be a file called flytekit.config, located in wherever your python
# interpreter originates.
CONFIGURATION_PATH = _common_config.FlyteStringConfigurationEntry(
    "internal", "configuration_path", default="flytekit.config"
)

# Project, Domain and Version represent the values at registration time.
PROJECT = _common_config.FlyteStringConfigurationEntry("internal", "project", default="")
DOMAIN = _common_config.FlyteStringConfigurationEntry("internal", "domain", default="")
NAME = _common_config.FlyteStringConfigurationEntry("internal", "name", default="")
VERSION = _common_config.FlyteStringConfigurationEntry("internal", "version", default="")

# Project, Domain and Version represent the values at registration time.
TASK_PROJECT = _common_config.FlyteStringConfigurationEntry("internal", "task_project", default="")
TASK_DOMAIN = _common_config.FlyteStringConfigurationEntry("internal", "task_domain", default="")
TASK_NAME = _common_config.FlyteStringConfigurationEntry("internal", "task_name", default="")
TASK_VERSION = _common_config.FlyteStringConfigurationEntry("internal", "task_version", default="")

# Execution project and domain represent the values passed by execution engine at runtime.
EXECUTION_PROJECT = _common_config.FlyteStringConfigurationEntry("internal", "execution_project", default="")
EXECUTION_DOMAIN = _common_config.FlyteStringConfigurationEntry("internal", "execution_domain", default="")
EXECUTION_WORKFLOW = _common_config.FlyteStringConfigurationEntry("internal", "execution_workflow", default="")
EXECUTION_LAUNCHPLAN = _common_config.FlyteStringConfigurationEntry("internal", "execution_launchplan", default="")
EXECUTION_NAME = _common_config.FlyteStringConfigurationEntry("internal", "execution_id", default="")

# This is another layer of logging level, which can be set by propeller, and can override the SDK configuration if
# necessary.  (See the sdk.py version of this as well.)
LOGGING_LEVEL = _common_config.FlyteIntegerConfigurationEntry("internal", "logging_level")

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
