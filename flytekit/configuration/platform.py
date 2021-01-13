from flytekit.common import constants as _constants
from flytekit.configuration import common as _config_common

URL = _config_common.FlyteRequiredStringConfigurationEntry("platform", "url")

HTTP_URL = _config_common.FlyteStringConfigurationEntry("platform", "http_url", default=None)
"""
If not starting with either http or https, this setting should begin with // as per the urlparse library and
https://tools.ietf.org/html/rfc1808.html, otherwise the netloc will not be properly parsed.

Currently the only use-case for this configuration setting is for Auth discovery. This setting supports the case where
Flyte Admin's gRPC and HTTP points are deployed on different ports.
"""

INSECURE = _config_common.FlyteBoolConfigurationEntry("platform", "insecure", default=False)

CLOUD_PROVIDER = _config_common.FlyteStringConfigurationEntry(
    "platform", "cloud_provider", default=_constants.CloudProvider.AWS
)

AUTH = _config_common.FlyteBoolConfigurationEntry("platform", "auth", default=False)
"""
This config setting should not normally be filled in. Whether or not an admin server requires authentication should be
something published by the admin server itself (typically by returning a 401). However, to help with migration, this
config object is here to force the SDK to attempt the auth flow even without prompting by Admin.
"""
