from __future__ import absolute_import

from flytekit.configuration import common as _config_common
from flytekit.common import constants as _constants

URL = _config_common.FlyteRequiredStringConfigurationEntry('platform', 'url')
INSECURE = _config_common.FlyteBoolConfigurationEntry('platform', 'insecure', default=False)

CLOUD_PROVIDER = _config_common.FlyteStringConfigurationEntry(
    'platform', 'cloud_provider', default=_constants.CloudProvider.AWS
)

AUTH = _config_common.FlyteBoolConfigurationEntry('platform', 'auth', default=False)
"""
This config setting should not normally be filled in. Whether or not an admin server requires authentication should be
something published by the admin server itself (typically by returning a 401). However, to help with migration, this
config object is here to force the SDK to attempt the auth flow even without prompting by Admin.
"""
