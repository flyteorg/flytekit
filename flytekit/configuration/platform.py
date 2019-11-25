from __future__ import absolute_import

from flytekit.configuration import common as _config_common
from flytekit.common import constants as _constants

URL = _config_common.FlyteRequiredStringConfigurationEntry('platform', 'url')
INSECURE = _config_common.FlyteBoolConfigurationEntry('platform', 'insecure', default=False)
USERNAME = _config_common.FlyteStringConfigurationEntry('platform', 'username')
PASSWORD = _config_common.FlyteStringConfigurationEntry('platform', 'password')
CLOUD_PROVIDER = _config_common.FlyteStringConfigurationEntry(
    'platform', 'cloud_provider', default=_constants.CloudProvider.AWS
)
