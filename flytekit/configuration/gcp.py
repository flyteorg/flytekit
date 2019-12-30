from __future__ import absolute_import

from flytekit.configuration import common as _config_common

GCS_PREFIX = _config_common.FlyteRequiredStringConfigurationEntry('gcp', 'gcs_prefix')
