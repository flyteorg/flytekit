from flytekit.configuration import common as _config_common

GCS_PREFIX = _config_common.FlyteRequiredStringConfigurationEntry("gcp", "gcs_prefix")
GSUTIL_PARALLELISM = _config_common.FlyteBoolConfigurationEntry("gcp", "gsutil_parallelism", default=False)
