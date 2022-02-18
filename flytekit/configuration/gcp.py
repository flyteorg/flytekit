from flytekit.configuration import common as _config_common

GSUTIL_PARALLELISM = _config_common.FlyteBoolConfigurationEntry("gcp", "gsutil_parallelism", default=False)
