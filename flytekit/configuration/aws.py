from flytekit.configuration import common as _config_common

S3_SHARD_FORMATTER = _config_common.FlyteRequiredStringConfigurationEntry("aws", "s3_shard_formatter")

S3_SHARD_STRING_LENGTH = _config_common.FlyteIntegerConfigurationEntry("aws", "s3_shard_string_length", default=2)

S3_ENDPOINT = _config_common.FlyteStringConfigurationEntry("aws", "endpoint", default=None)

S3_ACCESS_KEY_ID = _config_common.FlyteStringConfigurationEntry("aws", "access_key_id", default=None)

S3_SECRET_ACCESS_KEY = _config_common.FlyteStringConfigurationEntry("aws", "secret_access_key", default=None)

S3_ACCESS_KEY_ID_ENV_NAME = "AWS_ACCESS_KEY_ID"

S3_SECRET_ACCESS_KEY_ENV_NAME = "AWS_SECRET_ACCESS_KEY"

S3_ENDPOINT_ARG_NAME = "--endpoint-url"

ENABLE_DEBUG = _config_common.FlyteBoolConfigurationEntry("aws", "enable_debug", default=False)

RETRIES = _config_common.FlyteIntegerConfigurationEntry("aws", "retries", default=3)

BACKOFF_SECONDS = _config_common.FlyteIntegerConfigurationEntry("aws", "backoff_seconds", default=5)
