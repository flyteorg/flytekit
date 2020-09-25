from flytekit.configuration import common as _config_common

ASSUMABLE_IAM_ROLE = _config_common.FlyteStringConfigurationEntry("auth", "assumable_iam_role", default=None)
"""
This is the role the SDK will use by default to execute workflows.  For example, in AWS this should be an IAM role
string.
"""

KUBERNETES_SERVICE_ACCOUNT = _config_common.FlyteStringConfigurationEntry(
    "auth", "kubernetes_service_account", default=None
)
"""
This is the kubernetes service account that will be passed to workflow executions.
"""

RAW_OUTPUT_DATA_PREFIX = _config_common.FlyteStringConfigurationEntry("auth", "raw_output_data_prefix", default="")
"""
This is not output metadata but rather where users can specify an S3 or gcs path for offloaded data like blobs
and schemas.

The reason this setting is in this file is because it's inextricably tied to a workflow's role or service account,
since that is what ultimately gives the tasks the ability to write to certain buckets.
"""
