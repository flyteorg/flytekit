import typing
from dataclasses import dataclass

from flytekit.models import common as common_models
from flytekit.models import security


@dataclass
class Options(object):
    """
    These are options that can be configured for a launchplan during registration or overridden during an execution.
    For instance two people may want to run the same workflow but have the offloaded data stored in two different
    buckets. Or you may want labels or annotations to be different. This object is used when launching an execution
    in a Flyte backend, and also when registering launch plans.

    Args:
        labels: Custom labels to be applied to the execution resource
        annotations: Custom annotations to be applied to the execution resource
        security_context: Indicates security context for permissions triggered with this launch plan
        raw_output_data_config: Optional location of offloaded data for things like S3, etc.
            remote prefix for storage location of the form ``s3://<bucket>/key...`` or
            ``gcs://...`` or ``file://...``. If not specified will use the platform configured default. This is where
            the data for offloaded types is stored.
        max_parallelism: Controls the maximum number of tasknodes that can be run in parallel for the entire workflow.
        notifications: List of notifications for this execution.
        disable_notifications: This should be set to true if all notifications are intended to be disabled for this execution.
    """

    labels: typing.Optional[common_models.Labels] = None
    annotations: typing.Optional[common_models.Annotations] = None
    raw_output_data_config: typing.Optional[common_models.RawOutputDataConfig] = None
    security_context: typing.Optional[security.SecurityContext] = None
    max_parallelism: typing.Optional[int] = None
    notifications: typing.Optional[typing.List[common_models.Notification]] = None
    disable_notifications: typing.Optional[bool] = None
    overwrite_cache: typing.Optional[bool] = None

    @classmethod
    def default_from(
        cls,
        k8s_service_account: typing.Optional[str] = None,
        raw_data_prefix: typing.Optional[str] = None,
    ) -> "Options":
        return cls(
            security_context=(
                security.SecurityContext(run_as=security.Identity(k8s_service_account=k8s_service_account))
                if k8s_service_account
                else None
            ),
            raw_output_data_config=(
                common_models.RawOutputDataConfig(output_location_prefix=raw_data_prefix) if raw_data_prefix else None
            ),
        )
