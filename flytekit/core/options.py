import typing
import warnings
from dataclasses import dataclass

from flytekit.models import common as common_models
from flytekit.models import security


@dataclass
class Options(object):
    """
    These are options that can be configured for a launch plan during registration or overridden during an execution.
    For instance, two people may want to run the same workflow but have the offloaded data stored in two different
    buckets. Or you may want labels or annotations to be different. This object is used when launching an execution
    in a Flyte backend, and also when registering launch plans.

    Attributes:
        labels (typing.Optional[common_models.Labels]): Custom labels to be applied to the execution resource.
        annotations (typing.Optional[common_models.Annotations]): Custom annotations to be applied to the execution resource.
        raw_output_data_config (typing.Optional[common_models.RawOutputDataConfig]): Optional location of offloaded data
            for things like S3, etc. Remote prefix for storage location of the form ``s3://<bucket>/key...`` or
            ``gcs://...`` or ``file://...``. If not specified, will use the platform-configured default. This is where
            the data for offloaded types is stored.
        security_context (typing.Optional[security.SecurityContext]): Indicates security context for permissions triggered
            with this launch plan.
        concurrency (typing.Optional[int]): Controls the maximum number of task nodes that can be run in parallel for the
            entire workflow.
        notifications (typing.Optional[typing.List[common_models.Notification]]): List of notifications for this execution.
        disable_notifications (typing.Optional[bool]): Set to True if all notifications are intended to be disabled
            for this execution.
        overwrite_cache (typing.Optional[bool]): When set to True, forces the execution to overwrite any existing cached values.
    """

    labels: typing.Optional[common_models.Labels] = None
    annotations: typing.Optional[common_models.Annotations] = None
    raw_output_data_config: typing.Optional[common_models.RawOutputDataConfig] = None
    security_context: typing.Optional[security.SecurityContext] = None
    concurrency: typing.Optional[int] = None
    notifications: typing.Optional[typing.List[common_models.Notification]] = None
    disable_notifications: typing.Optional[bool] = None
    overwrite_cache: typing.Optional[bool] = None

    @property
    def max_parallelism(self) -> typing.Optional[int]:
        """
        [Deprecated] Use concurrency instead. This property is maintained for backward compatibility
        """
        warnings.warn(
            "max_parallelism is deprecated and will be removed in a future version. Use concurrency instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.concurrency

    @max_parallelism.setter
    def max_parallelism(self, value: typing.Optional[int]):
        """
        Setter for max_parallelism (deprecated in favor of concurrency)
        """
        warnings.warn(
            "max_parallelism is deprecated and will be removed in a future version. Use concurrency instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.concurrency = value

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
