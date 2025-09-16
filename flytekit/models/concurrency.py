from flyteidl.admin import launch_plan_pb2 as _launch_plan_idl

from flytekit.models import common as _common


class ConcurrencyLimitBehavior(object):
    SKIP = _launch_plan_idl.CONCURRENCY_LIMIT_BEHAVIOR_SKIP

    @classmethod
    def enum_to_string(cls, val):
        """
        :param int val:
        :rtype: Text
        """
        if val == cls.SKIP:
            return "SKIP"
        else:
            return "<UNKNOWN>"


class ConcurrencyPolicy(_common.FlyteIdlEntity):
    """
    Defines the concurrency policy for a launch plan.
    """

    def __init__(self, max_concurrency: int, behavior: ConcurrencyLimitBehavior = None):
        self._max_concurrency = max_concurrency
        self._behavior = behavior if behavior is not None else ConcurrencyLimitBehavior.SKIP

    @property
    def max_concurrency(self) -> int:
        """
        Maximum number of concurrent workflows allowed.
        """
        return self._max_concurrency

    @property
    def behavior(self) -> ConcurrencyLimitBehavior:
        """
        Policy behavior when concurrency limit is reached.
        """
        return self._behavior

    def to_flyte_idl(self) -> _launch_plan_idl.ConcurrencyPolicy:
        """
        :rtype: flyteidl.admin.launch_plan_pb2.ConcurrencyPolicy
        """
        return _launch_plan_idl.ConcurrencyPolicy(
            max=self.max_concurrency,
            behavior=self.behavior,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _launch_plan_idl.ConcurrencyPolicy) -> "ConcurrencyPolicy":
        """
        :param flyteidl.admin.launch_plan_pb2.ConcurrencyPolicy pb2_object:
        :rtype: ConcurrencyPolicy
        """
        return cls(
            max_concurrency=pb2_object.max,
            behavior=pb2_object.behavior,
        )
