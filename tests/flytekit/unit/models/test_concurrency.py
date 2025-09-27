from flyteidl.admin import launch_plan_pb2 as _launch_plan_idl

from flytekit.models.concurrency import ConcurrencyLimitBehavior, ConcurrencyPolicy


def test_concurrency_limit_behavior():
    assert ConcurrencyLimitBehavior.SKIP == _launch_plan_idl.CONCURRENCY_LIMIT_BEHAVIOR_SKIP

    # Test enum to string conversion
    assert ConcurrencyLimitBehavior.enum_to_string(ConcurrencyLimitBehavior.SKIP) == "SKIP"
    assert ConcurrencyLimitBehavior.enum_to_string(999) == "<UNKNOWN>"


def test_concurrency_policy():
    policy = ConcurrencyPolicy(max_concurrency=1, behavior=ConcurrencyLimitBehavior.SKIP)

    assert policy.max_concurrency == 1
    assert policy.behavior == ConcurrencyLimitBehavior.SKIP

    # Test serialization to protobuf
    pb = policy.to_flyte_idl()
    assert isinstance(pb, _launch_plan_idl.ConcurrencyPolicy)
    assert pb.max == 1
    assert pb.behavior == _launch_plan_idl.CONCURRENCY_LIMIT_BEHAVIOR_SKIP

    # Test deserialization from protobuf
    policy2 = ConcurrencyPolicy.from_flyte_idl(pb)
    assert policy2.max_concurrency == 1
    assert policy2.behavior == ConcurrencyLimitBehavior.SKIP


def test_concurrency_policy_with_different_max():
    # Test with a higher max value
    policy = ConcurrencyPolicy(max_concurrency=5, behavior=ConcurrencyLimitBehavior.SKIP)
    assert policy.max_concurrency == 5

    pb = policy.to_flyte_idl()
    assert pb.max == 5

    policy2 = ConcurrencyPolicy.from_flyte_idl(pb)
    assert policy2.max_concurrency == 5
