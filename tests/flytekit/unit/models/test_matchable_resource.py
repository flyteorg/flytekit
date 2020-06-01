from __future__ import absolute_import

from flytekit.models import matchable_resource


def test_cluster_resource_attributes():
    obj = matchable_resource.ClusterResourceAttributes({"cpu": "one million", "gpu": "just one"})
    assert obj.attributes == {"cpu": "one million", "gpu": "just one"}
    assert obj == matchable_resource.ClusterResourceAttributes.from_flyte_idl(obj.to_flyte_idl())


def test_execution_queue_attributes():
    obj = matchable_resource.ExecutionQueueAttributes(["foo", "bar", "baz"])
    assert obj.tags == ["foo", "bar", "baz"]
    assert obj == matchable_resource.ExecutionQueueAttributes.from_flyte_idl(obj.to_flyte_idl())


def test_matchable_resource():
    cluster_resource_attrs = matchable_resource.ClusterResourceAttributes({"cpu": "one million", "gpu": "just one"})
    obj = matchable_resource.MatchingAttributes(cluster_resource_attributes=cluster_resource_attrs)
    assert obj.cluster_resource_attributes == cluster_resource_attrs
    assert obj == matchable_resource.MatchingAttributes.from_flyte_idl(obj.to_flyte_idl())

    execution_queue_attributes = matchable_resource.ExecutionQueueAttributes(["foo", "bar", "baz"])
    obj2 = matchable_resource.MatchingAttributes(execution_queue_attributes=execution_queue_attributes)
    assert obj2.execution_queue_attributes == execution_queue_attributes
    assert obj2 == matchable_resource.MatchingAttributes.from_flyte_idl(obj2.to_flyte_idl())
