from flyteidl.admin import matchable_resource_pb2 as _matchable_resource

from flytekit.models import common as _common


class MatchableResource(object):
    TASK_RESOURCE = _matchable_resource.TASK_RESOURCE
    CLUSTER_RESOURCE = _matchable_resource.CLUSTER_RESOURCE
    EXECUTION_QUEUE = _matchable_resource.EXECUTION_QUEUE
    EXECUTION_CLUSTER_LABEL = _matchable_resource.EXECUTION_CLUSTER_LABEL
    QUALITY_OF_SERVICE_SPECIFICATION = _matchable_resource.QUALITY_OF_SERVICE_SPECIFICATION

    @classmethod
    def enum_to_string(cls, val):
        """
        :param int val:
        :rtype: Text
        """
        if val == cls.TASK_RESOURCE:
            return "TASK_RESOURCE"
        elif val == cls.CLUSTER_RESOURCE:
            return "CLUSTER_RESOURCE"
        elif val == cls.EXECUTION_QUEUE:
            return "EXECUTION_QUEUE"
        elif val == cls.EXECUTION_CLUSTER_LABEL:
            return "EXECUTION_CLUSTER_LABEL"
        elif val == cls.QUALITY_OF_SERVICE_SPECIFICATION:
            return "QUALITY_OF_SERVICE_SPECIFICATION"
        else:
            return "<UNKNOWN>"

    @classmethod
    def string_to_enum(cls, val):
        """
        :param Text val:
        :rtype: int
        """
        if val == "TASK_RESOURCE":
            return cls.TASK_RESOURCE
        elif val == "CLUSTER_RESOURCE":
            return cls.CLUSTER_RESOURCE
        elif val == "EXECUTION_QUEUE":
            return cls.EXECUTION_QUEUE
        elif val == "EXECUTION_CLUSTER_LABEL":
            return cls.EXECUTION_CLUSTER_LABEL
        elif val == cls.QUALITY_OF_SERVICE_SPECIFICATION:
            return "QUALITY_OF_SERVICE_SPECIFICATION"
        else:
            return "<UNKNOWN>"


class ClusterResourceAttributes(_common.FlyteIdlEntity):
    def __init__(self, attributes):
        """
        Custom resource attributes which will be applied in cluster resource creation (e.g. quotas).
        Dict keys are the *case-sensitive* names of variables in templatized resource files.
        Dict values should be the custom values which get substituted during resource creation.

        :param dict[Text, Text] attributes: Applied in cluster resource creation (e.g. quotas).
        """
        self._attributes = attributes

    @property
    def attributes(self):
        """
        Custom resource attributes which will be applied in cluster resource management
        :rtype: dict[Text, Text]
        """
        return self._attributes

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.matchable_resource_pb2.ClusterResourceAttributes
        """
        return _matchable_resource.ClusterResourceAttributes(attributes=self.attributes,)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.matchable_resource_pb2.ClusterResourceAttributes pb2_object:
        :rtype: ClusterResourceAttributes
        """
        return cls(attributes=pb2_object.attributes,)


class ExecutionQueueAttributes(_common.FlyteIdlEntity):
    def __init__(self, tags):
        """
        Tags used for assigning execution queues for tasks matching a project, domain and optionally, workflow.

        :param list[Text] tags:
        """
        self._tags = tags

    @property
    def tags(self):
        """
        :rtype: list[Text]
        """
        return self._tags

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.matchable_resource_pb2.ExecutionQueueAttributes
        """
        return _matchable_resource.ExecutionQueueAttributes(tags=self.tags,)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.matchable_resource_pb2.ExecutionQueueAttributes pb2_object:
        :rtype: ExecutionQueueAttributes
        """
        return cls(tags=pb2_object.tags,)


class ExecutionClusterLabel(_common.FlyteIdlEntity):
    def __init__(self, value):
        """
        Label value to determine where the execution will be run

        :param Text value:
        """
        self._value = value

    @property
    def value(self):
        """
        :rtype: Text
        """
        return self._value

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.matchable_resource_pb2.ExecutionClusterLabel
        """
        return _matchable_resource.ExecutionClusterLabel(value=self.value,)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.matchable_resource_pb2.ExecutionClusterLabel pb2_object:
        :rtype: ExecutionClusterLabel
        """
        return cls(value=pb2_object.value,)


class MatchingAttributes(_common.FlyteIdlEntity):
    def __init__(
        self, cluster_resource_attributes=None, execution_queue_attributes=None, execution_cluster_label=None,
    ):
        """
        At most one target from cluster_resource_attributes, execution_queue_attributes or execution_cluster_label
            can be set.
        :param ClusterResourceAttributes cluster_resource_attributes:
        :param ExecutionQueueAttributes execution_queue_attributes:
        :param ExecutionClusterLabel execution_cluster_label:
        """
        if cluster_resource_attributes:
            if execution_queue_attributes or execution_cluster_label:
                raise ValueError("Only one target can be set")
        elif execution_queue_attributes and execution_cluster_label:
            raise ValueError("Only one target can be set")

        self._cluster_resource_attributes = cluster_resource_attributes
        self._execution_queue_attributes = execution_queue_attributes
        self._execution_cluster_label = execution_cluster_label

    @property
    def cluster_resource_attributes(self):
        """
        Custom resource attributes which will be applied in cluster resource creation (e.g. quotas).
        :rtype: ClusterResourceAttributes
        """
        return self._cluster_resource_attributes

    @property
    def execution_queue_attributes(self):
        """
        Tags used for assigning execution queues for tasks.
        :rtype: ExecutionQueueAttributes
        """
        return self._execution_queue_attributes

    @property
    def execution_cluster_label(self):
        """
        Label value to determine where the execution will be run.
        :rtype: ExecutionClusterLabel
        """
        return self._execution_cluster_label

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.matchable_resource_pb2.MatchingAttributes
        """
        return _matchable_resource.MatchingAttributes(
            cluster_resource_attributes=self.cluster_resource_attributes.to_flyte_idl()
            if self.cluster_resource_attributes
            else None,
            execution_queue_attributes=self.execution_queue_attributes.to_flyte_idl()
            if self.execution_queue_attributes
            else None,
            execution_cluster_label=self.execution_cluster_label.to_flyte_idl()
            if self.execution_cluster_label
            else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.matchable_resource_pb2.MatchingAttributes pb2_object:
        :rtype: MatchingAttributes
        """
        return cls(
            cluster_resource_attributes=ClusterResourceAttributes.from_flyte_idl(pb2_object.cluster_resource_attributes)
            if pb2_object.HasField("cluster_resource_attributes")
            else None,
            execution_queue_attributes=ExecutionQueueAttributes.from_flyte_idl(pb2_object.execution_queue_attributes)
            if pb2_object.HasField("execution_queue_attributes")
            else None,
            execution_cluster_label=ExecutionClusterLabel.from_flyte_idl(pb2_object.execution_cluster_label)
            if pb2_object.HasField("execution_cluster_label")
            else None,
        )
