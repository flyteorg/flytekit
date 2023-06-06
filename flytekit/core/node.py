from __future__ import annotations

import datetime
import typing
from typing import Any, List

from flytekit.core.resources import Resources, convert_resources_to_resource_model
from flytekit.core.utils import _dnsify
from flytekit.loggers import logger
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.models.task import Resources as _resources_model


class Node(object):
    """
    This class will hold all the things necessary to make an SdkNode but we won't make one until we know things like
    ID, which from the registration step
    """

    def __init__(
        self,
        id: str,
        metadata: _workflow_model.NodeMetadata,
        bindings: List[_literal_models.Binding],
        upstream_nodes: List[Node],
        flyte_entity: Any,
    ):
        if id is None:
            raise ValueError("Illegal construction of node, without a Node ID")
        self._id = _dnsify(id)
        self._metadata = metadata
        self._bindings = bindings
        self._upstream_nodes = upstream_nodes
        self._flyte_entity = flyte_entity
        self._aliases: _workflow_model.Alias = None
        self._outputs = None
        self._resources: typing.Optional[_resources_model] = None

    def runs_before(self, other: Node):
        """
        This is typically something we shouldn't do. This modifies an attribute of the other instance rather than
        self. But it's done so only because we wanted this English function to be the same as the shift function.
        That is, calling node_1.runs_before(node_2) and node_1 >> node_2 are the same. The shift operator going the
        other direction is not implemented to further avoid confusion. Right shift was picked rather than left shift
        because that's what most users are familiar with.
        """
        if self not in other._upstream_nodes:
            other._upstream_nodes.append(self)

    def __rshift__(self, other: Node):
        self.runs_before(other)
        return other

    @property
    def name(self) -> str:
        return self._id

    @property
    def outputs(self):
        if self._outputs is None:
            raise AssertionError("Cannot use outputs with all Nodes, node must've been created from create_node()")
        return self._outputs

    @property
    def id(self) -> str:
        return self._id

    @property
    def bindings(self) -> List[_literal_models.Binding]:
        return self._bindings

    @property
    def upstream_nodes(self) -> List[Node]:
        return self._upstream_nodes

    @property
    def flyte_entity(self) -> Any:
        return self._flyte_entity

    @property
    def metadata(self) -> _workflow_model.NodeMetadata:
        return self._metadata

    def with_overrides(self, *args, **kwargs):
        if "node_name" in kwargs:
            # Convert the node name into a DNS-compliant.
            # https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
            self._id = _dnsify(kwargs["node_name"])
        if "aliases" in kwargs:
            alias_dict = kwargs["aliases"]
            if not isinstance(alias_dict, dict):
                raise AssertionError("Aliases should be specified as dict[str, str]")
            self._aliases = []
            for k, v in alias_dict.items():
                self._aliases.append(_workflow_model.Alias(var=k, alias=v))
        if "requests" in kwargs or "limits" in kwargs:
            requests = kwargs.get("requests")
            if requests and not isinstance(requests, Resources):
                raise AssertionError("requests should be specified as flytekit.Resources")
            limits = kwargs.get("limits")
            if limits and not isinstance(limits, Resources):
                raise AssertionError("limits should be specified as flytekit.Resources")

            self._resources = convert_resources_to_resource_model(requests=requests, limits=limits)
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
            if timeout is None:
                self._metadata._timeout = datetime.timedelta()
            elif isinstance(timeout, int):
                self._metadata._timeout = datetime.timedelta(seconds=timeout)
            elif isinstance(timeout, datetime.timedelta):
                self._metadata._timeout = timeout
            else:
                raise ValueError("timeout should be duration represented as either a datetime.timedelta or int seconds")
        if "retries" in kwargs:
            retries = kwargs["retries"]
            self._metadata._retries = (
                _literal_models.RetryStrategy(0) if retries is None else _literal_models.RetryStrategy(retries)
            )
        if "interruptible" in kwargs:
            self._metadata._interruptible = kwargs["interruptible"]
        if "name" in kwargs:
            self._metadata._name = kwargs["name"]
        if "task_config" in kwargs:
            logger.warning("This override is beta. We may want to revisit this in the future.")
            new_task_config = kwargs["task_config"]
            if not isinstance(new_task_config, type(self.flyte_entity._task_config)):
                raise ValueError("can't change the type of the task config")
            self.flyte_entity._task_config = new_task_config
        if "container_image" in kwargs:
            self.flyte_entity._container_image = kwargs["container_image"]
        return self


def _convert_resource_overrides(
    resources: typing.Optional[Resources], resource_name: str
) -> typing.List[_resources_model.ResourceEntry]:
    if resources is None:
        return []

    resource_entries = []
    if resources.cpu is not None:
        resource_entries.append(_resources_model.ResourceEntry(_resources_model.ResourceName.CPU, resources.cpu))

    if resources.mem is not None:
        resource_entries.append(_resources_model.ResourceEntry(_resources_model.ResourceName.MEMORY, resources.mem))

    if resources.gpu is not None:
        resource_entries.append(_resources_model.ResourceEntry(_resources_model.ResourceName.GPU, resources.gpu))

    if resources.storage is not None:
        resource_entries.append(
            _resources_model.ResourceEntry(_resources_model.ResourceName.STORAGE, resources.storage)
        )
    if resources.ephemeral_storage is not None:
        resource_entries.append(
            _resources_model.ResourceEntry(
                _resources_model.ResourceName.EPHEMERAL_STORAGE,
                resources.ephemeral_storage,
            )
        )

    return resource_entries
