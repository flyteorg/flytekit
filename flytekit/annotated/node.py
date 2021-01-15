from __future__ import annotations

from typing import Any, List

from flytekit.common.utils import _dnsify
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model


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
        self._id = _dnsify(id)
        self._metadata = metadata
        self._bindings = bindings
        self._upstream_nodes = upstream_nodes
        self._flyte_entity = flyte_entity
        self._sdk_node = None
        self._aliases: _workflow_model.Alias = None

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

    @property
    def id(self) -> str:
        return self._id

    @property
    def bindings(self) -> List[_literal_models.Binding]:
        return self._bindings

    @property
    def upstream_nodes(self) -> List[Node]:
        return self._upstream_nodes

    def with_overrides(self, *args, **kwargs):
        if "node_name" in kwargs:
            self._id = kwargs["node_name"]
        if "aliases" in kwargs:
            alias_dict = kwargs["aliases"]
            if not isinstance(alias_dict, dict):
                raise AssertionError("Aliases should be specified as dict[str, str]")
            self._aliases = []
            for k, v in alias_dict.items():
                self._aliases.append(_workflow_model.Alias(var=k, alias=v))
