from typing import Any, List

from flytekit.common.nodes import SdkNode
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.common.utils import _dnsify

# TODO: Refactor this into something cleaner once we have a pattern for Tasks/Workflows/Launchplans
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
        upstream_nodes: List["Node"],
        flyte_entity: Any,
    ):
        self._id = _dnsify(id)
        self._metadata = metadata
        self._bindings = bindings
        self._upstream_nodes = upstream_nodes
        self._flyte_entity = flyte_entity
        self._sdk_node = None

    def get_registerable_entity(self) -> SdkNode:
        if self._sdk_node is not None:
            return self._sdk_node
        # TODO: Figure out import cycles in the future
        from flytekit.annotated.task import Task
        from flytekit.annotated.workflow import Workflow

        if self._flyte_entity is None:
            raise Exception("Node flyte entity none")

        if self._flyte_entity._registerable_entity is None:
            print("WARNING!!!!!!!!!!!!!!!!!!!  not really")
            # raise Exception("Node registerable entity has not been set")

        for n in self._upstream_nodes:
            if n._sdk_node is None:
                n.get_registerable_entity()
        sdk_nodes = [n.get_registerable_entity() for n in self._upstream_nodes]

        if isinstance(self._flyte_entity, Task):
            self._sdk_node = SdkNode(
                self._id,
                upstream_nodes=sdk_nodes,
                bindings=self._bindings,
                metadata=self._metadata,
                sdk_task=self._flyte_entity.get_registerable_entity(),
            )
        elif isinstance(self._flyte_entity, Workflow):
            self._sdk_node = SdkNode(
                self._id,
                upstream_nodes=sdk_nodes,
                bindings=self._bindings,
                metadata=self._metadata,
                sdk_workflow=self._flyte_entity.get_registerable_entity(),
            )
        # TODO: Add new annotated LaunchPlan when done
        else:
            raise Exception("not a task or workflow, not sure what to do")

        return self._sdk_node

    @property
    def id(self) -> str:
        return self._id
