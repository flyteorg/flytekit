from __future__ import annotations

from typing import Any, Dict, List, Union, Optional

from flytekit.annotated.context_manager import ExecutionState, FlyteContext
from flytekit.common.nodes import SdkNode
from flytekit.common.utils import _dnsify
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model
import collections

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
        from flytekit.annotated.launch_plan import LaunchPlan
        from flytekit.annotated.task import PythonTask
        from flytekit.annotated.workflow import Workflow

        if self._flyte_entity is None:
            raise Exception("Node flyte entity none")

        for n in self._upstream_nodes:
            if n._sdk_node is None:
                n.get_registerable_entity()
        sdk_nodes = [n.get_registerable_entity() for n in self._upstream_nodes]

        if isinstance(self._flyte_entity, PythonTask):
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
        elif isinstance(self._flyte_entity, LaunchPlan):
            self._sdk_node = SdkNode(
                self._id,
                upstream_nodes=sdk_nodes,
                bindings=self._bindings,
                metadata=self._metadata,
                sdk_launch_plan=self._flyte_entity.get_registerable_entity(),
            )
        else:
            raise Exception("not a task or workflow, not sure what to do")

        return self._sdk_node

    @property
    def id(self) -> str:
        return self._id

    def __getattr__(self, item):
        """
        There's two syntaxes we can go for. When a user does
            n1 = create_node("fds", t1, inputs={'a': a})  # x, y = t1(a=a)
        we can have the user access the outputs via
            n1.output_0 OR n1.outputs.output_0.
        I think the former is cleaner for two reasons
        """
        # if has

    # TODO: Figure out import cycles in the future
    from flytekit.annotated.launch_plan import LaunchPlan
    from flytekit.annotated.task import PythonTask
    from flytekit.annotated.workflow import Workflow

    @classmethod
    def create_node(cls, node_id: str, flyte_entity: Union[PythonTask, Workflow, LaunchPlan],
                    inputs: Optional[Dict[str, Any]]) -> Node:

        inputs = inputs or {}  # in cases where the task doesn't take inputs
        result = flyte_entity(**inputs)  # Can be None, Promise, Tuple[Promise], native value(s)


        ctx = FlyteContext.current_context()
        # When in compilation, need to actually create a node
        if ctx.compilation_state is not None:
            # Creating the node is done by calling the given task/launch plan/subworkflow, since those __call__ methods
            # will automatically create the node.
            flyte_entity(**inputs)

        # When in local execution we need to call the function and retrieve args.
        elif (
                ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION
        ):
            ...
