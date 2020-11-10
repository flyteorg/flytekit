from __future__ import annotations

import datetime
from typing import Any, List, Optional

from flytekit.annotated import interface as flyte_interface
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.promise import Promise, binding_from_python_std, create_task_output
from flytekit.common import constants as _common_constants
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.nodes import SdkNode
from flytekit.common.promise import NodeOutput as _NodeOutput
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

    def get_registerable_entity(self) -> SdkNode:
        if self._sdk_node is not None:
            return self._sdk_node
        # TODO: Figure out import cycles in the future
        from flytekit.annotated.condition import BranchNode
        from flytekit.annotated.launch_plan import LaunchPlan
        from flytekit.annotated.task import PythonTask
        from flytekit.annotated.workflow import Workflow

        if self._flyte_entity is None:
            raise Exception(f"Node {self.id} has no flyte entity")

        sdk_nodes = [
            n.get_registerable_entity() for n in self._upstream_nodes if n.id != _common_constants.GLOBAL_INPUT_NODE_ID
        ]

        if isinstance(self._flyte_entity, PythonTask):
            self._sdk_node = SdkNode(
                self._id,
                upstream_nodes=sdk_nodes,
                bindings=self._bindings,
                metadata=self._metadata,
                sdk_task=self._flyte_entity.get_registerable_entity(),
            )
            if self._aliases:
                self._sdk_node._output_aliases = self._aliases
        elif isinstance(self._flyte_entity, Workflow):
            self._sdk_node = SdkNode(
                self._id,
                upstream_nodes=sdk_nodes,
                bindings=self._bindings,
                metadata=self._metadata,
                sdk_workflow=self._flyte_entity.get_registerable_entity(),
            )
        elif isinstance(self._flyte_entity, BranchNode):
            self._sdk_node = SdkNode(
                self._id,
                upstream_nodes=sdk_nodes,
                bindings=self._bindings,
                metadata=self._metadata,
                sdk_branch=self._flyte_entity.get_registerable_entity(),
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

    @property
    def bindings(self) -> List[_literal_models.Binding]:
        return self._bindings

    @property
    def upstream_nodes(self) -> List["Node"]:
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


def create_and_link_node(
    ctx: FlyteContext,
    entity,
    interface: flyte_interface.Interface,
    *args,
    timeout: Optional[datetime.timedelta] = None,
    retry_strategy: Optional[_literal_models.RetryStrategy] = None,
    **kwargs,
):
    """
    This method is used to generate a node with bindings. This is not used in the execution path.
    """
    if ctx.compilation_state is None:
        raise _user_exceptions.FlyteAssertion("Cannot create node when not compiling...")

    used_inputs = set()
    bindings = []

    typed_interface = flyte_interface.transform_interface_to_typed_interface(interface)

    for k in sorted(interface.inputs):
        var = typed_interface.inputs[k]
        if k not in kwargs:
            raise _user_exceptions.FlyteAssertion("Input was not specified for: {} of type {}".format(k, var.type))
        bindings.append(
            binding_from_python_std(
                ctx, var_name=k, expected_literal_type=var.type, t_value=kwargs[k], t_value_type=interface.inputs[k]
            )
        )
        used_inputs.add(k)

    extra_inputs = used_inputs ^ set(kwargs.keys())
    if len(extra_inputs) > 0:
        raise _user_exceptions.FlyteAssertion(
            "Too many inputs were specified for the interface.  Extra inputs were: {}".format(extra_inputs)
        )

    # Detect upstream nodes
    # These will be our annotated Nodes until we can amend the Promise to use NodeOutputs that reference our Nodes
    upstream_nodes = list(
        set(
            [
                input_val.ref.sdk_node
                for input_val in kwargs.values()
                if isinstance(input_val, Promise) and input_val.ref.node_id != _common_constants.GLOBAL_INPUT_NODE_ID
            ]
        )
    )

    node_metadata = _workflow_model.NodeMetadata(
        f"{entity.__module__}.{entity.name}",
        timeout or datetime.timedelta(),
        retry_strategy or _literal_models.RetryStrategy(0),
    )

    # TODO: Clean up NodeOutput dependency on SdkNode, then rename variable
    non_sdk_node = Node(
        # TODO: Better naming, probably a derivative of the function name.
        id=f"{ctx.compilation_state.prefix}node-{len(ctx.compilation_state.nodes)}",
        metadata=node_metadata,
        bindings=sorted(bindings, key=lambda b: b.var),
        upstream_nodes=upstream_nodes,  # type: ignore
        flyte_entity=entity,
    )
    ctx.compilation_state.add_node(non_sdk_node)

    # Create a node output object for each output, they should all point to this node of course.
    node_outputs = []
    for output_name, output_var_model in typed_interface.outputs.items():
        # TODO: If node id gets updated later, we have to make sure to update the NodeOutput model's ID, which
        #  is currently just a static str
        node_outputs.append(Promise(output_name, _NodeOutput(sdk_node=non_sdk_node, sdk_type=None, var=output_name)))
        # Don't print this, it'll crash cuz sdk_node._upstream_node_ids might be None, but idl code will break

    return create_task_output(node_outputs)
