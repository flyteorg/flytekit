import math
from typing import TYPE_CHECKING, Any, List, Optional, Set, Tuple, Union

from flyteidl.core import workflow_pb2 as _core_workflow

from flytekit.core import interface as flyte_interface
from flytekit.core.context_manager import ExecutionState, FlyteContext
from flytekit.core.interface import (
    transform_interface_to_list_interface,
    transform_interface_to_typed_interface,
)
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.node import Node
from flytekit.core.promise import (
    Promise,
    VoidPromise,
    create_and_link_node,
    create_and_link_node_from_remote,
    flyte_entity_call_handler,
    translate_inputs_to_literals,
)
from flytekit.core.task import ReferenceTask
from flytekit.loggers import logger
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.models.literals import Literal, LiteralCollection, Scalar

ARRAY_NODE_SUBNODE_NAME = "array_node_subnode"

if TYPE_CHECKING:
    from flytekit.remote import FlyteLaunchPlan


class ArrayNode:
    def __init__(
        self,
        target: Union[LaunchPlan, ReferenceTask, "FlyteLaunchPlan"],
        bindings: Optional[List[_literal_models.Binding]] = None,
        concurrency: Optional[int] = None,
        min_successes: Optional[int] = None,
        min_success_ratio: Optional[float] = None,
        metadata: Optional[_workflow_model.NodeMetadata] = None,
    ):
        """
        :param target: The target Flyte entity to map over
        :param concurrency: If specified, this limits the number of mapped tasks than can run in parallel to the given batch
            size. If the size of the input exceeds the concurrency value, then multiple batches will be run serially until
            all inputs are processed. If set to 0, this means unbounded concurrency. If left unspecified, this means the
            array node will inherit parallelism from the workflow
        :param min_successes: The minimum number of successful executions. If set, this takes precedence over
            min_success_ratio
        :param min_success_ratio: The minimum ratio of successful executions.
        :param metadata: The metadata for the underlying node
        """
        from flytekit.remote import FlyteLaunchPlan

        self.target = target
        self._concurrency = concurrency
        self.id = target.name
        self._bindings = bindings or []
        self.metadata = metadata
        self._data_mode = None
        self._execution_mode = None

        if min_successes is not None:
            self._min_successes = min_successes
            self._min_success_ratio = None
        else:
            self._min_success_ratio = min_success_ratio if min_success_ratio is not None else 1.0
            self._min_successes = 0

        if self.target.python_interface:
            n_outputs = len(self.target.python_interface.outputs)
        else:
            n_outputs = len(self.target.interface.outputs)
        if n_outputs > 1:
            raise ValueError("Only tasks with a single output are supported in map tasks.")

        # TODO - bound inputs are not supported at the moment
        self._bound_inputs: Set[str] = set()

        output_as_list_of_optionals = min_success_ratio is not None and min_success_ratio != 1 and n_outputs == 1

        self._remote_interface = None
        if self.target.python_interface:
            self._python_interface = transform_interface_to_list_interface(
                self.target.python_interface, self._bound_inputs, output_as_list_of_optionals
            )
        elif self.target.interface:
            self._remote_interface = self.target.interface.transform_interface_to_list()
        else:
            raise ValueError("No interface found for the target entity.")

        if isinstance(target, (LaunchPlan, FlyteLaunchPlan)):
            self._data_mode = _core_workflow.ArrayNode.SINGLE_INPUT_FILE
            self._execution_mode = _core_workflow.ArrayNode.FULL_STATE
        elif isinstance(target, ReferenceTask):
            self._data_mode = _core_workflow.ArrayNode.INDIVIDUAL_INPUT_FILES
            self._execution_mode = _core_workflow.ArrayNode.MINIMAL_STATE
        else:
            raise ValueError(f"Only LaunchPlans are supported for now, but got {type(target)}")

    def construct_node_metadata(self) -> _workflow_model.NodeMetadata:
        # Part of SupportsNodeCreation interface
        return self.metadata or _workflow_model.NodeMetadata(name=self.target.name)

    @property
    def name(self) -> str:
        # Part of SupportsNodeCreation interface
        return self.target.name

    @property
    def python_interface(self) -> flyte_interface.Interface:
        # Part of SupportsNodeCreation interface
        return self._python_interface

    @property
    def interface(self) -> _interface_models.TypedInterface:
        # Required in get_serializable_node
        if self._remote_interface:
            return self._remote_interface
        raise AttributeError("interface attribute is not available")

    @property
    def bindings(self) -> List[_literal_models.Binding]:
        # Required in get_serializable_node
        return self._bindings

    @property
    def upstream_nodes(self) -> List[Node]:
        # Required in get_serializable_node
        return []

    @property
    def flyte_entity(self) -> Any:
        return self.target

    @property
    def data_mode(self) -> _core_workflow.ArrayNode.DataMode:
        return self._data_mode

    def local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise]:
        if self._remote_interface:
            raise ValueError("Mapping over remote entities is not supported in local execution.")

        outputs_expected = True
        if not self.python_interface.outputs:
            outputs_expected = False

        mapped_entity_count = 0
        for binding in self.bindings:
            k = binding.var
            if k not in self._bound_inputs:
                v = kwargs[k]
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], self.target.python_interface.inputs[k]):
                    mapped_entity_count = len(v)
                    break
                else:
                    raise ValueError(
                        f"Expected a list of {self.target.python_interface.inputs[k]} but got {type(v)} instead."
                    )

        failed_count = 0
        min_successes = mapped_entity_count
        if self._min_successes:
            min_successes = self._min_successes
        elif self._min_success_ratio:
            min_successes = math.ceil(min_successes * self._min_success_ratio)

        literals = []
        for i in range(mapped_entity_count):
            single_instance_inputs = {}
            for binding in self.bindings:
                k = binding.var
                if k not in self._bound_inputs:
                    single_instance_inputs[k] = kwargs[k][i]
                else:
                    single_instance_inputs[k] = kwargs[k]

            # translate Python native inputs to Flyte literals
            typed_interface = transform_interface_to_typed_interface(self.target.python_interface)
            literal_map = translate_inputs_to_literals(
                ctx,
                incoming_values=single_instance_inputs,
                flyte_interface_types={} if typed_interface is None else typed_interface.inputs,
                native_types=self.target.python_interface.inputs,
            )
            kwargs_literals = {k1: Promise(var=k1, val=v1) for k1, v1 in literal_map.items()}

            try:
                output = self.target.__call__(**kwargs_literals)
                if outputs_expected:
                    literals.append(output.val)
            except Exception as exc:
                if outputs_expected:
                    literal_with_none = Literal(scalar=Scalar(none_type=_literal_models.Void()))
                    literals.append(literal_with_none)
                    failed_count += 1
                    if mapped_entity_count - failed_count < min_successes:
                        logger.error("The number of successful tasks is lower than the minimum")
                        raise exc

        if outputs_expected:
            return Promise(var="o0", val=Literal(collection=LiteralCollection(literals=literals)))
        return VoidPromise(self.name)

    def local_execution_mode(self):
        return ExecutionState.Mode.LOCAL_TASK_EXECUTION

    @property
    def min_success_ratio(self) -> Optional[float]:
        return self._min_success_ratio

    @property
    def min_successes(self) -> Optional[int]:
        return self._min_successes

    @property
    def concurrency(self) -> Optional[int]:
        return self._concurrency

    @property
    def execution_mode(self) -> _core_workflow.ArrayNode.ExecutionMode:
        return self._execution_mode

    @property
    def is_original_sub_node_interface(self) -> bool:
        return True

    def __call__(self, *args, **kwargs):
        if not self._bindings:
            ctx = FlyteContext.current_context()
            # since a new entity with an updated list interface is not created, we have to work around the mismatch
            # between the interface and the inputs. Also, don't link the node to the compilation state,
            # since we don't want to add the subnode to the workflow as a node
            if self._remote_interface:
                bound_subnode = create_and_link_node_from_remote(
                    ctx,
                    entity=self.flyte_entity,
                    add_node_to_compilation_state=False,
                    overridden_interface=self._remote_interface,
                    **kwargs,
                )
                self._bindings = bound_subnode.ref.node.bindings
                return create_and_link_node_from_remote(
                    ctx,
                    entity=self,
                    **kwargs,
                )
            bound_subnode = create_and_link_node(
                ctx,
                entity=self.flyte_entity,
                add_node_to_compilation_state=False,
                overridden_interface=self.python_interface,
                node_id=ARRAY_NODE_SUBNODE_NAME,
                **kwargs,
            )
            self._bindings = bound_subnode.ref.node.bindings
        return flyte_entity_call_handler(self, *args, **kwargs)


def array_node(
    target: Union[LaunchPlan, ReferenceTask, "FlyteLaunchPlan"],
    concurrency: Optional[int] = None,
    min_success_ratio: Optional[float] = None,
    min_successes: Optional[int] = None,
):
    """
    ArrayNode implementation that maps over tasks and other Flyte entities

    :param target: The target Flyte entity to map over
    :param concurrency: If specified, this limits the number of mapped tasks than can run in parallel to the given batch
        size. If the size of the input exceeds the concurrency value, then multiple batches will be run serially until
        all inputs are processed. If set to 0, this means unbounded concurrency. If left unspecified, this means the
        array node will inherit parallelism from the workflow
    :param min_successes: The minimum number of successful executions. If set, this takes precedence over
        min_success_ratio
    :param min_success_ratio: The minimum ratio of successful executions
    :return: A callable function that takes in keyword arguments and returns a Promise created by
        flyte_entity_call_handler
    """
    from flytekit.remote import FlyteLaunchPlan

    if not isinstance(target, (LaunchPlan, FlyteLaunchPlan, ReferenceTask)):
        raise ValueError("Only LaunchPlans and ReferenceTasks are supported for now.")

    node = ArrayNode(
        target=target,
        concurrency=concurrency,
        min_successes=min_successes,
        min_success_ratio=min_success_ratio,
    )

    return node
