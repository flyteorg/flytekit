from __future__ import annotations

import collections
import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

from flytekit.common import constants as _common_constants
from flytekit.common.exceptions.user import FlyteValidationException, FlyteValueException
from flytekit.core.class_based_resolver import ClassStorageTaskResolver
from flytekit.core.condition import ConditionalSection
from flytekit.core.context_manager import (
    BranchEvalMode,
    CompilationState,
    ExecutionState,
    FlyteContext,
    FlyteContextManager,
    FlyteEntities,
)
from flytekit.core.interface import (
    Interface,
    transform_inputs_to_parameters,
    transform_interface_to_typed_interface,
    transform_signature_to_interface,
)
from flytekit.core.node import Node
from flytekit.core.promise import (
    NodeOutput,
    Promise,
    VoidPromise,
    binding_from_python_std,
    create_and_link_node,
    create_task_output,
    translate_inputs_to_literals,
)
from flytekit.core.python_auto_container import PythonAutoContainerTask
from flytekit.core.reference_entity import ReferenceEntity, WorkflowReference
from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import logger
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model

GLOBAL_START_NODE = Node(
    id=_common_constants.GLOBAL_INPUT_NODE_ID,
    metadata=None,
    bindings=[],
    upstream_nodes=[],
    flyte_entity=None,
)


class WorkflowFailurePolicy(Enum):
    FAIL_IMMEDIATELY = _workflow_model.WorkflowMetadata.OnFailurePolicy.FAIL_IMMEDIATELY
    FAIL_AFTER_EXECUTABLE_NODES_COMPLETE = (
        _workflow_model.WorkflowMetadata.OnFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE
    )


@dataclass
class WorkflowMetadata(object):
    on_failure: WorkflowFailurePolicy

    def __post_init__(self):
        if (
            self.on_failure != WorkflowFailurePolicy.FAIL_IMMEDIATELY
            and self.on_failure != WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE
        ):
            raise FlyteValidationException(f"Failure policy {self.on_failure} not acceptable")

    def to_flyte_model(self):
        if self.on_failure == WorkflowFailurePolicy.FAIL_IMMEDIATELY:
            on_failure = 0
        else:
            on_failure = 1
        return _workflow_model.WorkflowMetadata(on_failure=on_failure)


@dataclass
class WorkflowMetadataDefaults(object):
    """
    This class is similarly named to the one above. Please see the IDL for more information but essentially, this
    WorkflowMetadataDefaults class represents the defaults that are handed down to a workflow's tasks, whereas
    WorkflowMetadata represents metadata about the workflow itself.
    """

    interruptible: bool

    def __post_init__(self):
        if self.interruptible is not True and self.interruptible is not False:
            raise FlyteValidationException(f"Interruptible must be boolean, {self.interruptible} invalid")

    def to_flyte_model(self):
        return _workflow_model.WorkflowMetadataDefaults(interruptible=self.interruptible)


def construct_input_promises(inputs: List[str]):
    return {
        input_name: Promise(var=input_name, val=NodeOutput(node=GLOBAL_START_NODE, var=input_name))
        for input_name in inputs
    }


def get_promise(binding_data: _literal_models.BindingData, outputs_cache: Dict[Node, Dict[str, Promise]]) -> Promise:
    """
    This is a helper function that will turn a binding into a Promise object, using a lookup map. Please see
    get_promise_map for the rest of the details.
    """
    if binding_data.promise is not None:
        if not isinstance(binding_data.promise, NodeOutput):
            raise FlyteValidationException(
                f"Binding data Promises have to be of the NodeOutput type {type(binding_data.promise)} found"
            )
        # b.var is the name of the input to the task
        # binding_data.promise.var is the name of the upstream node's output we want
        return outputs_cache[binding_data.promise.node][binding_data.promise.var]
    elif binding_data.scalar is not None:
        return Promise(var="placeholder", val=_literal_models.Literal(scalar=binding_data.scalar))
    elif binding_data.collection is not None:
        literals = []
        for bd in binding_data.collection.bindings:
            p = get_promise(bd, outputs_cache)
            literals.append(p.val)
        return Promise(
            var="placeholder",
            val=_literal_models.Literal(collection=_literal_models.LiteralCollection(literals=literals)),
        )
    elif binding_data.map is not None:
        literals = {}
        for k, bd in binding_data.map.bindings.items():
            p = get_promise(bd, outputs_cache)
            literals[k] = p.val
        return Promise(
            var="placeholder", val=_literal_models.Literal(map=_literal_models.LiteralMap(literals=literals))
        )

    raise FlyteValidationException("Binding type unrecognized.")


def get_promise_map(
    bindings: List[_literal_models.Binding], outputs_cache: Dict[Node, Dict[str, Promise]]
) -> Dict[str, Promise]:
    """
    Local execution of imperatively defined workflows is done node by node. This function will fill in the node's
    entity's input arguments, which are specified using the bindings list, and a map of nodes to its outputs.
    Basically this takes the place of propeller in resolving bindings, pulling in outputs from previously completed
    nodes and filling in the necessary inputs.
    """
    entity_kwargs = {}
    for b in bindings:
        entity_kwargs[b.var] = get_promise(b.binding, outputs_cache)

    return entity_kwargs


class WorkflowBase(object):
    def __init__(
        self,
        name: str,
        workflow_metadata: WorkflowMetadata,
        workflow_metadata_defaults: WorkflowMetadataDefaults,
        python_interface: Interface,
        **kwargs,
    ):
        self._name = name
        self._workflow_metadata = workflow_metadata
        self._workflow_metadata_defaults = workflow_metadata_defaults
        self._python_interface = python_interface
        self._interface = transform_interface_to_typed_interface(python_interface)
        self._inputs = {}
        self._unbound_inputs = set()
        self._nodes = []
        self._output_bindings: Optional[List[_literal_models.Binding]] = []
        FlyteEntities.entities.append(self)
        super().__init__(**kwargs)

    @property
    def name(self) -> str:
        return self._name

    @property
    def short_name(self) -> str:
        return self._name.split(".")[-1]

    @property
    def workflow_metadata(self) -> Optional[WorkflowMetadata]:
        return self._workflow_metadata

    @property
    def workflow_metadata_defaults(self):
        return self._workflow_metadata_defaults

    @property
    def python_interface(self) -> Interface:
        return self._python_interface

    @property
    def interface(self) -> _interface_models.TypedInterface:
        return self._interface

    @property
    def output_bindings(self) -> List[_literal_models.Binding]:
        return self._output_bindings

    @property
    def nodes(self) -> List[Node]:
        return self._nodes

    def __repr__(self):
        return (
            f"WorkflowBase - {self._name} && "
            f"Inputs ({len(self._python_interface.inputs)}): {self._python_interface.inputs} && "
            f"Outputs ({len(self._python_interface.outputs)}): {self._python_interface.outputs} && "
            f"Output bindings: {self._output_bindings} && "
        )

    def __call__(self, *args, **kwargs):
        """
        The call pattern for Workflows is close to, but not exactly, the call pattern for Tasks. For local execution,
        it goes

        __call__ -> _local_execute -> execute

        From execute, different things happen for the two Workflow styles. For PythonFunctionWorkflows, the Python
        function is run, for the ImperativeWorkflow, each node is run one at a time.
        """
        if len(args) > 0:
            raise AssertionError("Only Keyword Arguments are supported for Workflow executions")

        ctx = FlyteContextManager.current_context()

        # Get default agruements and override with kwargs passed in
        input_kwargs = self.python_interface.default_inputs_as_kwargs
        input_kwargs.update(kwargs)

        # The first condition is compilation.
        if ctx.compilation_state is not None:
            return create_and_link_node(ctx, entity=self, interface=self.python_interface, **input_kwargs)

        # This condition is hit when this workflow (self) is being called as part of a parent's workflow local run.
        # The context specifying the local workflow execution has already been set.
        elif (
            ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION
        ):
            if ctx.execution_state.branch_eval_mode == BranchEvalMode.BRANCH_SKIPPED:
                if self.python_interface and self.python_interface.output_tuple_name:
                    variables = [k for k in self.python_interface.outputs.keys()]
                    output_tuple = collections.namedtuple(self.python_interface.output_tuple_name, variables)
                    nones = [None for _ in self.python_interface.outputs.keys()]
                    return output_tuple(*nones)
                else:
                    return None
            # We are already in a local execution, just continue the execution context
            return self._local_execute(ctx, **input_kwargs)

        # Last is starting a local workflow execution
        else:
            # Run some sanity checks
            # Even though the _local_execute call generally expects inputs to be Promises, we don't have to do the
            # conversion here in this loop. The reason is because we don't prevent users from specifying inputs
            # as direct scalars, which means there's another Promise-generating loop inside _local_execute too
            for k, v in input_kwargs.items():
                if k not in self.interface.inputs:
                    raise ValueError(f"Received unexpected keyword argument {k}")
                if isinstance(v, Promise):
                    raise ValueError(f"Received a promise for a workflow call, when expecting a native value for {k}")

            result = None
            with FlyteContextManager.with_context(
                ctx.with_execution_state(
                    ctx.new_execution_state().with_params(mode=ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION)
                )
            ) as child_ctx:
                result = self._local_execute(child_ctx, **input_kwargs)

            expected_outputs = len(self.python_interface.outputs)
            if expected_outputs == 0:
                if result is None or isinstance(result, VoidPromise):
                    return None
                else:
                    raise Exception(f"Workflow local execution expected 0 outputs but something received {result}")

            if (1 < expected_outputs == len(result)) or (result is not None and expected_outputs == 1):
                if isinstance(result, Promise):
                    v = [v for k, v in self.python_interface.outputs.items()][0]  # get output native type
                    return TypeEngine.to_python_value(ctx, result.val, v)
                else:
                    for prom in result:
                        if not isinstance(prom, Promise):
                            raise Exception("should be promises")
                        native_list = [
                            TypeEngine.to_python_value(ctx, promise.val, self.python_interface.outputs[promise.var])
                            for promise in result
                        ]
                        return tuple(native_list)

            raise ValueError("expected outputs and actual outputs do not match")

    def execute(self, **kwargs):
        raise Exception("Should not be called")

    def _local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise]:
        # This is done to support the invariant that Workflow local executions always work with Promise objects
        # holding Flyte literal values. Even in a wf, a user can call a sub-workflow with a Python native value.
        for k, v in kwargs.items():
            if not isinstance(v, Promise):
                t = self.python_interface.inputs[k]
                kwargs[k] = Promise(var=k, val=TypeEngine.to_literal(ctx, v, t, self.interface.inputs[k].type))

        # The output of this will always be a combination of Python native values and Promises containing Flyte
        # Literals.
        function_outputs = self.execute(**kwargs)

        # First handle the empty return case.
        # A workflow function may return a task that doesn't return anything
        #   def wf():
        #       return t1()
        # or it may not return at all
        #   def wf():
        #       t1()
        # In the former case we get the task's VoidPromise, in the latter we get None
        if isinstance(function_outputs, VoidPromise) or function_outputs is None:
            if len(self.python_interface.outputs) != 0:
                raise FlyteValueException(
                    function_outputs,
                    f"{function_outputs} received but interface has {len(self.python_interface.outputs)} outputs.",
                )
            return VoidPromise(self.name)

        # Because we should've already returned in the above check, we just raise an error here.
        if len(self.python_interface.outputs) == 0:
            raise FlyteValueException(
                function_outputs, f"{function_outputs} received but should've been VoidPromise or None."
            )

        expected_output_names = list(self.python_interface.outputs.keys())
        if len(expected_output_names) == 1:
            # Here we have to handle the fact that the wf could've been declared with a typing.NamedTuple of
            # length one. That convention is used for naming outputs - and single-length-NamedTuples are
            # particularly troublesome but elegant handling of them is not a high priority
            # Again, we're using the output_tuple_name as a proxy.
            if self.python_interface.output_tuple_name and isinstance(function_outputs, tuple):
                wf_outputs_as_map = {expected_output_names[0]: function_outputs[0]}
            else:
                wf_outputs_as_map = {expected_output_names[0]: function_outputs}
        else:
            wf_outputs_as_map = {expected_output_names[i]: function_outputs[i] for i, _ in enumerate(function_outputs)}

        # Basically we need to repackage the promises coming from the tasks into Promises that match the workflow's
        # interface. We do that by extracting out the literals, and creating new Promises
        wf_outputs_as_literal_dict = translate_inputs_to_literals(
            ctx,
            wf_outputs_as_map,
            flyte_interface_types=self.interface.outputs,
            native_types=self.python_interface.outputs,
        )
        # Recreate new promises that use the workflow's output names.
        new_promises = [Promise(var, wf_outputs_as_literal_dict[var]) for var in expected_output_names]

        return create_task_output(new_promises, self.python_interface)


class ImperativeWorkflow(WorkflowBase):
    def __init__(
        self,
        name: str,
        failure_policy: Optional[WorkflowFailurePolicy] = None,
        interruptible: Optional[bool] = False,
    ):
        metadata = WorkflowMetadata(on_failure=failure_policy or WorkflowFailurePolicy.FAIL_IMMEDIATELY)
        workflow_metadata_defaults = WorkflowMetadataDefaults(interruptible)
        self._compilation_state = CompilationState(prefix="")
        self._inputs = {}
        self._unbound_inputs = set()
        super().__init__(
            name=name,
            workflow_metadata=metadata,
            workflow_metadata_defaults=workflow_metadata_defaults,
            python_interface=Interface(),
        )

    @property
    def compilation_state(self) -> CompilationState:
        """
        Compilation is done a bit at a time, one task or other entity call at a time. This is why this workflow
        class has to keep track of its own compilation state.
        """
        return self._compilation_state

    @property
    def nodes(self) -> List[Node]:
        return self._compilation_state.nodes

    @property
    def inputs(self) -> Dict[str, Promise]:
        """
        This holds the input promises to the workflow. The nodes in these Promise objects should always point to
        the global start node.
        """
        return self._inputs

    def __repr__(self):
        return super().__repr__() + f"Nodes ({len(self.compilation_state.nodes)}): {self.compilation_state.nodes}"

    def execute(self, **kwargs):
        """
        Called by _local_execute. This function is how local execution for imperative workflows runs. Because when an
        entity is added using the add_entity function, all inputs to that entity should've been already declared, we
        can just iterate through the nodes in order and we shouldn't run into any dependency issues. That is, we force
        the user to declare entities already in a topological sort. To keep track of outputs, we create a map to
        start things off, filled in only with the workflow inputs (if any). As things are run, their outputs are stored
        in this map.
        After all nodes are run, we fill in workflow level outputs the same way as any other previous node.
        """
        if not self.ready():
            raise FlyteValidationException(f"Workflow not ready, wf is currently {self}")

        # Create a map that holds the outputs of each node.
        intermediate_node_outputs = {GLOBAL_START_NODE: {}}  # type: Dict[Node, Dict[str, Promise]]

        # Start things off with the outputs of the global input node, i.e. the inputs to the workflow.
        # _local_execute should've already ensured that all the values in kwargs are Promise objects
        for k, v in kwargs.items():
            intermediate_node_outputs[GLOBAL_START_NODE][k] = v

        # Next iterate through the nodes in order.
        for node in self.compilation_state.nodes:
            if node not in intermediate_node_outputs.keys():
                intermediate_node_outputs[node] = {}

            # Retrieve the entity from the node, and call it by looking up the promises the node's bindings require,
            # and then fill them in using the node output tracker map we have.
            entity = node.flyte_entity
            entity_kwargs = get_promise_map(node.bindings, intermediate_node_outputs)

            # Handle the calling and outputs of each node's entity
            results = entity(**entity_kwargs)
            expected_output_names = list(entity.python_interface.outputs.keys())

            if isinstance(results, VoidPromise) or results is None:
                continue  # pragma: no cover # Move along, nothing to assign

            # Because we should've already returned in the above check, we just raise an Exception here.
            if len(entity.python_interface.outputs) == 0:
                raise FlyteValueException(results, f"{results} received but should've been VoidPromise or None.")

            # if there's only one output,
            if len(expected_output_names) == 1:
                if entity.python_interface.output_tuple_name and isinstance(results, tuple):
                    intermediate_node_outputs[node][expected_output_names[0]] = results[0]
                else:
                    intermediate_node_outputs[node][expected_output_names[0]] = results

            else:
                if len(results) != len(expected_output_names):
                    raise FlyteValueException(results, f"Different lengths {results} {expected_output_names}")
                for idx, r in enumerate(results):
                    intermediate_node_outputs[node][expected_output_names[idx]] = r

        # The rest of this function looks like the above but now we're doing it for the workflow as a whole rather
        # than just one node at a time.
        if len(self.python_interface.outputs) == 0:
            return VoidPromise(self.name)

        # The values that we return below from the output have to be pulled by fulfilling all of the
        # workflow's output bindings.
        # The return style here has to match what 1) what the workflow would've returned had it been declared
        # functionally, and 2) what a user would return in mock function. That is, if it's a tuple, then it
        # should be a tuple here, if it's a one element named tuple, then we do a one-element non-named tuple,
        # if it's a single element then we return a single element
        if len(self.output_bindings) == 1:
            # Again use presence of output_tuple_name to understand that we're dealing with a one-element
            # named tuple
            if self.python_interface.output_tuple_name:
                return (get_promise(self.output_bindings[0].binding, intermediate_node_outputs),)
            # Just a normal single element
            return get_promise(self.output_bindings[0].binding, intermediate_node_outputs)
        return tuple([get_promise(b.binding, intermediate_node_outputs) for b in self.output_bindings])

    def add_entity(self, entity: PythonAutoContainerTask, **kwargs) -> Node:
        """
        Anytime you add an entity, all the inputs to the entity must be bound.
        """
        # circular import
        from flytekit.core.node_creation import create_node

        ctx = FlyteContext.current_context()
        if ctx.compilation_state is not None:
            raise Exception("Can't already be compiling")
        with FlyteContextManager.with_context(ctx.with_compilation_state(self.compilation_state)) as ctx:
            n = create_node(entity=entity, **kwargs)

            def get_input_values(input_value):
                if isinstance(input_value, list):
                    input_promises = []
                    for x in input_value:
                        input_promises.extend(get_input_values(x))
                    return input_promises
                if isinstance(input_value, dict):
                    input_promises = []
                    for _, v in input_value.items():
                        input_promises.extend(get_input_values(v))
                    return input_promises
                else:
                    return [input_value]

            # Every time an entity is added, mark it as used.
            for input_value in get_input_values(kwargs):
                if input_value in self._unbound_inputs:
                    self._unbound_inputs.remove(input_value)
            return n

    def add_workflow_input(self, input_name: str, python_type: Type) -> Interface:
        """
        Adds an input to the workflow.
        """
        if input_name in self._inputs:
            raise FlyteValidationException(f"Input {input_name} has already been specified for wf {self.name}.")
        self._python_interface = self._python_interface.with_inputs(extra_inputs={input_name: python_type})
        self._interface = transform_interface_to_typed_interface(self._python_interface)
        self._inputs[input_name] = Promise(var=input_name, val=NodeOutput(node=GLOBAL_START_NODE, var=input_name))
        self._unbound_inputs.add(self._inputs[input_name])
        return self._inputs[input_name]

    def add_workflow_output(
        self, output_name: str, p: Union[Promise, List[Promise], Dict[str, Promise]], python_type: Optional[Type] = None
    ):
        """
        Add an output with the given name from the given node output.
        """
        if output_name in self._python_interface.outputs:
            raise FlyteValidationException(f"Output {output_name} already exists in workflow {self.name}")

        if python_type is None:
            if type(p) == list or type(p) == dict:
                raise FlyteValidationException(
                    f"If specifying a list or dict of Promises, you must specify the python_type type for {output_name}"
                    f" starting with the container type (e.g. List[int]"
                )
            python_type = p.ref.node.flyte_entity.python_interface.outputs[p.var]
            logger.debug(f"Inferring python type for wf output {output_name} from Promise provided {python_type}")

        flyte_type = TypeEngine.to_literal_type(python_type=python_type)

        ctx = FlyteContext.current_context()
        if ctx.compilation_state is not None:
            raise Exception("Can't already be compiling")
        with FlyteContextManager.with_context(ctx.with_compilation_state(self.compilation_state)) as ctx:
            b = binding_from_python_std(
                ctx, output_name, expected_literal_type=flyte_type, t_value=p, t_value_type=python_type
            )
            self._output_bindings.append(b)
            self._python_interface = self._python_interface.with_outputs(extra_outputs={output_name: python_type})
            self._interface = transform_interface_to_typed_interface(self._python_interface)

    def ready(self) -> bool:
        """
        This function returns whether or not the workflow is in a ready state, which means
          * Has at least one node
          * All workflow inputs are bound

        These conditions assume that all nodes and workflow i/o changes were done with the functions above, which
        do additional checking.
        """
        if len(self.compilation_state.nodes) == 0:
            return False

        if len(self._unbound_inputs) > 0:
            return False

        return True


class PythonFunctionWorkflow(WorkflowBase, ClassStorageTaskResolver):
    """
    Please read :std:ref:`flyte:divedeep-workflows` first for a high-level understanding of what workflows are in Flyte.
    This Python object represents a workflow  defined by a function and decorated with the
    :py:func:`@workflow <flytekit.workflow>` decorator. Please see notes on that object for additional information.
    """

    def __init__(
        self,
        workflow_function: Callable,
        metadata: Optional[WorkflowMetadata],
        default_metadata: Optional[WorkflowMetadataDefaults],
    ):
        name = f"{workflow_function.__module__}.{workflow_function.__name__}"
        self._workflow_function = workflow_function
        native_interface = transform_signature_to_interface(inspect.signature(workflow_function))

        # TODO do we need this - can this not be in launchplan only?
        #    This can be in launch plan only, but is here only so that we don't have to re-evaluate. Or
        #    we can re-evaluate.
        self._input_parameters = None
        super().__init__(
            name=name,
            workflow_metadata=metadata,
            workflow_metadata_defaults=default_metadata,
            python_interface=native_interface,
        )

    @property
    def function(self):
        return self._workflow_function

    def task_name(self, t: PythonAutoContainerTask) -> str:
        return f"{self.name}.{t.__module__}.{t.name}"

    def compile(self, **kwargs):
        """
        Supply static Python native values in the kwargs if you want them to be used in the compilation. This mimics
        a 'closure' in the traditional sense of the word.
        """
        ctx = FlyteContextManager.current_context()
        self._input_parameters = transform_inputs_to_parameters(ctx, self.python_interface)
        all_nodes = []
        prefix = f"{ctx.compilation_state.prefix}-{self.short_name}-" if ctx.compilation_state is not None else ""

        with FlyteContextManager.with_context(
            ctx.with_compilation_state(CompilationState(prefix=prefix, task_resolver=self))
        ) as comp_ctx:
            # Construct the default input promise bindings, but then override with the provided inputs, if any
            input_kwargs = construct_input_promises([k for k in self.interface.inputs.keys()])
            input_kwargs.update(kwargs)
            workflow_outputs = self._workflow_function(**input_kwargs)
            all_nodes.extend(comp_ctx.compilation_state.nodes)

            # This little loop was added as part of the task resolver change. The task resolver interface itself is
            # more or less stateless (the future-proofing get_all_tasks function notwithstanding). However the
            # implementation of the TaskResolverMixin that this workflow class inherits from (ClassStorageTaskResolver)
            # does store state. This loop adds Tasks that are defined within the body of the workflow to the workflow
            # object itself.
            for n in comp_ctx.compilation_state.nodes:
                if isinstance(n.flyte_entity, PythonAutoContainerTask) and n.flyte_entity.task_resolver == self:
                    logger.debug(f"WF {self.name} saving task {n.flyte_entity.name}")
                    self.add(n.flyte_entity)

        # Iterate through the workflow outputs
        bindings = []
        output_names = list(self.interface.outputs.keys())
        # The reason the length 1 case is separate is because the one output might be a list. We don't want to
        # iterate through the list here, instead we should let the binding creation unwrap it and make a binding
        # collection/map out of it.
        if len(output_names) == 1:
            if isinstance(workflow_outputs, tuple) and len(workflow_outputs) != 1:
                raise AssertionError(
                    f"The Workflow specification indicates only one return value, received {len(workflow_outputs)}"
                )
            t = self.python_interface.outputs[output_names[0]]
            b = binding_from_python_std(
                ctx,
                output_names[0],
                self.interface.outputs[output_names[0]].type,
                workflow_outputs,
                t,
            )
            bindings.append(b)
        elif len(output_names) > 1:
            if not isinstance(workflow_outputs, tuple):
                raise AssertionError("The Workflow specification indicates multiple return values, received only one")
            if len(output_names) != len(workflow_outputs):
                raise Exception(f"Length mismatch {len(output_names)} vs {len(workflow_outputs)}")
            for i, out in enumerate(output_names):
                if isinstance(workflow_outputs[i], ConditionalSection):
                    raise AssertionError("A Conditional block (if-else) should always end with an `else_()` clause")
                t = self.python_interface.outputs[out]
                b = binding_from_python_std(
                    ctx,
                    out,
                    self.interface.outputs[out].type,
                    workflow_outputs[i],
                    t,
                )
                bindings.append(b)

        # Save all the things necessary to create an SdkWorkflow, except for the missing project and domain
        self._nodes = all_nodes
        self._output_bindings = bindings
        if not output_names:
            return None
        if len(output_names) == 1:
            return bindings[0]
        return tuple(bindings)

    def execute(self, **kwargs):
        """
        This function is here only to try to streamline the pattern between workflows and tasks. Since tasks
        call execute from dispatch_execute which is in _local_execute, workflows should also call an execute inside
        _local_execute. This makes mocking cleaner.
        """
        return self._workflow_function(**kwargs)


def workflow(
    _workflow_function=None,
    failure_policy: Optional[WorkflowFailurePolicy] = None,
    interruptible: Optional[bool] = False,
):
    """
    This decorator declares a function to be a Flyte workflow. Workflows are declarative entities that construct a DAG
    of tasks using the data flow between tasks.

    Unlike a task, the function body of a workflow is evaluated at serialization-time (aka compile-time). This is because
    while we can determine the entire structure of a task by looking at the function's signature,
    workflows need to run through the function itself because the body of the function is what expresses the workflow structure.
    It's also important to note that, local execution notwithstanding, it is not evaluated again when the workflow runs on Flyte.
    That is, workflows should not call non-Flyte entities since they are only run once (again, this is with respect to
    the platform, local runs notwithstanding).

    Please see the :std:doc:`cookbook:sphx_glr_auto_core_flyte_basics_basic_workflow.py` for more usage examples.

    :param _workflow_function: This argument is implicitly passed and represents the decorated function.
    :param failure_policy: Use the options in flytekit.WorkflowFailurePolicy
    :param interruptible: Whether or not tasks launched from this workflow are by default interruptible
    """

    def wrapper(fn):
        workflow_metadata = WorkflowMetadata(on_failure=failure_policy or WorkflowFailurePolicy.FAIL_IMMEDIATELY)

        workflow_metadata_defaults = WorkflowMetadataDefaults(interruptible)

        workflow_instance = PythonFunctionWorkflow(
            fn, metadata=workflow_metadata, default_metadata=workflow_metadata_defaults
        )
        workflow_instance.compile()
        return workflow_instance

    if _workflow_function:
        return wrapper(_workflow_function)
    else:
        return wrapper


class ReferenceWorkflow(ReferenceEntity, PythonFunctionWorkflow):
    """
    A reference workflow is a pointer to a workflow that already exists on your Flyte installation. This
    object will not initiate a network call to Admin, which is why the user is asked to provide the expected interface.
    If at registration time the interface provided causes an issue with compilation, an error will be returned.
    """

    def __init__(
        self, project: str, domain: str, name: str, version: str, inputs: Dict[str, Type], outputs: Dict[str, Type]
    ):
        super().__init__(WorkflowReference(project, domain, name, version), inputs, outputs)


def reference_workflow(
    project: str,
    domain: str,
    name: str,
    version: str,
) -> Callable[[Callable[..., Any]], ReferenceWorkflow]:
    """
    A reference workflow is a pointer to a workflow that already exists on your Flyte installation. This
    object will not initiate a network call to Admin, which is why the user is asked to provide the expected interface.
    If at registration time the interface provided causes an issue with compilation, an error will be returned.
    """

    def wrapper(fn) -> ReferenceWorkflow:
        interface = transform_signature_to_interface(inspect.signature(fn))
        return ReferenceWorkflow(project, domain, name, version, interface.inputs, interface.outputs)

    return wrapper
