from __future__ import annotations

import inspect
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

from flytekit.common import constants as _common_constants
from flytekit.common.exceptions.user import FlyteValidationException, FlyteValueException
from flytekit.core.condition import ConditionalSection
from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteEntities
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
from flytekit.core.reference_entity import ReferenceEntity, WorkflowReference
from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import logger
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model

GLOBAL_START_NODE = Node(
    id=_common_constants.GLOBAL_INPUT_NODE_ID, metadata=None, bindings=[], upstream_nodes=[], flyte_entity=None,
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


class Workflow(object):
    """
    When you assign a name to a node.

    * Any upstream node that is not assigned, recursively assign
    * When you get the call to the constructor, keep in mind there may be duplicate nodes, because they all should
      be wrapper nodes.
    """

    def __init__(
        self,
        workflow_function: Callable,
        metadata: Optional[WorkflowMetadata],
        default_metadata: Optional[WorkflowMetadataDefaults],
    ):
        self._name = f"{workflow_function.__module__}.{workflow_function.__name__}"
        self._workflow_function = workflow_function
        self._native_interface = transform_signature_to_interface(inspect.signature(workflow_function))
        self._interface = transform_interface_to_typed_interface(self._native_interface)
        # These will get populated on compile only
        self._nodes = None
        self._output_bindings: Optional[List[_literal_models.Binding]] = None
        self._workflow_metadata = metadata
        self._workflow_metadata_defaults = default_metadata

        # TODO do we need this - can this not be in launchplan only?
        #    This can be in launch plan only, but is here only so that we don't have to re-evaluate. Or
        #    we can re-evaluate.
        self._input_parameters = None
        FlyteEntities.entities.append(self)

    @property
    def function(self):
        return self._workflow_function

    @property
    def name(self) -> str:
        return self._name

    @property
    def short_name(self) -> str:
        return self._name.split(".")[-1]

    @property
    def python_interface(self) -> Interface:
        return self._native_interface

    @property
    def interface(self) -> _interface_models.TypedInterface:
        return self._interface

    @property
    def workflow_metadata(self) -> Optional[WorkflowMetadata]:
        return self._workflow_metadata

    @property
    def workflow_metadata_defaults(self):
        return self._workflow_metadata_defaults

    def compile(self, **kwargs):
        """
        Supply static Python native values in the kwargs if you want them to be used in the compilation. This mimics
        a 'closure' in the traditional sense of the word.
        """
        ctx = FlyteContext.current_context()
        self._input_parameters = transform_inputs_to_parameters(ctx, self._native_interface)
        all_nodes = []
        prefix = f"{ctx.compilation_state.prefix}-{self.short_name}-" if ctx.compilation_state is not None else None
        with ctx.new_compilation_context(prefix=prefix) as comp_ctx:
            # Construct the default input promise bindings, but then override with the provided inputs, if any
            input_kwargs = construct_input_promises([k for k in self.interface.inputs.keys()])
            input_kwargs.update(kwargs)
            workflow_outputs = self._workflow_function(**input_kwargs)
            all_nodes.extend(comp_ctx.compilation_state.nodes)

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
            t = self._native_interface.outputs[output_names[0]]
            b = binding_from_python_std(
                ctx, output_names[0], self.interface.outputs[output_names[0]].type, workflow_outputs, t,
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
                t = self._native_interface.outputs[out]
                b = binding_from_python_std(ctx, out, self.interface.outputs[out].type, workflow_outputs[i], t,)
                bindings.append(b)

        # Save all the things necessary to create an SdkWorkflow, except for the missing project and domain
        self._nodes = all_nodes
        self._output_bindings = bindings
        if not output_names:
            return None
        if len(output_names) == 1:
            return bindings[0]
        return tuple(bindings)

    def _local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise]:
        """
        Performs local execution of a workflow. kwargs are expected to be Promises for the most part (unless,
        someone has hardcoded in my_wf(input_1=5) or something).
        :param ctx: The FlyteContext
        :param kwargs: parameters for the workflow itself
        """
        logger.info(f"Executing Workflow {self._name}, ctx{ctx.execution_state.Mode}")

        # This is done to support the invariant that Workflow local executions always work with Promise objects
        # holding Flyte literal values. Even in a wf, a user can call a sub-workflow with a Python native value.
        for k, v in kwargs.items():
            if not isinstance(v, Promise):
                t = self._native_interface.inputs[k]
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

        # Because we should've already returned in the above check, we just raise an Exception here.
        if len(self.python_interface.outputs) == 0:
            raise FlyteValueException(
                function_outputs, f"{function_outputs} received but should've been VoidPromise or None."
            )

        expected_output_names = list(self.interface.outputs.keys())
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

        new_promises = [Promise(var, wf_outputs_as_literal_dict[var]) for var in expected_output_names]

        # TODO: With the native interface, create_task_output should be able to derive the typed interface, and it
        #   should be able to do the conversion of the output of the execute() call directly.
        return create_task_output(new_promises, self._native_interface)

    def execute(self, **kwargs):
        """
        This function is here only to try to streamline the pattern between workflows and tasks. Since tasks
        call execute from dispatch_execute which is in _local_execute, workflows should also call an execute inside
        _local_execute. This makes mocking cleaner.
        """
        return self._workflow_function(**kwargs)

    def __call__(self, *args, **kwargs):
        if len(args) > 0:
            raise AssertionError("Only Keyword Arguments are supported for Workflow executions")

        ctx = FlyteContext.current_context()

        # Handle subworkflows in compilation
        if ctx.compilation_state is not None:
            input_kwargs = self._native_interface.default_inputs_as_kwargs
            input_kwargs.update(kwargs)
            return create_and_link_node(ctx, entity=self, interface=self._native_interface, **input_kwargs)
        elif (
            ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION
        ):
            # We are already in a local execution, just continue the execution context
            return self._local_execute(ctx, **kwargs)

        # When someone wants to run the workflow function locally. Assume that the inputs given are given as Python
        # native values. _local_execute will always translate Python native literals to Flyte literals, so no worries
        # there, but it'll return Promise objects.
        else:
            # Run some sanity checks
            # Even though the _local_execute call generally expects inputs to be Promises, we don't have to do the
            # conversion here in this loop. The reason is because we don't prevent users from specifying inputs
            # as direct scalars, which means there's another Promise-generating loop inside _local_execute too
            for k, v in kwargs.items():
                if k not in self.interface.inputs:
                    raise ValueError(f"Received unexpected keyword argument {k}")
                if isinstance(v, Promise):
                    raise ValueError(f"Received a promise for a workflow call, when expecting a native value for {k}")

            with ctx.new_execution_context(mode=ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION) as ctx:
                result = self._local_execute(ctx, **kwargs)

            expected_outputs = len(self._native_interface.outputs)
            if expected_outputs == 0:
                if result is None or isinstance(result, VoidPromise):
                    return None
                else:
                    raise Exception(f"Workflow local execution expected 0 outputs but something received {result}")

            if (expected_outputs > 1 and len(result) == expected_outputs) or (
                expected_outputs == 1 and result is not None
            ):
                if isinstance(result, Promise):
                    v = [v for k, v in self._native_interface.outputs.items()][0]
                    return TypeEngine.to_python_value(ctx, result.val, v)
                else:
                    for prom in result:
                        if not isinstance(prom, Promise):
                            raise Exception("should be promises")
                        native_list = [
                            TypeEngine.to_python_value(ctx, promise.val, self._native_interface.outputs[promise.var])
                            for promise in result
                        ]
                        return tuple(native_list)

            raise ValueError("expected outputs and actual outputs do not match")


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

    Please see the :std:doc:`auto_core_basic/basic_workflow` for more usage examples.

    :param _workflow_function: This argument is implicitly passed and represents the decorated function.
    :param failure_policy: Use the options in flytekit.WorkflowFailurePolicy
    :param interruptible: Whether or not tasks launched from this workflow are by default interruptible
    """

    def wrapper(fn):
        workflow_metadata = WorkflowMetadata(on_failure=failure_policy or WorkflowFailurePolicy.FAIL_IMMEDIATELY)

        workflow_metadata_defaults = WorkflowMetadataDefaults(interruptible)

        workflow_instance = Workflow(fn, metadata=workflow_metadata, default_metadata=workflow_metadata_defaults)
        workflow_instance.compile()
        return workflow_instance

    if _workflow_function:
        return wrapper(_workflow_function)
    else:
        return wrapper


class ReferenceWorkflow(ReferenceEntity, Workflow):
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
    project: str, domain: str, name: str, version: str,
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
