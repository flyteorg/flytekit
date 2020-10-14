import inspect
from typing import Dict, Any, Callable

from flytekit import engine as flytekit_engine, logger
from flytekit.annotated.condition import ConditionalSection
from flytekit.annotated.context_manager import FlyteContext, ExecutionState
from flytekit.annotated.interface import transform_signature_to_interface, transform_interface_to_typed_interface, \
    transform_inputs_to_parameters
from flytekit.annotated.promise import Promise
from flytekit.common import constants as _common_constants
from flytekit.common.workflow import SdkWorkflow as _SdkWorkflow
from flytekit.models import literals as _literal_models, types as _type_models
from flytekit.models.core import identifier as _identifier_model


class Workflow(object):
    """
    When you assign a name to a node.
    * Any upstream node that is not assigned, recursively assign
    * When you get the call to the constructor, keep in mind there may be duplicate nodes, because they all should
      be wrapper nodes.
    """

    def __init__(self, workflow_function: Callable):
        self._name = workflow_function.__name__
        self._workflow_function = workflow_function
        self._native_interface = transform_signature_to_interface(inspect.signature(workflow_function))
        self._interface = transform_interface_to_typed_interface(self._native_interface)
        # This will get populated on compile only
        self._sdk_workflow = None
        # TODO do we need this - can this not be in launchplan only?
        #    This can be in launch plan only, but is here only so that we don't have to re-evaluate. Or
        #    we can re-evaluate.
        self._input_parameters = None

    @property
    def function(self):
        return self._workflow_function

    @property
    def name(self):
        return self._name

    @property
    def interface(self):
        return self._interface

    def compile(self):
        # TODO should we even define it here?
        self._input_parameters = transform_inputs_to_parameters(self._native_interface)
        all_nodes = []
        ctx = FlyteContext.current_context()
        with ctx.new_compilation_context() as comp_ctx:
            # Fill in call args by constructing input bindings
            input_kwargs = {
                k: _type_models.OutputReference(_common_constants.GLOBAL_INPUT_NODE_ID, k)
                for k in self._native_interface.inputs.keys()
            }
            workflow_outputs = self._workflow_function(**input_kwargs)
            all_nodes.extend(comp_ctx.compilation_state.nodes)

        # Iterate through the workflow outputs
        #  Get the outputs and use them to construct the old Output objects
        #    promise.NodeOutputs (let's just focus on this one first for POC)
        #    or Input objects from above in the case of a passthrough value
        #    or outputs can be like 5, or 'hi'
        # These should line up with the output input argument
        # TODO: Add length checks.
        bindings = []
        output_names = list(self._native_interface.outputs.keys())
        if len(output_names) > 0:
            for i, out in enumerate(workflow_outputs):
                output_name = output_names[i]
                # TODO: Check that the outputs returned type match the interface.
                # output_literal_type = out.literal_type
                # logger.debug(f"Got output wrapper: {out}")
                # logger.debug(f"Var name {output_name} wf output name {outputs[i]} type: {output_literal_type}")
                binding_data = _literal_models.BindingData(promise=out)
                bindings.append(_literal_models.Binding(var=output_name, binding=binding_data))

        # TODO: Again, at this point, we should be able to identify the name of the workflow
        workflow_id = _identifier_model.Identifier(_identifier_model.ResourceType.WORKFLOW,
                                                   "proj", "dom", "moreblah", "1")

        # Create a FlyteWorkflow object. We call this like how promote_from_model would call this, by ignoring the
        # fancy arguments and supplying just the raw elements manually. Alternatively we can construct the
        # WorkflowTemplate object, and then call promote_from_model.
        self._sdk_workflow = _SdkWorkflow(inputs=None, outputs=None, nodes=all_nodes, id=workflow_id, metadata=None,
                                          metadata_defaults=None, interface=self._interface, output_bindings=bindings)
        if not output_names:
            return None
        if len(output_names) == 1:
            return bindings[0]
        return tuple(bindings)

    def _local_execute(self, ctx: FlyteContext, nested=False, **kwargs) -> Dict[str, Any]:
        """
        Performs local execution of a workflow
        :param ctx: The FlyteContext
        :param nested: boolean that indicates if this is a nested workflow execution (subworkflow)
        :param kwargs: parameters for the workflow itself
        """
        logger.info(f"Executing Workflow {self._name} with nested={nested}, ctx{ctx.execution_state.Mode}")
        # NOTE: All inputs received by Workflow in this mode should be Promises.
        # TODO, how will this work for dynamic workflow? Ideally dynamic workflow should use dispatch_execute
        for k, v in kwargs.items():
            if isinstance(v, Promise):
                if not v.is_ready:
                    raise BrokenPipeError(
                        f"Expected an actual value from the previous step, but received an incomplete promise {v}")

        # TODO: These are all assumed to be TaskCallOutput objects, but they can
        #   be other things as well.  What if someone just returns 5? Should we disallow this?
        function_outputs = self._workflow_function(**kwargs)

        if nested:
            # If this is a subworkflow it should return promises upstream - to parent workflow
            logger.info(f"Executing subworkfow returning promises")
            return function_outputs

        output_names = list(self._interface.outputs.keys())
        output_literal_map = {}
        # TODO Ketan fix this make it into a simple promise transformation
        if len(output_names) > 1:
            for idx, var_name in enumerate(output_names):
                op = function_outputs[idx]
                if isinstance(op, Promise):
                    output_literal_map[var_name] = op.val
                elif isinstance(op, ConditionalSection):
                    raise AssertionError("A Conditional block (if-else) should always end with an `else_()` clause")
                else:
                    raise AssertionError("Illegal workflow construction detected")
        elif len(output_names) == 1:
            output_literal_map[output_names[0]] = function_outputs.val
        else:
            return None

        return flytekit_engine.idl_literal_map_to_python_value(ctx, _literal_models.LiteralMap(
            literals=output_literal_map))

    def __call__(self, *args, **kwargs):

        if len(args) > 0:
            raise Exception('not allowed')

        ctx = FlyteContext.current_context()
        # Reserved for when we have subworkflows
        if ctx.compilation_state is not None:
            return self.compile()
        elif ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            print(f"{self._name} in local execution mode")
            # We are already in a local execution, just continue the execution context
            return self._local_execute(ctx, nested=True, **kwargs)
        else:
            # When someone wants to run the workflow function locally
            with ctx.new_execution_context(mode=ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION) as ctx:
                for k, v in kwargs.items():
                    if k not in self._interface.inputs:
                        # Should we do this in strict mode?
                        raise ValueError(f"Received unexpected keyword argument {k}")
                    if isinstance(v, Promise):
                        raise ValueError(
                            f"Received a promise for a workflow call, when expecting a native value for {k}")
                    kwargs[k] = Promise(
                        var=k, val=flytekit_engine.python_value_to_idl_literal(ctx, v, self._interface.inputs[k].type))
                return self._local_execute(ctx, **kwargs)


def workflow(_workflow_function=None):
    # Unlike for tasks, where we can determine the entire structure of the task by looking at the function's signature,
    # workflows need to have the body of the function itself run at module-load time. This is because the body of the
    # workflow is what expresses the workflow structure.
    def wrapper(fn):
        # TODO: Again, at this point, we should be able to identify the name of the workflow
        workflow_id = _identifier_model.Identifier(_identifier_model.ResourceType.WORKFLOW,
                                                   "proj", "dom", "moreblah", "1")
        workflow_instance = Workflow(fn)
        workflow_instance.compile()
        workflow_instance.id = workflow_id

        return workflow_instance

    if _workflow_function:
        return wrapper(_workflow_function)
    else:
        return wrapper
