import collections
import datetime as _datetime
import inspect
import re
from abc import abstractmethod
from typing import Callable, Union, Dict, DefaultDict, Type, Any, List, Tuple

from flytekit import engine as flytekit_engine, logger
from flytekit.annotated.context_manager import ExecutionState, FlyteContext
from flytekit.annotated.interface import Interface, transform_interface_to_typed_interface, \
    transform_signature_to_interface
from flytekit.annotated.promise import Promise, create_task_output
from flytekit.common import nodes as _nodes, interface as _common_interface
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.promise import NodeOutput as _NodeOutput
from flytekit.models import task as _task_model, literals as _literal_models
from flytekit.models.core import workflow as _workflow_model, identifier as _identifier_model


# This is the least abstract task. It will have access to the loaded Python function
# itself if run locally, so it will always be a Python task.
# This is analogous to the current SdkRunnableTask. Need to analyze the benefits of duplicating the class versus
# adding to it. Also thinking that the relationship to SdkTask should be a has-one relationship rather than an is-one.
# I'm not attached to this class at all, it's just here as a stand-in. Everything in this PR is subject to change.
#
# I think the class layers are IDL -> Model class -> SdkBlah class. While the model and generated-IDL classes
# obviously encapsulate the IDL, the SdkTask/Workflow/Launchplan/Node classes should encapsulate the control plane.
# That is, all the control plane interactions we wish to build should belong there. (I think this is how it's done
# already.)
class Task(object):

    def __init__(self, name: str, interface: _common_interface.TypedInterface, metadata: _task_model.TaskMetadata,
                 *args, **kwargs):
        self._name = name
        self._interface = interface
        self._metadata = metadata

    def _compile(self, ctx: FlyteContext, *args, **kwargs):
        """
        This method is used to compile a task and generate nodes with bindings. This is not used in the execution path
        """
        used_inputs = set()
        bindings = []

        for k in sorted(self.interface.inputs):
            var = self.interface.inputs[k]
            if k not in kwargs:
                raise _user_exceptions.FlyteAssertion(
                    "Input was not specified for: {} of type {}".format(k, var.type)
                )
            bindings.append(flytekit_engine.binding_from_python_std(ctx, k, var.type, kwargs[k]))
            used_inputs.add(k)

        extra_inputs = used_inputs ^ set(kwargs.keys())
        if len(extra_inputs) > 0:
            raise _user_exceptions.FlyteAssertion(
                "Too many inputs were specified for the interface.  Extra inputs were: {}".format(extra_inputs)
            )

        # Detect upstream nodes
        upstream_nodes = [input_val.sdk_node for input_val in kwargs.values() if isinstance(input_val, _NodeOutput)]

        # TODO: Make the metadata name the full name of the (function)?
        sdk_node = _nodes.SdkNode(
            # TODO
            id=f"node-{len(ctx.compilation_state.nodes)}",
            metadata=_workflow_model.NodeMetadata(self._name, self.metadata.timeout,
                                                  self.metadata.retries,
                                                  self.metadata.interruptible),
            bindings=sorted(bindings, key=lambda b: b.var),
            upstream_nodes=upstream_nodes,
            sdk_task=self
        )
        ctx.compilation_state.nodes.append(sdk_node)

        # Create a node output object for each output, they should all point to this node of course.
        # TODO: Again, we need to be sure that we end up iterating through the output names in the correct order
        #  investigate this and document here.
        node_outputs = []
        for output_name, output_var_model in self.interface.outputs.items():
            # TODO: If node id gets updated later, we have to make sure to update the NodeOutput model's ID, which
            #  is currently just a static str
            node_outputs.append(Promise(output_name, _NodeOutput(sdk_node=sdk_node, sdk_type=None, var=output_name)))
        # Don't print this, it'll crash cuz sdk_node._upstream_node_ids might be None, but idl code will break

        return create_task_output(node_outputs)

    def _local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, None]:
        """
        This code is used only in the case when we want to dispatch_execute with outputs from a previous node
        For regular execution, dispatch_execute is invoked directly.
        """
        # Unwrap the kwargs values. After this, we essentially have a LiteralMap
        for k, v in kwargs.items():
            if isinstance(v, Promise):
                if not v.is_ready:
                    raise BrokenPipeError(
                        f"Expected an actual value from the previous step, but received an incomplete promise {v}")
                kwargs[k] = v.val

        input_literal_map = _literal_models.LiteralMap(literals=kwargs)

        outputs_literal_map = self.dispatch_execute(ctx, input_literal_map)
        outputs_literals = outputs_literal_map.literals

        # TODO maybe this is the part that should be done for local execution, we pass the outputs to some special
        #    location, otherwise we dont really need to right? The higher level execute could just handle literalMap
        # After running, we again have to wrap the outputs, if any, back into NodeOutput objects
        output_names = list(self.interface.outputs.keys())
        node_results = []
        if len(output_names) != len(outputs_literals):
            # Length check, clean up exception
            raise Exception(f"Length difference {len(output_names)} {len(outputs_literals)}")

        vals = [Promise(var, outputs_literals[var]) for var in output_names]
        return create_task_output(vals)

    def dispatch_execute(
            self, ctx: FlyteContext, input_literal_map: _literal_models.LiteralMap) -> _literal_models.LiteralMap:
        """
        This method translates Flytes native Type system based inputs and dispatches the actual call to the executor
        NOTE: This method is also invoked during runtime
        """
        # Translate the input literals to Python native
        native_inputs = flytekit_engine.idl_literal_map_to_python_value(ctx, input_literal_map)

        native_outputs = self.execute(**native_inputs)

        expected_output_names = list(self.interface.outputs.keys())
        if len(expected_output_names) == 1:
            native_outputs_as_map = {expected_output_names[0]: native_outputs}
        else:
            # Question: How do you know you're going to enumerate them in the correct order? Even if autonamed, will
            # output2 come before output100 if there's a hundred outputs? We don't! We'll have to circle back to
            # the Python task instance and inspect annotations again. Or we change the Python model representation
            # of the interface to be an ordered dict and we fill it in correctly to begin with.
            native_outputs_as_map = {expected_output_names[i]: native_outputs[i] for i, _ in
                                     enumerate(native_outputs)}

        # We manually construct a LiteralMap here because task inputs and outputs actually violate the assumption
        # built into the IDL that all the values of a literal map are of the same type.
        outputs_literal_map = _literal_models.LiteralMap(literals={
            k: flytekit_engine.python_value_to_idl_literal(ctx, v, self.interface.outputs[k].type) for k, v in
            native_outputs_as_map.items()
        })
        print("Outputs!")
        print(outputs_literal_map)
        return outputs_literal_map

    @abstractmethod
    def execute(self, **kwargs) -> Any:
        pass

    def __call__(self, *args, **kwargs):
        # When a Task is () aka __called__, there are three things we may do:
        #  a. Task Execution Mode - just run the Python function as Python normally would. Flyte steps completely
        #     out of the way.
        #  b. Compilation Mode - this happens when the function is called as part of a workflow (potentially
        #     dynamic task?). Instead of running the user function, produce promise objects and create a node.
        #  c. Workflow Execution Mode - when a workflow is being run locally. Even though workflows are functions
        #     and everything should be able to be passed through naturally, we'll want to wrap output values of the
        #     function into objects, so that potential .with_cpu or other ancillary functions can be attached to do
        #     nothing. Subsequent tasks will have to know how to unwrap these. If by chance a non-Flyte task uses a
        #     task output as an input, things probably will fail pretty obviously.
        if len(args) > 0:
            raise _user_exceptions.FlyteAssertion(
                f"In Flyte workflows, on keyword args are supported to pass inputs to workflows and tasks."
                f"Aborting execution as detected {len(args)} positional args {args}"
            )

        ctx = FlyteContext.current_context()
        if ctx.compilation_state is not None and ctx.compilation_state.mode == 1:
            return self._compile(ctx, *args, **kwargs)
        elif ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            return self._local_execute(ctx, **kwargs)
        else:
            logger.warning("task run without context - executing raw function")
            return self.execute(**kwargs)

    @property
    def interface(self) -> _common_interface.TypedInterface:
        return self._interface

    @property
    def metadata(self) -> _task_model.TaskMetadata:
        return self._metadata

    @property
    def name(self) -> str:
        return self._name


class PythonFunctionTask(Task):

    def __init__(self, task_function: Callable, metadata: _task_model.TaskMetadata, ignore_input_vars: List[str] = None,
                 *args, **kwargs):
        self._native_interface = transform_signature_to_interface(inspect.signature(task_function))
        mutated_interface = self._native_interface.remove_inputs(ignore_input_vars)
        interface = transform_interface_to_typed_interface(mutated_interface)
        super().__init__(name=task_function.__name__, interface=interface, metadata=metadata, *args, **kwargs)
        self._task_function = task_function

    def execute(self, **kwargs) -> Any:
        return self._task_function(**kwargs)

    def native_interface(self) -> Interface:
        return self._native_interface


class PysparkFunctionTask(PythonFunctionTask):
    def __init__(self, task_function: Callable, metadata: _task_model.TaskMetadata, *args, **kwargs):
        super(PysparkFunctionTask, self).__init__(task_function, metadata,
                                                  ignore_input_vars=["spark_session", "spark_context"], *args, **kwargs)

    def execute(self, **kwargs) -> Any:
        if "spark_session" in self.native_interface().inputs:
            kwargs["spark_session"] = None
        if "spark_context" in self.native_interface().inputs:
            kwargs["spark_context"] = None
        return self._task_function(**kwargs)


class AbstractSQLTask(Task):
    """
    Base task types for all SQL tasks
    """

    # TODO this should be replaced with Schema Type
    _OUTPUTS = {"results": str}
    _INPUT_REGEX = re.compile(r'({{\s*.inputs.(\w+)\s*}})', re.IGNORECASE)

    def __init__(self, name: str, query_template: str, inputs: Dict[str, Type], metadata: _task_model.TaskMetadata,
                 *args,
                 **kwargs):
        _interface = transform_interface_to_typed_interface(Interface(inputs=inputs, outputs=self._OUTPUTS))
        super().__init__(name=name, interface=_interface, metadata=metadata, *args, **kwargs)
        self._query_template = query_template

    @property
    def query_template(self) -> str:
        return self._query_template

    def execute(self, **kwargs) -> Any:
        modified_query = self._query_template
        matched = set()
        for match in self._INPUT_REGEX.finditer(self._query_template):
            expr = match.groups()[0]
            var = match.groups()[1]
            if var not in kwargs:
                raise ValueError(
                    f"Variable {var} in Query (part of {expr}) not found in inputs {kwargs.keys()}")
            matched.add(var)
            val = kwargs[var]
            # str conversion should be deliberate, with right conversion for each type
            modified_query = modified_query.replace(expr, str(val))

        if len(matched) < len(kwargs.keys()):
            diff = set(kwargs.keys()).difference(matched)
            raise ValueError(f"Extra Inputs have not matches in query template - missing {diff}")

        print(f"Evaluated Query\n {modified_query}")
        return None


TaskTypePlugins: DefaultDict[str, Type[PythonFunctionTask]] = collections.defaultdict(
    lambda: PythonFunctionTask,
    {
        "python_task": PythonFunctionTask,
        "task": PythonFunctionTask,
        "spark": PysparkFunctionTask,
    }
)


class Resources(object):
    def __init__(self, cpu=None, mem=None, gpu=None, storage=None):
        self._cpu = cpu
        self._mem = mem
        self._gpu = gpu
        self._storage = storage


def metadata(
        cache: bool = False,
        cache_version: str = "",
        retries: int = 0,
        interruptible: bool = False,
        deprecated: str = "",
        timeout: Union[_datetime.timedelta, int] = None) -> _task_model.TaskMetadata:
    return _task_model.TaskMetadata(
        discoverable=cache,
        runtime=_task_model.RuntimeMetadata(
            _task_model.RuntimeMetadata.RuntimeType.FLYTE_SDK,
            '1.2.3',
            'python'
        ),
        timeout=timeout,
        retries=_literal_models.RetryStrategy(retries),
        interruptible=interruptible,
        discovery_version=cache_version,
        deprecated_error_message=deprecated,
    )


def task(
        _task_function: Callable = None,
        task_type: str = "",
        cache: bool = False,
        cache_version: str = "",
        retries: int = 0,
        interruptible: bool = False,
        deprecated: str = "",
        timeout: Union[_datetime.timedelta, int] = None,
        environment: Dict[str, str] = None,
        *args, **kwargs) -> Callable:
    def wrapper(fn) -> PythonFunctionTask:
        _timeout = timeout
        if _timeout and not isinstance(_timeout, _datetime.timedelta):
            if isinstance(_timeout, int):
                _timeout = _datetime.timedelta(seconds=_timeout)
            else:
                raise ValueError("timeout should be duration represented as either a datetime.timedelta or int seconds")

        _metadata = metadata(cache, cache_version, retries, interruptible, deprecated, timeout)

        task_instance = TaskTypePlugins[task_type](fn, _metadata, *args, **kwargs)
        # TODO: One of the things I want to make sure to do is better naming support. At this point, we should already
        #       be able to determine the name of the task right? Can anyone think of situations where we can't?
        #       Where does the current instance tracker come into play?
        task_instance.id = _identifier_model.Identifier(_identifier_model.ResourceType.TASK, "proj", "dom", "blah", "1")

        return task_instance

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper
