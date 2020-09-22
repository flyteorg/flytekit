import datetime as _datetime
import inspect
from typing import List, Dict, Tuple, NamedTuple, Generator
from collections import OrderedDict

from flytekit import logger
from flytekit.common import (
    nodes as _nodes,
    constants as _common_constants,
    interface as _common_interface,
)
from flytekit.common.nodes import OutputParameterMapper
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.promise import Input as _WorkflowInput, NodeOutput as _NodeOutput
from flytekit.common.types import primitives as _primitives, helpers as _type_helpers
from flytekit.common.workflow import Output as _WorkflowOutput, SdkWorkflow as _SdkWorkflow
from flytekit.configuration.common import CONFIGURATION_SINGLETON
from flytekit.models import interface as _interface_models, literals as _literal_models
from flytekit.models import task as _task_model, types as _type_models
from flytekit.models.core import workflow as _workflow_model, identifier as _identifier_model
from flytekit.annotated.type_engine import BASE_TYPES
from flytekit.annotated import type_engine
from flytekit import engine as flytekit_engine
from flytekit.annotated.context_manager import FlyteContext


# Set this to 11 or higher if you don't want to see debug output
logger.setLevel(10)


def get_default_args(func):
    """
    Returns the default arguments to a function as a dict. Will be empty if there are none.
    """
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }


# Please see the comments on the PythonTask class for additional context. This object is just here as a stand-in?
class PythonWorkflow(object):
    """
    When you assign a name to a node.
    * Any upstream node that is not assigned, recursively assign
    * When you get the call to the constructor, keep in mind there may be duplicate nodes, because they all should
      be wrapper nodes.
    """
    def __init__(self, workflow_function):
        self._workflow_function = workflow_function

    def __call__(self, *args, **kwargs):
        if CONFIGURATION_SINGLETON.x == 1:
            if len(args) > 0:
                raise Exception('not allowed')

            print(f"compilation mode. Args are: {args}")
            print(f"Locals: {locals()}")

            # logger.debug(f"Compiling workflow... ")

        else:
            # Can we relax this in the future?  People are used to this right now, so it's okay.
            if len(args) > 0:
                raise Exception('When using the workflow decorator, all inputs must be specified with kwargs only')

            return self._workflow_function(*args, **kwargs)


def workflow(
        _workflow_function=None,
        outputs: List[str]=None
):
    # Unlike for tasks, where we can determine the entire structure of the task by looking at the function's signature,
    # workflows need to have the body of the function itself run at module-load time. This is because the body of the
    # workflow is what expresses the workflow structure.
    def wrapper(fn):
        workflow_annotations = fn.__annotations__
        inputs = {k: v for k, v in workflow_annotations.items() if k != 'return'}
        inputs_variable_map = get_variable_map(inputs)
        outputs_variable_map = get_output_variable_map(workflow_annotations)
        interface_model = _interface_models.TypedInterface(inputs=inputs_variable_map, outputs=outputs_variable_map)

        # Create promises out of all the inputs. Check for defaults in the function definition.
        default_inputs = get_default_args(fn)
        input_parameter_models = []
        for input_name, input_variable_obj in inputs_variable_map.items():
            # TODO: Fix defaults and required
            parameter_model = _interface_models.Parameter(var=input_variable_obj, default=None, required=True)
            input_parameter_models.append(parameter_model)

        all_nodes = []
        ctx = FlyteContext.current_context()
        with ctx.new_compilation_state() as comp_ctx:
            # Fill in call args
            workflow_outputs = fn()
            all_nodes.extend(comp_ctx.compilation_state.nodes)

        # Iterate through the workflow outputs
        #  Get the outputs and use them to construct the old Output objects
        #    promise.NodeOutputs (let's just focus on this one first for POC)
        #    or Input objects from above in the case of a passthrough value
        #    or outputs can be like 5, or 'hi'

        # These should line up with the output input argument
        # TODO: Add length checks.

        # logger.debug(f"Workflow outputs: {outputs}")

        bindings = []
        output_names = outputs_variable_map.keys()
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
        # logger.debug(f"Inputs {input_parameters}")
        # logger.debug(f"Output objects {workflow_output_objs}")
        # logger.debug(f"Nodes {all_nodes}")

        # Create a FlyteWorkflow object. We call this like how promote_from_model would call this, by ignoring the
        # fancy arguments and supplying just the raw elements manually. Alternatively we can construct the
        # WorkflowTemplate object, and then call promote_from_model.
        sdk_workflow = _SdkWorkflow(inputs=None, outputs=None, nodes=all_nodes, id=workflow_id, metadata=None,
                                    metadata_defaults=None, interface=interface_model, output_bindings=bindings)
        logger.debug(f"SdkWorkflow {sdk_workflow}")

        workflow_instance = PythonWorkflow(fn)
        workflow_instance.id = workflow_id

        return workflow_instance

    if _workflow_function:
        return wrapper(_workflow_function)
    else:
        return wrapper


# Has Python in the name because this is the least abstract task. It will have access to the loaded Python function
# itself if run locally, so it will always be a Python task.
# This is analogous to the current SdkRunnableTask. Need to analyze the benefits of duplicating the class versus
# adding to it. Also thinking that the relationship to SdkTask should be a has-one relationship rather than an is-one.
# I'm not attached to this class at all, it's just here as a stand-in. Everything in this PR is subject to change.
#
# I think the class layers are IDL -> Model class -> SdkBlah class. While the model and generated-IDL classes
# obviously encapsulate the IDL, the SdkTask/Workflow/Launchplan/Node classes should encapsulate the control plane.
# That is, all the control plane interactions we wish to build should belong there. (I think this is how it's done
# already.)
class PythonTask(object):
    task_type = _common_constants.SdkTaskType.PYTHON_TASK

    def __init__(self, task_function, interface, metadata: _task_model.TaskMetadata, info):
        self._task_function = task_function
        self._interface = interface
        self._metadata = metadata
        self._info = info

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
        ctx = FlyteContext.current_context()

        if ctx.compilation_state is not None and ctx.compilation_state.mode == 1:
            if len(args) > 0:
                raise _user_exceptions.FlyteAssertion(
                    "When adding a task as a node in a workflow, all inputs must be specified with kwargs only.  We "
                    "detected {} positional args.".format(len(args))
                )

            used_inputs = set()
            bindings = []

            for k in sorted(self.interface.inputs):
                var = self.interface.inputs[k]
                if k not in kwargs:
                    raise _user_exceptions.FlyteAssertion(
                        "Input was not specified for: {} of type {}".format(k, var.type)
                    )
                bindings.append(flytekit_engine.binding_from_python_std(k, var.type, kwargs[k]))
                used_inputs.add(k)

            extra_inputs = used_inputs ^ set(kwargs.keys())
            if len(extra_inputs) > 0:
                raise _user_exceptions.FlyteAssertion(
                    "Too many inputs were specified for the interface.  Extra inputs were: {}".format(extra_inputs)
                )

            # Detect upstream nodes
            # TODO: This becomes more complicated if dealing with lists/dicts that contain NodeOutputs
            upstream_nodes = [input_val.sdk_node for input_val in kwargs.values() if isinstance(input_val, _NodeOutput)]

            # TODO: Make the metadata name the full name of the (function)?
            # There should be no reason to ever assign a random node id (at least in a non-dynamic context), so we
            # leave it empty for now.
            sdk_node = _nodes.SdkNode(
                # TODO
                id=f"node-{len(ctx.compilation_state.nodes)}",
                metadata=_workflow_model.NodeMetadata(self._task_function.__name__, self.metadata.timeout, self.metadata.retries,
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
                node_outputs.append(_NodeOutput(sdk_node=sdk_node, sdk_type=None, var=output_name,
                                                literal_type=output_var_model.type))
            # Don't print this, it'll crash cuz sdk_node._upstream_node_ids might be None, but idl code will break

            if len(self.interface.outputs) > 1:
                return tuple(node_outputs)
            else:
                return node_outputs[0]

        else:
            return self._task_function(*args, **kwargs)

    @property
    def interface(self) -> _common_interface.TypedInterface:
        return self._interface

    @property
    def metadata(self) -> _task_model.TaskMetadata:
        return self._metadata


def task(
        _task_function=None,
        cache_version='',
        retries=0,
        interruptible=None,
        deprecated='',
        storage_request=None,
        cpu_request=None,
        gpu_request=None,
        memory_request=None,
        storage_limit=None,
        cpu_limit=None,
        gpu_limit=None,
        memory_limit=None,
        cache=False,
        timeout=None,
        environment=None,
):
    def wrapper(fn):
        # Just saving everything as a hash for now, Ketan will figure out what to do with this in the future
        task_obj = {}
        task_obj['task_type'] = _common_constants.SdkTaskType.PYTHON_TASK,
        task_obj['retries'] = retries,
        task_obj['storage_request'] = storage_request,
        task_obj['cpu_request'] = cpu_request,
        task_obj['gpu_request'] = gpu_request,
        task_obj['memory_request'] = memory_request,
        task_obj['storage_limit'] = storage_limit,
        task_obj['cpu_limit'] = cpu_limit,
        task_obj['gpu_limit'] = gpu_limit,
        task_obj['memory_limit'] = memory_limit,
        task_obj['environment'] = environment,
        task_obj['custom'] = {}

        metadata = _task_model.TaskMetadata(
            cache,
            _task_model.RuntimeMetadata(
                _task_model.RuntimeMetadata.RuntimeType.FLYTE_SDK,
                '1.2.3',
                'python'
            ),
            timeout or _datetime.timedelta(seconds=0),
            _literal_models.RetryStrategy(retries),
            interruptible,
            cache_version,
            deprecated
        )

        interface = get_interface_from_task_info(fn.__annotations__)

        task_instance = PythonTask(fn, interface, metadata, task_obj)
        # TODO: One of the things I want to make sure to do is better naming support. At this point, we should already
        #       be able to determine the name of the task right? Can anyone think of situations where we can't?
        #       Where does the current instance tracker come into play?
        task_instance.id = _identifier_model.Identifier(_identifier_model.ResourceType.TASK, "proj", "dom", "blah", "1")

        return task_instance

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def output_name_generator(length: int) -> Generator[str, None, None]:
    for x in range(0, length):
        yield f"out_{x}"


def get_output_variable_map(task_annotations: Dict[str, type]) -> Dict[str, _interface_models.Variable]:
    """
    Outputs can have various signatures, and we need to handle all of them:

        # Option 1
        nt1 = typing.NamedTuple("NT1", x_str=str, y_int=int)
        def t(a: int, b: str) -> nt1: ...

        # Option 2
        def t(a: int, b: str) -> typing.NamedTuple("NT1", x_str=str, y_int=int): ...

        # Option 3
        def t(a: int, b: str) -> typing.Tuple[int, str]: ...

        # Option 4
        def t(a: int, b: str) -> (int, str): ...

        # Option 5
        def t(a: int, b: str) -> str: ...

    TODO: We'll need to check the actual return types for in all cases as well, to make sure Flyte IDL actually
          supports it. For instance, typing.Tuple[Optional[int]] is not something we can represent currently.

    TODO: Generator[A,B,C] types are also valid, indicating dynamic tasks. Will need to implement.

    Note that Options 1 and 2 are identical, just syntactic sugar. In the NamedTuple case, we'll use the names in the
    definition. In all other cases, we'll automatically generate output names, indexed starting at 0.

    :param task_annotations: the __annotations__ attribute of a type hinted function.
    """
    if "return" in task_annotations:
        incoming_rt = task_annotations['return']

        # Handle options 1 and 2 first. The only way to check if the return type is a typing.NamedTuple, is to check
        # for this field. Using isinstance or issubclass doesn't work.
        if hasattr(incoming_rt, '_field_types'):
            logger.debug(f'Task returns named tuple {incoming_rt}')
            return_map = incoming_rt._field_types

        # Handle option 3
        elif hasattr(incoming_rt, '__origin__') and incoming_rt.__origin__ is tuple:
            logger.debug(f'Task returns unnamed typing.Tuple {incoming_rt}')
            return_types = incoming_rt.__args__
            return_names = [x for x in output_name_generator(len(incoming_rt.__args__))]
            return_map = OrderedDict(zip(return_names, return_types))

        # Handle option 4
        elif type(incoming_rt) is tuple:
            logger.debug(f'Task returns unnamed native tuple {incoming_rt}')
            return_names = [x for x in output_name_generator(len(incoming_rt))]
            return_map = OrderedDict(zip(return_names, incoming_rt))

        # Assume option 5
        else:
            logger.debug(f'Task returns a single output of type {incoming_rt}')
            return_map = {"out_0": incoming_rt}

        return get_variable_map(return_map)
    else:
        logger.debug(f'No return type found in annotations, returning empty map')
        # In the case where a task doesn't have a return type specified, assume that there are no outputs
        return {}


def get_interface_from_task_info(task_annotations: Dict[str, type]) -> _common_interface.TypedInterface:
    """
    From the annotations on a task function that the user should have provided, and the output names they want to use
    for each output parameter, construct the TypedInterface object

    For now the fancy object, maybe in the future a dumb object.

    :param task_annotations:
    :param output_names:
    """
    outputs_map = get_output_variable_map(task_annotations)

    inputs = OrderedDict()
    for k, v in task_annotations.items():
        if k != 'return':
            inputs[k] = v

    inputs_map = get_variable_map(inputs)
    interface_model = _interface_models.TypedInterface(inputs_map, outputs_map)

    # Maybe in the future we can just use the model
    return _common_interface.TypedInterface.promote_from_model(interface_model)


def get_variable_map(variable_map: Dict[str, type]) -> Dict[str, _interface_models.Variable]:
    """
    Given a map of str (names of inputs for instance) to their Python native types, return a map of the name to a
    Flyte Variable object with that type.
    """
    res = OrderedDict()
    e = type_engine.BaseEngine()
    for k, v in variable_map.items():
        res[k] = _interface_models.Variable(type=e.native_type_to_literal_type(v), description=k)

    return res
