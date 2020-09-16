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


class WorkflowOutputs(tuple):
    def __init__(self, *args, **kwargs):
        if len(kwargs) > 0:
            raise Exception("nope, can't do this")
        self._args = args

        if CONFIGURATION_SINGLETON.x == 1:
            # TODO: POC assumes that all args are promise.NodeOutputs - fix this
            # We need to go through the same exercise as for tasks. In fact, this is the reason why we need to have the
            # WorkflowOutputs object at all... it's just a way to inject code before the workflow function returns. The
            # code that we need to inject is this right here. We need to loop through all the nodes to see if any are
            # not set. For instance, given the same example:
            # (in a workflow...)
            #     a = t1()  # This produces a promise.NodeOutput which contains a node
            #     b = t2(a)
            #     return WorkflowOutputs(b)
            # the 'a' node should have its id already by virtue of being handled in t2's call. However, 'b' itself
            # isn't assigned yet. This will loop through and make sure that they're all assigned.
            # Unlike in the task case however, if it doesn't find a node name we will raise an Exception. For now at
            # least, we're not going to allow people to write
            #    return WorkflowOutputs(t2(a))
            for promise_node_output in args:
                n = promise_node_output.sdk_node
                if n.id is None:
                    # Should we do this?  Or should we just go back one stack frame, and then assume it's always going
                    # to be the workflow definition function? That way, we can basically do what we do now, which is
                    # to do a dir() on the workflow class, and detect all the members.
                    node_id = get_earliest_promise_name(n)
                    if node_id is None:
                        raise Exception(f"nope can't be none {n}")
                    n._id = node_id

                # Now that we have ids for the top level nodes, let's go down and recursively fill in any that are
                # still missing an id
                fill_in_upstream_nodes(n)

    @property
    def args(self):
        return self._args


def workflow(
        _workflow_function=None,
        outputs: List[str]=None
):
    # Unlike for tasks, where we can determine the entire structure of the task by looking at the function's signature,
    # workflows need to have the body of the function itself run at module-load time. This is because the body of the
    # workflow is what expresses the workflow structure.
    def wrapper(fn):
        old_setting = CONFIGURATION_SINGLETON.x
        CONFIGURATION_SINGLETON.x = 1

        task_annotations = fn.__annotations__
        inputs = {k: v for k, v in task_annotations.items() if k != 'return'}

        # Create inputs, just inputs. Outputs need to come later.
        inputs_map = get_variable_map(inputs)

        # Create promises out of all the inputs. Check for defaults in the function definition.
        default_inputs = get_default_args(fn)
        input_parameters = []
        input_parameter_models = []
        for input_name, input_variable_obj in inputs_map.items():
            # TODO: Fix defaults and required
            parameter_model = _interface_models.Parameter(var=input_variable_obj, default=None, required=True)
            input_parameter_models.append(parameter_model)

            # This is a bit annoying. I'd like to work directly with the Parameter model like above , but for now
            # it's easier to use the promise.Input wrapper
            # This is also annoying... I already have the literal type, but I have to go back to the SDK type (invoking
            # the type engine)... in the constructor, it again turns it back to the literal type when creating the
            # Parameter model.
            sdk_type = _type_helpers.get_sdk_type_from_literal_type(input_variable_obj.type)
            # logger.debug(f"Converting literal type {input_variable_obj.type} to sdk type {sdk_type}")
            arg_map = {'default': default_inputs[input_name]} if input_name in default_inputs else {}
            input_parameters.append(_WorkflowInput(name=input_name, type=sdk_type, **arg_map))

        # Fill in call args later - for now this only works for workflows with no inputs
        workflow_outputs = fn()

        # Iterate through the workflow outputs and collect two things
        #  1. Get the outputs and use them to construct the old Output objects
        #      promise.NodeOutputs (let's just focus on this one first for POC)
        #      or Input objects from above in the case of a passthrough value
        #      or outputs can be like 5, or 'hi' (<-- actually they can't right? but only because we can't name them).
        #           Maybe this is something we can add in the future using kwargs if there's demand for it.
        #  2. Iterate through the outputs and collect all the nodes.

        workflow_output_objs = []
        all_nodes = []
        # These should line up with the output input argument
        # TODO: Add length checks.
        # logger.debug(f"Workflow outputs: {outputs}")

        # Construct the output portion of the interface
        # Only works when all outputs are promise.NodeOutput's
        outputs_map = {}
        for i, out in enumerate(workflow_outputs.args):
            outputs[i]: _interface_models.Variable(type=out.sdk_type.to_flyte_literal_type(),
                                                   description=f"Original var name {out.var}")

        # With both input and output types and names sorted, we can create the interface model.
        interface_model = _interface_models.TypedInterface(inputs=inputs_map, outputs=outputs_map)

        # This section constructs the output bindings of the workflow. Can merge with the above iteration if desired.
        bindings = []
        for i, out in enumerate(workflow_outputs.args):
            output_name = out.var
            output_literal_type = out.sdk_type.to_flyte_literal_type()
            # logger.debug(f"Got output wrapper: {out}")
            # logger.debug(f"Var name {output_name} wf output name {outputs[i]} type: {output_literal_type}")
            binding_data = _literal_models.BindingData(promise=out)
            binding = _literal_models.Binding(var=outputs[i], binding=binding_data)
            bindings.append(binding)

            # Recursively discover all nodes. The final WorkflowOutput object may have only been called with one node/
            # output, but there may be a whole bunch more linked to earlier.
            all_nodes.extend(get_all_upstream_nodes(out.sdk_node))
            all_nodes.append(out.sdk_node)

            # Formerly, used the regular common.workflow.Output object, which is just a wrapper around a BindingData,
            # and a Variable.
            # workflow_output_objs.append(_WorkflowOutput(outputs[i], out, out.sdk_type))

        # TODO: Again, at this point, we should be able to identify the name of the workflow
        workflow_id = _identifier_model.Identifier(_identifier_model.ResourceType.WORKFLOW,
                                                   "proj", "dom", "moreblah", "1")
        # logger.debug("+++++++++++++++++++++++++++++++++++++")
        # logger.debug(f"Inputs {input_parameters}")
        # logger.debug(f"Output objects {workflow_output_objs}")
        # logger.debug(f"Nodes {all_nodes}")

        # Create a FlyteWorkflow object. We call this like how promote_from_model would call this, by ignoring the
        # fancy arguments and supplying just the raw elements manually. Alternatively we can construct the
        # WorkflowTemplate object, and then call promote_from_model.
        sdk_workflow = _SdkWorkflow(inputs=None, outputs=None, nodes=all_nodes, id=workflow_id, metadata=None,
                                    metadata_defaults=None, interface=interface_model, output_bindings=bindings)
        # logger.debug(f"SdkWorkflow {sdk_workflow}")
        # logger.debug("+++++++++++++++++++++++++++++++++++++")

        workflow_instance = PythonWorkflow(fn)
        workflow_instance.id = workflow_id

        CONFIGURATION_SINGLETON.x = old_setting
        return workflow_instance

    if _workflow_function:
        return wrapper(_workflow_function)
    else:
        return wrapper


def fill_in_upstream_nodes(sdk_node: _nodes.SdkNode):
    """
    Recursively fill in missing node IDs, depth-first.
    :param sdk_node:
    :return:
    """
    if sdk_node.id is None:
        raise Exception(f"Can't fill in upstream when node given is itself not ID'ed")

    for upstream in sdk_node.upstream_nodes:
        if upstream._id is None:
            upstream._id = sdk_node.id + f"-anon_{upstream.metadata.name}"
        fill_in_upstream_nodes(upstream)


def get_all_upstream_nodes(sdk_node: _nodes.SdkNode):
    """
    Recursively get all the upstream nodes for a node, depth-first.
    """
    res = []
    res.extend(sdk_node.upstream_nodes)
    for n in sdk_node.upstream_nodes:
        res.extend(get_all_upstream_nodes(n))
    return res


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

            # TODO: Make the metadata name the full name of the (function)?
            # There should be no reason to ever assign a random node id (at least in a non-dynamic context), so we
            # leave it empty for now.
            sdk_node = _nodes.SdkNode(
                id=None,
                metadata=_workflow_model.NodeMetadata(self._task_function.__name__, self.metadata.timeout, self.metadata.retries,
                                                      self.metadata.interruptible),
                bindings=sorted(bindings, key=lambda b: b.var),
                upstream_nodes=[],  # Set these later
                sdk_task=self
            )
            ctx.compilation_state.nodes.append(sdk_node)

            # TODO: Return multiple versions of the _same_ node, but with different output names. This is what this
            #       OutputParameterMapper does for us, but should we try to move away from it?
            ppp = OutputParameterMapper(self.interface.outputs, sdk_node)
            # Don't print this, it'll crash cuz sdk_node._upstream_node_ids might be None, but idl code will break

            if len(self.interface.outputs) > 1:
                # Why do we need to do this? Just for proper binding downstream, nothing else. This is
                # basically what happens when you call my_task_node.outputs.foo, but we're doing it for the user.
                wrapped_nodes = [ppp[k] for k in self.interface.outputs.keys()]
                return tuple(wrapped_nodes)
            else:
                return ppp[list(self.interface.outputs.keys())[0]]

        else:
            return self._task_function(*args, **kwargs)

    @property
    def interface(self) -> interface.TypedInterface:
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


def get_interface_from_task_info(task_annotations: Dict[str, type]) -> interface.TypedInterface:
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
    return interface.TypedInterface.promote_from_model(interface_model)


def get_variable_map(variable_map: Dict[str, type]) -> Dict[str, _interface_models.Variable]:
    """
    Given a map of str (names of inputs for instance) to their Python native types, return a map of the name to a
    Flyte Variable object with that type.
    """
    res = {}
    e = type_engine.BaseEngine()
    for k, v in variable_map.items():
        res[k] = _interface_models.Variable(type=e.native_type_to_literal_type(v), description=k)

    return res


def get_earliest_promise_name(node_object):
    """
    This is the hackiest bit of this whole file. We walk up the stack, looking at the local variables defined at each
    layer, in order to detect what variable an object was assigned to. See the call-site for more comments.
    """
    frame = inspect.currentframe()
    var_name = None

    while frame:
        # Have to make a copy because otherwise you get a iteration size changed in place error.
        frame_locals = {k: v for k, v in frame.f_locals.items()}
        for k, v in frame_locals.items():
            if isinstance(v, _NodeOutput):
                # Use is here instead of == because we're looking for pointer equality, not value, which for IDL
                # entities compares the to_flyte_idl() output.  For nodes that have upstream nodes that are still
                # missing ids, calling to_flyte_idl() throws an Exception.
                if v.sdk_node is node_object:
                    # logger.debug(f"Got key {k} at {frame.f_lineno}")
                    var_name = k
        frame = frame.f_back

    # logger.debug(f"For node {node_object.id} returning text label {var_name}")
    return var_name
