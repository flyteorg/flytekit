from __future__ import annotations
from typing import List, Dict, Tuple
import datetime as _datetime
from flytekit.common import constants as _common_constants
from flytekit.configuration.common import CONFIGURATION_SINGLETON
from flytekit.common import interface
from flytekit.common import (
    nodes as _nodes, sdk_bases as _sdk_bases, workflow_execution as _workflow_execution
)
from flytekit.common.types.helpers import python_std_to_sdk_type
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.mixins import registerable as _registerable, hash as _hash_mixin, launchable as _launchable_mixin
from flytekit.configuration import internal as _internal_config
from flytekit.engines import loader as _engine_loader
from flytekit.models import common as _common_model, task as _task_model, types as _type_models
from flytekit.models.core import workflow as _workflow_model, identifier as _identifier_model
from flytekit.common.exceptions import user as _user_exceptions, system as _system_exceptions
from flytekit.common.types import helpers as _type_helpers
import six as _six

from flytekit.common import sdk_bases as _sdk_bases, promise as _promise
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import helpers as _type_helpers, containers as _containers, primitives as _primitives
from flytekit.models import interface as _interface_models, literals as _literal_models



# Has Python in the name because this is the least abstract task. It will have access to the loaded Python function
# itself if run locally, so it will always be a Python task.
# This is analagous to the current SdkRunnableTask. Need to analyze the benefits of duplicating the class versus
# adding to it.
class PythonTask(object):
    task_type = _common_constants.SdkTaskType.PYTHON_TASK

    def __init__(self, task_function, interface, metadata: _task_model.TaskMetadata, info):
        self._task_function = task_function
        self._interface = interface
        self._metadata = metadata
        self._info = info

    def __call__(self, *args, **kwargs):
        if CONFIGURATION_SINGLETON.x == 1:
            # Instead of calling the function, scan the inputs, if any, construct a node of inputs and outputs
            # But what do you call the output references?  How do you refer to the node name?
            #
            print('here')

            if len(args) > 0:
                raise _user_exceptions.FlyteAssertion(
                    "When adding a task as a node in a workflow, all inputs must be specified with kwargs only.  We "
                    "detected {} positional args.".format(len(args))
                )

            # Move away from this to use basic model classes instead
            bindings, upstream_nodes = self.interface.create_bindings_for_inputs(kwargs)

            return _nodes.SdkNode(
                id=None,
                metadata=_workflow_model.NodeMetadata("", self.metadata.timeout, self.metadata.retries,
                                                      self.metadata.interruptible),
                bindings=sorted(bindings, key=lambda b: b.var),
                upstream_nodes=upstream_nodes,
                sdk_task=self
            )
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
        outputs=None,
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
    print('======================(((((((((((((((((((((9')

    def wrapper(fn):

        task_obj = {}
        task_obj['task_type'] = _common_constants.SdkTaskType.PYTHON_TASK,
        task_obj['discovery_version'] = cache_version,
        task_obj['retries'] = retries,
        task_obj['interruptible'] = interruptible,
        task_obj['deprecated'] = deprecated,
        task_obj['storage_request'] = storage_request,
        task_obj['cpu_request'] = cpu_request,
        task_obj['gpu_request'] = gpu_request,
        task_obj['memory_request'] = memory_request,
        task_obj['storage_limit'] = storage_limit,
        task_obj['cpu_limit'] = cpu_limit,
        task_obj['gpu_limit'] = gpu_limit,
        task_obj['memory_limit'] = memory_limit,
        task_obj['discoverable'] = cache,
        task_obj['timeout'] = timeout or _datetime.timedelta(seconds=0),
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

        interface = get_interface_from_task_info(fn.__annotations__, outputs or [])

        task_instance = PythonTask(fn, interface, metadata, task_obj)
        task_instance.id = _identifier_model.Identifier(_identifier_model.ResourceType.TASK, "proj", "dom", "blah", "1")

        return task_instance

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def get_interface_from_task_info(task_annotations: Dict[str, type], output_names: List[str]) -> interface.TypedInterface:
    """
    From the annotations on a task function that the user should have provided, and the output names they want to use
    for each output parameter, construct the TypedInterface object

    For now the fancy object, maybe in the future a dumb object.

    :param task_annotations:
    :param output_names:
    """

    if 'return' not in task_annotations and len(output_names) != 0:
        raise Exception('1fkdsa39mlsda')

    return_types: Tuple[type] = []

    if type(task_annotations['return']) != tuple:
        # If there's just one return value, the return type is not a tuple so let's make it a tuple
        return_types = (task_annotations['return'],)
    else:
        return_types = task_annotations['return']

    if len(output_names) != len(return_types):
        raise Exception(f"Output length mismatch expected {output_names} got {return_types}")

    inputs = {k: v for k, v in task_annotations.items() if k != 'return'}

    inputs_map = get_variable_map(inputs)
    outputs_map = get_variable_map_from_lists(output_names, return_types)
    print(f"Inputs map is {inputs_map}")
    print(f"Outputs map is {outputs_map}")
    interface_model = _interface_models.TypedInterface(inputs_map, outputs_map)

    # Maybe in the future we can just use the model
    return interface.TypedInterface.promote_from_model(interface_model)


def get_variable_map_from_lists(variable_names: List[str], python_types: Tuple[type]) -> Dict[str, _interface_models.Variable]:
    return get_variable_map(dict(zip(variable_names, python_types)))


SIMPLE_TYPE_LOOKUP_TABLE: Dict[type, _type_models.LiteralType] = {
    int: _primitives.Integer.to_flyte_literal_type(),
    float: _primitives.Float.to_flyte_literal_type(),
    bool: _primitives.Boolean,
    _datetime.datetime: _primitives.Datetime.to_flyte_literal_type(),
    _datetime.timedelta: _primitives.Timedelta.to_flyte_literal_type(),
    str: _primitives.String.to_flyte_literal_type(),
    dict: _primitives.Generic.to_flyte_literal_type(),
}


def get_variable_map(variable_map: Dict[str, type]) -> Dict[str, _interface_models.Variable]:
    # print(f"var type map {variable_map}")
    # print('-------------')
    res = {}
    for k, v in variable_map.items():
        if v not in SIMPLE_TYPE_LOOKUP_TABLE:
            raise Exception(f"Python type {v} not yet supported.")
        flyte_literal_type = SIMPLE_TYPE_LOOKUP_TABLE[v]
        res[k] = _interface_models.Variable(type=flyte_literal_type, description=k)

    return res


# def create_bindings_for_inputs(interface_input, map_of_bindings) -> List[_literal_models.Binding]:
#     """
#     :param dict[Text, T] map_of_bindings:  This can be scalar primitives, it can be node output references,
#         lists, etc..
#     :rtype: (list[flytekit.models.literals.Binding])
#     :raises: flytekit.common.exceptions.user.FlyteAssertion
#     """
#     binding_data = dict()
#
#     for k in sorted(interface_input):
#         var = interface_input[k]
#         if k not in map_of_bindings:
#             raise _user_exceptions.FlyteAssertion(
#                 f"Input was not specified for: {k} of type {var.type}"
#             )
#
#         binding_data[k] = BindingData.from_python_std(
#             var.type,
#             map_of_bindings[k],
#         )
#
#     extra_inputs = set(binding_data.keys()) ^ set(map_of_bindings.keys())
#     if len(extra_inputs) > 0:
#         raise _user_exceptions.FlyteAssertion(
#             "Too many inputs were specified for the interface.  Extra inputs were: {}".format(extra_inputs)
#         )
#
#     seen_nodes = set()
#     min_upstream = list()
#     for n in all_upstream_nodes:
#         if n not in seen_nodes:
#             seen_nodes.add(n)
#             min_upstream.append(n)
#
#     return [_literal_models.Binding(k, bd) for k, bd in _six.iteritems(binding_data)], min_upstream



