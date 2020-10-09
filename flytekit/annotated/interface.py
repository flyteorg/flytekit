import copy
import inspect
from collections import OrderedDict
from typing import Dict, Generator, Union, Type, Tuple, List, Any, _GenericAlias, TypeVar

from flytekit import logger
from flytekit.annotated import type_engine
from flytekit.common import interface as _common_interface
from flytekit.models import interface as _interface_models, types as _type_models


class Interface(object):
    """
    A Python native interface object, like inspect.signature but simpler.
    """

    def __init__(self, inputs: Dict[str, Union[Type, Tuple[Type, Any]]] = None, outputs: Dict[str, Type] = None):
        """
        :param outputs: Output variables and their types as a dictionary
        :param inputs: the variable and its type only
        """
        self._inputs = {}
        if inputs:
            for k, v in inputs.items():
                if isinstance(v, Tuple) and len(v) > 1:
                    self._inputs[k] = v
                else:
                    self._inputs[k] = (v, None)
        self._outputs = outputs

    @property
    def inputs(self) -> Dict[str, Type]:
        r = {}
        for k, v in self._inputs.items():
            r[k] = v[0]
        return r

    @property
    def inputs_with_defaults(self) -> Dict[str, Tuple[Type, Any]]:
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    def remove_inputs(self, vars: List[str]) -> "Interface":
        """
        This method is useful in removing some variables from the Flyte backend inputs specification, as these are
        implicit local only inputs or will be supplied by the library at runtime. For example, spark-session etc
        It creates a new instance of interface with the requested variables removed
        """
        if vars is None:
            return self
        new_inputs = copy.copy(self._inputs)
        for v in vars:
            if v in new_inputs:
                del new_inputs[v]
        return Interface(new_inputs, self._outputs)

    def with_inputs(self, extra_inputs: Dict[str, Type]) -> "Interface":
        """
        Use this to add additional inputs to the interface. This is useful for adding additional implicit inputs that
        are added without the user requesting for them
        """
        if not extra_inputs:
            return self
        new_inputs = copy.copy(self._inputs)
        for k, v in extra_inputs:
            if k in new_inputs:
                raise ValueError(f"Input {k} cannot be added as it already exists in the interface")
            new_inputs[k] = v
        return Interface(new_inputs, self._outputs)

    def with_outputs(self, extra_outputs: Dict[str, Type]) -> "Interface":
        """
        This method allows addition of extra outputs are expected from a task specification
        """
        if not extra_outputs:
            return self
        new_outputs = copy.copy(self._outputs)
        for k, v in extra_outputs:
            if k in new_outputs:
                raise ValueError(f"Output {k} cannot be added as it already exists in the interface")
            new_outputs[k] = v
        return Interface(self._inputs, new_outputs)


def transform_inputs_to_parameters(interface: Interface) -> _interface_models.ParameterMap:
    """
    Transforms the given interface (with inputs) to a Parameter Map with defaults set
    :param interface: the interface object
    """
    if interface is None or interface.inputs_with_defaults is None:
        return _interface_models.ParameterMap({})
    inputs_vars = transform_variable_map(interface.inputs)
    params = {}
    inputs_with_def = interface.inputs_with_defaults
    for k, v in inputs_vars.items():
        val, _default = inputs_with_def[k]
        # TODO: Fix defaults and required
        required = _default is None
        params[k] = _interface_models.Parameter(var=v, default=_default, required=required)
    return _interface_models.ParameterMap(params)


def transform_interface_to_typed_interface(interface: Interface) -> _common_interface.TypedInterface:
    """
    Transform the given simple python native interface to FlyteIDL's interface
    """
    if interface is None:
        return None

    inputs_map = transform_variable_map(interface.inputs)
    outputs_map = transform_variable_map(interface.outputs)
    interface_model = _interface_models.TypedInterface(inputs_map, outputs_map)

    # Maybe in the future we can just use the model
    return _common_interface.TypedInterface.promote_from_model(interface_model)


def transform_variable_map_to_collection(
        m: Dict[str, _interface_models.Variable]) -> Dict[str, _interface_models.Variable]:
    """
    Converts a given variables to be collections of their type. This is useful for array jobs / map style code.
    It will create a collection of types even if any one these types is not a collection type
    """
    if m is None:
        return {}

    all_types_are_collection = True
    for k, v in m.items():
        if v.type.collection_type is None:
            all_types_are_collection = False
            break

    if all_types_are_collection:
        return m

    om = {}
    for k, v in m.items():
        om[k] = _interface_models.Variable(type=_type_models.LiteralType(collection_type=v.type),
                                           description=v.description)
    return om


def transform_typed_interface_to_collection_interface(
        interface: _common_interface.TypedInterface) -> _common_interface.TypedInterface:
    """
    Takes a single task interface and interpolates it to an array interface - to allow performing distributed python map
    like functions
    """
    map_inputs = transform_variable_map_to_collection(interface.inputs)
    map_outputs = transform_variable_map_to_collection(interface.outputs)
    return _common_interface.TypedInterface(inputs=map_inputs, outputs=map_outputs)


def transform_signature_to_interface(signature: inspect.Signature) -> Interface:
    """
    From the annotations on a task function that the user should have provided, and the output names they want to use
    for each output parameter, construct the TypedInterface object

    For now the fancy object, maybe in the future a dumb object.

    """
    outputs = extract_return_annotation(signature.return_annotation)

    inputs = OrderedDict()
    for k, v in signature.parameters.items():
        # Inputs with default values are currently ignored, we may want to look into that in the future
        inputs[k] = (v.annotation, v.default if v.default is not inspect.Parameter.empty else None)

    return Interface(inputs, outputs)


def transform_variable_map(variable_map: Dict[str, type]) -> Dict[str, _interface_models.Variable]:
    """
    Given a map of str (names of inputs for instance) to their Python native types, return a map of the name to a
    Flyte Variable object with that type.
    """
    res = OrderedDict()
    for k, v in variable_map.items():
        res[k] = transform_type(v, k)

    return res


def transform_type(x: type, description: str = None) -> _interface_models.Variable:
    e = type_engine.BaseEngine()
    return _interface_models.Variable(type=e.native_type_to_literal_type(x), description=description)


def default_output_name(index: int = 0) -> str:
    return f"out_{index}"


def output_name_generator(length: int) -> Generator[str, None, None]:
    for x in range(0, length):
        yield default_output_name(x)


def extract_return_annotation(return_annotation: Union[Type, Tuple]) -> Dict[str, Type]:
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

        # Option 6
        def t(a: int, b: str) -> None: ...

    TODO: We'll need to check the actual return types for in all cases as well, to make sure Flyte IDL actually
          supports it. For instance, typing.Tuple[Optional[int]] is not something we can represent currently.

    TODO: Generator[A,B,C] types are also valid, indicating dynamic tasks. Will need to implement.

    Note that Options 1 and 2 are identical, just syntactic sugar. In the NamedTuple case, we'll use the names in the
    definition. In all other cases, we'll automatically generate output names, indexed starting at 0.
    """

    # Handle Option 6
    # We can think about whether we should add a default output name with type None in the future.
    if return_annotation is None or return_annotation is inspect.Signature.empty:
        return {}

    # This statement results in true for typing.Namedtuple, single and void return types, so this
    # handles Options 1, 2, and 5. Even though NamedTuple for us is multi-valued, it's a single value for Python
    if isinstance(return_annotation, Type) or isinstance(return_annotation, _GenericAlias) \
            or isinstance(return_annotation, TypeVar):
        # isinstance / issubclass does not work for Namedtuple.
        # Options 1 and 2
        if hasattr(return_annotation, '_field_types'):
            logger.debug(f'Task returns named tuple {return_annotation}')
            return return_annotation._field_types
        # Option 5
        logger.debug(f'Task returns a single output of type {return_annotation}')
        return {default_output_name(): return_annotation}

    return_map = {}
    # Now lets handle multi-valued return annotations
    if hasattr(return_annotation, '__origin__') and return_annotation.__origin__ is tuple:
        # Handle option 3
        logger.debug(f'Task returns unnamed typing.Tuple {return_annotation}')
        return_types = return_annotation.__args__
    else:
        # Handle option 4
        logger.debug(f'Task returns unnamed native tuple {return_annotation}')
        return_types = return_annotation

    return_map = OrderedDict(zip(list(output_name_generator(len(return_types))), return_types))
    return return_map
