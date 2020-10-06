import inspect
from collections import OrderedDict
from typing import Dict, Generator, Union, Type, Tuple

from flytekit import logger
from flytekit.annotated import type_engine
from flytekit.common import interface as _common_interface
from flytekit.models import interface as _interface_models


def transform_signature_to_typed_interface(signature: inspect.Signature) -> _common_interface.TypedInterface:
    """
    From the annotations on a task function that the user should have provided, and the output names they want to use
    for each output parameter, construct the TypedInterface object

    For now the fancy object, maybe in the future a dumb object.

    """
    outputs = extract_return_annotation(signature.return_annotation)

    inputs = OrderedDict()
    for k, v in signature.parameters.items():
        # Inputs with default values are currently ignored, we may want to look into that in the future
        inputs[k] = v.annotation

    inputs_map = transform_variable_map(inputs)
    outputs_map = transform_variable_map(outputs)
    interface_model = _interface_models.TypedInterface(inputs_map, outputs_map)

    # Maybe in the future we can just use the model
    return _common_interface.TypedInterface.promote_from_model(interface_model)


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
    if isinstance(return_annotation, Type):
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
