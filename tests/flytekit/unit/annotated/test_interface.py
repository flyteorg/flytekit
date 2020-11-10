import inspect
import os
import typing
from typing import Dict, List

from flytekit.annotated import context_manager
from flytekit.annotated.interface import (
    extract_return_annotation,
    transform_inputs_to_parameters,
    transform_signature_to_interface,
    transform_variable_map,
)
from flytekit.extended_typing import FlyteFile
from flytekit.models.core import types as _core_types


def test_extract_only():
    def x() -> typing.NamedTuple("NT1", x_str=str, y_int=int):
        ...

    return_types = extract_return_annotation(inspect.signature(x).return_annotation)
    assert len(return_types) == 2
    assert return_types["x_str"] == str
    assert return_types["y_int"] == int

    def t() -> List[int]:
        ...

    return_type = extract_return_annotation(inspect.signature(t).return_annotation)
    assert len(return_type) == 1
    assert return_type["out_0"]._name == "List"
    assert return_type["out_0"].__origin__ == list

    def t() -> Dict[str, int]:
        ...

    return_type = extract_return_annotation(inspect.signature(t).return_annotation)
    assert len(return_type) == 1
    assert return_type["out_0"]._name == "Dict"
    assert return_type["out_0"].__origin__ == dict

    def t(a: int, b: str) -> typing.Tuple[int, str]:
        ...

    return_type = extract_return_annotation(inspect.signature(t).return_annotation)
    assert len(return_type) == 2
    assert return_type["out_0"] == int
    assert return_type["out_1"] == str

    def t(a: int, b: str) -> (int, str):
        ...

    return_type = extract_return_annotation(inspect.signature(t).return_annotation)
    assert len(return_type) == 2
    assert return_type["out_0"] == int
    assert return_type["out_1"] == str

    def t(a: int, b: str) -> str:
        ...

    return_type = extract_return_annotation(inspect.signature(t).return_annotation)
    assert len(return_type) == 1
    assert return_type["out_0"] == str

    def t(a: int, b: str) -> None:
        ...

    return_type = extract_return_annotation(inspect.signature(t).return_annotation)
    assert len(return_type) == 0

    def t(a: int, b: str) -> List[int]:
        ...

    return_type = extract_return_annotation(inspect.signature(t).return_annotation)
    assert len(return_type) == 1
    assert return_type["out_0"] == List[int]

    def t(a: int, b: str) -> Dict[str, int]:
        ...

    return_type = extract_return_annotation(inspect.signature(t).return_annotation)
    assert len(return_type) == 1
    assert return_type["out_0"] == Dict[str, int]


def test_named_tuples():
    nt1 = typing.NamedTuple("NT1", x_str=str, y_int=int)

    def x(a: int, b: str) -> typing.NamedTuple("NT1", x_str=str, y_int=int):
        return ("hello world", 5)

    def y(a: int, b: str) -> nt1:
        return nt1("hello world", 5)

    result = transform_variable_map(extract_return_annotation(inspect.signature(x).return_annotation))
    assert result["x_str"].type.simple == 3
    assert result["y_int"].type.simple == 1

    result = transform_variable_map(extract_return_annotation(inspect.signature(y).return_annotation))
    assert result["x_str"].type.simple == 3
    assert result["y_int"].type.simple == 1


def test_unnamed_typing_tuple():
    def z(a: int, b: str) -> typing.Tuple[int, str]:
        return 5, "hello world"

    result = transform_variable_map(extract_return_annotation(inspect.signature(z).return_annotation))
    assert result["out_0"].type.simple == 1
    assert result["out_1"].type.simple == 3


def test_regular_tuple():
    def q(a: int, b: str) -> (int, str):
        return 5, "hello world"

    result = transform_variable_map(extract_return_annotation(inspect.signature(q).return_annotation))
    assert result["out_0"].type.simple == 1
    assert result["out_1"].type.simple == 3


def test_single_output_new_decorator():
    def q(a: int, b: str) -> int:
        return a + len(b)

    result = transform_variable_map(extract_return_annotation(inspect.signature(q).return_annotation))
    assert result["out_0"].type.simple == 1


def test_sig_files():
    def q() -> os.PathLike:
        ...

    result = transform_variable_map(extract_return_annotation(inspect.signature(q).return_annotation))
    assert isinstance(result["out_0"].type.blob, _core_types.BlobType)


def test_file_types():
    def t1() -> FlyteFile["svg"]:
        ...

    return_type = extract_return_annotation(inspect.signature(t1).return_annotation)
    assert return_type["out_0"].extension() == FlyteFile["svg"].extension()


def test_parameters_and_defaults():
    ctx = context_manager.FlyteContext.current_context()

    def z(a: int, b: str) -> typing.Tuple[int, str]:
        ...

    our_interface = transform_signature_to_interface(inspect.signature(z))
    params = transform_inputs_to_parameters(ctx, our_interface)
    assert params.parameters["a"].required
    assert params.parameters["a"].default is None
    assert params.parameters["b"].required
    assert params.parameters["b"].default is None

    def z(a: int, b: str = "hello") -> typing.Tuple[int, str]:
        ...

    our_interface = transform_signature_to_interface(inspect.signature(z))
    params = transform_inputs_to_parameters(ctx, our_interface)
    assert params.parameters["a"].required
    assert params.parameters["a"].default is None
    assert not params.parameters["b"].required
    assert params.parameters["b"].default.scalar.primitive.string_value == "hello"

    def z(a: int = 7, b: str = "eleven") -> typing.Tuple[int, str]:
        ...

    our_interface = transform_signature_to_interface(inspect.signature(z))
    params = transform_inputs_to_parameters(ctx, our_interface)
    assert not params.parameters["a"].required
    assert params.parameters["a"].default.scalar.primitive.integer == 7
    assert not params.parameters["b"].required
    assert params.parameters["b"].default.scalar.primitive.string_value == "eleven"
