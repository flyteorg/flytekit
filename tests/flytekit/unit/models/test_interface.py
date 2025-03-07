import pytest

from flytekit.models import interface, types
from tests.flytekit.common.parameterizers import LIST_OF_ALL_LITERAL_TYPES


@pytest.mark.parametrize("literal_type", LIST_OF_ALL_LITERAL_TYPES)
def test_variable_type(literal_type):
    var = interface.Variable(type=literal_type, description="abc")
    assert var.type == literal_type
    assert var.description == "abc"
    assert var == interface.Variable.from_flyte_idl(var.to_flyte_idl())


@pytest.mark.parametrize("literal_type", LIST_OF_ALL_LITERAL_TYPES)
def test_variable_type_list(literal_type):
    var = interface.Variable(type=literal_type, description="abc")
    collection_var = interface.Variable(
        type=types.LiteralType(collection_type=literal_type),
        description="abc",
    )
    assert collection_var == interface.Variable.from_flyte_idl(var.to_flyte_idl_list())


@pytest.mark.parametrize("literal_type", LIST_OF_ALL_LITERAL_TYPES)
def test_typed_interface(literal_type):
    typed_interface = interface.TypedInterface(
        {"a": interface.Variable(literal_type, "description1")},
        {"b": interface.Variable(literal_type, "description2"), "c": interface.Variable(literal_type, "description3")},
    )

    assert typed_interface.inputs["a"].type == literal_type
    assert typed_interface.outputs["b"].type == literal_type
    assert typed_interface.outputs["c"].type == literal_type
    assert typed_interface.inputs["a"].description == "description1"
    assert typed_interface.outputs["b"].description == "description2"
    assert typed_interface.outputs["c"].description == "description3"
    assert len(typed_interface.inputs) == 1
    assert len(typed_interface.outputs) == 2

    pb = typed_interface.to_flyte_idl()
    deserialized_typed_interface = interface.TypedInterface.from_flyte_idl(pb)
    assert typed_interface == deserialized_typed_interface

    assert deserialized_typed_interface.inputs["a"].type == literal_type
    assert deserialized_typed_interface.outputs["b"].type == literal_type
    assert deserialized_typed_interface.outputs["c"].type == literal_type
    assert deserialized_typed_interface.inputs["a"].description == "description1"
    assert deserialized_typed_interface.outputs["b"].description == "description2"
    assert deserialized_typed_interface.outputs["c"].description == "description3"
    assert len(deserialized_typed_interface.inputs) == 1
    assert len(deserialized_typed_interface.outputs) == 2


@pytest.mark.parametrize("literal_type", LIST_OF_ALL_LITERAL_TYPES)
@pytest.mark.parametrize(
    "bound_inputs, excluded_inputs",
    [
        (set(), set()),
        ({"a"}, set()),
        (set(), {"b"}),
        ({"a"}, {"b"}),
        ({"a", "b"}, set()),
        (set(), {"a", "b"}),
    ])
def test_transform_interface_to_list(literal_type, bound_inputs, excluded_inputs):
    typed_interface = interface.TypedInterface(
        {
            "a": interface.Variable(literal_type, "description1"),
            "b": interface.Variable(literal_type, "description2")
        },
        {
            "c": interface.Variable(literal_type, "description3"),
            "d": interface.Variable(literal_type, "description4")
        },
    )

    deserialized_typed_interface_list = typed_interface.transform_interface_to_list(
        bound_inputs=bound_inputs,
        excluded_inputs=excluded_inputs
    )

    for param in typed_interface.inputs:
        if param in excluded_inputs:
            assert param not in deserialized_typed_interface_list.inputs
        elif param in bound_inputs:
            assert deserialized_typed_interface_list.inputs[param].type == literal_type
        else:
            assert deserialized_typed_interface_list.inputs[param].type == types.LiteralType(
                collection_type=literal_type
            )

    for param in typed_interface.outputs:
        assert deserialized_typed_interface_list.outputs[param].type == types.LiteralType(
            collection_type=literal_type
        )
        assert (deserialized_typed_interface_list.outputs[param].description ==
                typed_interface.outputs[param].description)

    assert len(deserialized_typed_interface_list.inputs) == 2 - len(excluded_inputs)
    assert len(deserialized_typed_interface_list.outputs) == 2


def test_parameter():
    v = interface.Variable(types.LiteralType(simple=types.SimpleType.BOOLEAN), "asdf asdf asdf")
    obj = interface.Parameter(var=v)
    assert obj.var == v

    obj2 = interface.Parameter.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.var == v


def test_parameter_map():
    v = interface.Variable(types.LiteralType(simple=types.SimpleType.BOOLEAN), "asdf asdf asdf")
    p = interface.Parameter(var=v)

    obj = interface.ParameterMap({"ppp": p})
    obj2 = interface.ParameterMap.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2


def test_variable_map():
    v = interface.Variable(types.LiteralType(simple=types.SimpleType.BOOLEAN), "asdf asdf asdf")
    obj = interface.VariableMap({"vvv": v})

    obj2 = interface.VariableMap.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
