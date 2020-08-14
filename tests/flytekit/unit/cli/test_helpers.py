from __future__ import absolute_import

import pytest

from flytekit.clis import helpers
from flytekit.models import literals, types
from flytekit.models.interface import Parameter, ParameterMap, Variable


def test_parse_args_into_dict():
    sample_args1 = (u"input_b=mystr", u"input_c=18")
    sample_args2 = ("input_a=mystr===d",)
    sample_args3 = ()
    output = helpers.parse_args_into_dict(sample_args1)
    assert output["input_b"] == "mystr"
    assert output["input_c"] == "18"

    output = helpers.parse_args_into_dict(sample_args2)
    assert output["input_a"] == "mystr===d"

    output = helpers.parse_args_into_dict(sample_args3)
    assert output == {}


def test_construct_literal_map_from_variable_map():
    v = Variable(type=types.LiteralType(simple=types.SimpleType.INTEGER), description="some description")
    variable_map = {
        "inputa": v,
    }

    input_txt_dictionary = {"inputa": "15"}

    literal_map = helpers.construct_literal_map_from_variable_map(variable_map, input_txt_dictionary)
    parsed_literal = literal_map.literals["inputa"].value
    ll = literals.Scalar(primitive=literals.Primitive(integer=15))
    assert parsed_literal == ll


def test_construct_literal_map_from_parameter_map():
    v = Variable(type=types.LiteralType(simple=types.SimpleType.INTEGER), description="some description")
    p = Parameter(var=v, required=True)
    pm = ParameterMap(parameters={"inputa": p})

    input_txt_dictionary = {"inputa": "15"}

    literal_map = helpers.construct_literal_map_from_parameter_map(pm, input_txt_dictionary)
    parsed_literal = literal_map.literals["inputa"].value
    ll = literals.Scalar(primitive=literals.Primitive(integer=15))
    assert parsed_literal == ll

    with pytest.raises(Exception):
        helpers.construct_literal_map_from_parameter_map(pm, {})


def test_strtobool():
    assert not helpers.str2bool("False")
    assert not helpers.str2bool("OFF")
    assert not helpers.str2bool("no")
    assert not helpers.str2bool("0")
    assert helpers.str2bool("t")
    assert helpers.str2bool("true")
    assert helpers.str2bool("stuff")
