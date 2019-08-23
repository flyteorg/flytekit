from __future__ import absolute_import

import pytest

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import primitives, containers
from flytekit.models import types as literal_types, literals
from six.moves import range as _range


def test_list():
    list_type = containers.List(primitives.Integer)
    assert list_type.to_flyte_literal_type().simple is None
    assert list_type.to_flyte_literal_type().map_value_type is None
    assert list_type.to_flyte_literal_type().schema is None
    assert list_type.to_flyte_literal_type().collection_type.simple == literal_types.SimpleType.INTEGER

    list_value = list_type.from_python_std([1, 2, 3, 4])
    assert list_value.to_python_std() == [1, 2, 3, 4]
    assert list_type.from_flyte_idl(list_value.to_flyte_idl()) == list_value

    assert list_value.collection.literals[0].scalar.primitive.integer == 1
    assert list_value.collection.literals[1].scalar.primitive.integer == 2
    assert list_value.collection.literals[2].scalar.primitive.integer == 3
    assert list_value.collection.literals[3].scalar.primitive.integer == 4

    obj2 = list_type.from_string('[1, 2, 3,4]')
    assert obj2 == list_value

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_python_std(['a', 'b', 'c', 'd'])

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_python_std([1, 2, 3, 'abc'])

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_python_std(1)

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_python_std([[1]])

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_string('["fdsa"]')

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_string('[1, 2, 3, []]')

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_string('\'["not list json"]\'')

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_string('["unclosed","list"')


def test_string_list():
    list_type = containers.List(primitives.String)
    obj = list_type.from_string('["fdsa", "fff3", "fdsfhuie", "frfJliEILles", ""]')
    assert len(obj.collection.literals) == 5
    assert obj.to_python_std() == ["fdsa", "fff3", "fdsfhuie", "frfJliEILles", ""]

    # Test that two classes of the same type are comparable
    list_type_two = containers.List(primitives.String)
    obj2 = list_type_two.from_string('["fdsa", "fff3", "fdsfhuie", "frfJliEILles", ""]')
    assert obj == obj2


def test_empty_parsing():
    list_type = containers.List(primitives.String)
    obj = list_type.from_string('[]')
    assert len(obj) == 0

    # The String primitive type does not allow lists or maps to be converted
    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_string('["fdjs", []]')

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_string('["fdjs", {}]')


def test_nested_list():
    list_type = containers.List(containers.List(primitives.Integer))

    assert list_type.to_flyte_literal_type().simple is None
    assert list_type.to_flyte_literal_type().map_value_type is None
    assert list_type.to_flyte_literal_type().schema is None
    assert list_type.to_flyte_literal_type().collection_type.simple is None
    assert list_type.to_flyte_literal_type().collection_type.map_value_type is None
    assert list_type.to_flyte_literal_type().collection_type.schema is None
    assert list_type.to_flyte_literal_type().collection_type.collection_type.simple == literal_types.SimpleType.INTEGER

    gt = [[1, 2, 3], [4, 5, 6], []]
    list_value = list_type.from_python_std(gt)
    assert list_value.to_python_std() == gt
    assert list_type.from_flyte_idl(list_value.to_flyte_idl()) == list_value

    assert list_value.collection.literals[0].collection.literals[0].scalar.primitive.integer == 1
    assert list_value.collection.literals[0].collection.literals[1].scalar.primitive.integer == 2
    assert list_value.collection.literals[0].collection.literals[2].scalar.primitive.integer == 3

    assert list_value.collection.literals[1].collection.literals[0].scalar.primitive.integer == 4
    assert list_value.collection.literals[1].collection.literals[1].scalar.primitive.integer == 5
    assert list_value.collection.literals[1].collection.literals[2].scalar.primitive.integer == 6

    assert len(list_value.collection.literals[2].collection.literals) == 0

    obj = list_type.from_string('[[1, 2, 3], [4, 5, 6]]')
    assert len(obj) == 2
    assert len(obj.collection.literals[0]) == 3


def test_reprs():
    list_type = containers.List(primitives.Integer)
    obj = list_type.from_python_std(list(_range(3)))
    assert obj.short_string() == "List<Integer>(len=3, [Integer(0), Integer(1), Integer(2)])"
    assert obj.verbose_string() == \
        "List<Integer>(\n" \
        "\tlen=3,\n" \
        "\t[\n" \
        "\t\tInteger(0),\n" \
        "\t\tInteger(1),\n" \
        "\t\tInteger(2)\n" \
        "\t]\n" \
        ")"

    nested_list_type = containers.List(containers.List(primitives.Integer))
    nested_obj = nested_list_type.from_python_std([list(_range(3)), list(_range(3))])

    assert nested_obj.short_string() == \
        "List<List<Integer>>(len=2, [List<Integer>(len=3, [Integer(0), Integer(1), Integer(2)]), " \
        "List<Integer>(len=3, [Integer(0), Integer(1), Integer(2)])])"
    assert nested_obj.verbose_string() == \
        "List<List<Integer>>(\n" \
        "\tlen=2,\n" \
        "\t[\n" \
        "\t\tList<Integer>(\n" \
        "\t\t\tlen=3,\n" \
        "\t\t\t[\n" \
        "\t\t\t\tInteger(0),\n" \
        "\t\t\t\tInteger(1),\n" \
        "\t\t\t\tInteger(2)\n" \
        "\t\t\t]\n" \
        "\t\t),\n" \
        "\t\tList<Integer>(\n" \
        "\t\t\tlen=3,\n" \
        "\t\t\t[\n" \
        "\t\t\t\tInteger(0),\n" \
        "\t\t\t\tInteger(1),\n" \
        "\t\t\t\tInteger(2)\n" \
        "\t\t\t]\n" \
        "\t\t)\n" \
        "\t]\n" \
        ")"


def test_model_promotion():
    list_type = containers.List(primitives.Integer)
    list_model = literals.Literal(
        collection=literals.LiteralCollection(
            literals=[
                literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=0))),
                literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
                literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=2)))
            ]
        )
    )
    list_obj = list_type.promote_from_model(list_model)
    assert len(list_obj.collection.literals) == 3
    assert isinstance(list_obj.collection.literals[0], primitives.Integer)
    assert list_obj == list_type.from_python_std([0, 1, 2])
    assert list_obj == list_type([primitives.Integer(0), primitives.Integer(1), primitives.Integer(2)])
