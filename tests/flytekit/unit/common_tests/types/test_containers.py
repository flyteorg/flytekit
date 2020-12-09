import pytest
from six.moves import range as _range

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import containers, primitives
from flytekit.models import literals
from flytekit.models import types as literal_types


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

    obj2 = list_type.from_string("[1, 2, 3, 4]")
    assert obj2 == list_value

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_python_std(["a", "b", "c", "d"])

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_python_std([1, 2, 3, "abc"])

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_python_std(1)

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_python_std([[1]])

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_string('["fdsa"]')

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_string("[1, 2, 3, []]")

    with pytest.raises(_user_exceptions.FlyteTypeException):
        list_type.from_string("'[\"not list json\"]'")

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
    obj = list_type.from_string("[]")
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

    obj = list_type.from_string("[[1, 2, 3], [4, 5, 6]]")
    assert len(obj) == 2
    assert len(obj.collection.literals[0]) == 3


def test_list_reprs():
    list_type = containers.List(primitives.Integer)
    obj = list_type.from_python_std(list(_range(3)))
    assert obj.short_string() == "List<Integer>(len=3, [Integer(0), Integer(1), Integer(2)])"
    assert (
        obj.verbose_string() == "List<Integer>(\n"
        "\tlen=3,\n"
        "\t[\n"
        "\t\tInteger(0),\n"
        "\t\tInteger(1),\n"
        "\t\tInteger(2)\n"
        "\t]\n"
        ")"
    )

    nested_list_type = containers.List(containers.List(primitives.Integer))
    nested_obj = nested_list_type.from_python_std([list(_range(3)), list(_range(3))])

    assert (
        nested_obj.short_string()
        == "List<List<Integer>>(len=2, [List<Integer>(len=3, [Integer(0), Integer(1), Integer(2)]), "
        "List<Integer>(len=3, [Integer(0), Integer(1), Integer(2)])])"
    )
    assert (
        nested_obj.verbose_string() == "List<List<Integer>>(\n"
        "\tlen=2,\n"
        "\t[\n"
        "\t\tList<Integer>(\n"
        "\t\t\tlen=3,\n"
        "\t\t\t[\n"
        "\t\t\t\tInteger(0),\n"
        "\t\t\t\tInteger(1),\n"
        "\t\t\t\tInteger(2)\n"
        "\t\t\t]\n"
        "\t\t),\n"
        "\t\tList<Integer>(\n"
        "\t\t\tlen=3,\n"
        "\t\t\t[\n"
        "\t\t\t\tInteger(0),\n"
        "\t\t\t\tInteger(1),\n"
        "\t\t\t\tInteger(2)\n"
        "\t\t\t]\n"
        "\t\t)\n"
        "\t]\n"
        ")"
    )


def test_model_promotion():
    list_type = containers.List(primitives.Integer)
    list_model = literals.Literal(
        collection=literals.LiteralCollection(
            literals=[
                literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=0))),
                literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
                literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=2))),
            ]
        )
    )
    list_obj = list_type.promote_from_model(list_model)
    assert len(list_obj.collection.literals) == 3
    assert isinstance(list_obj.collection.literals[0], primitives.Integer)
    assert list_obj == list_type.from_python_std([0, 1, 2])
    assert list_obj == list_type([primitives.Integer(0), primitives.Integer(1), primitives.Integer(2)])


def test_map():
    map_type = containers.Map(primitives.Integer)
    assert map_type.is_castable_from(containers.Map(primitives.Integer))
    assert not map_type.is_castable_from(containers.Map(primitives.String))

    assert map_type.to_flyte_literal_type().simple is None
    assert map_type.to_flyte_literal_type().collection_type is None
    assert map_type.to_flyte_literal_type().schema is None
    assert map_type.to_flyte_literal_type().map_value_type.simple == literal_types.SimpleType.INTEGER

    d = {"a": 1, "b": 2, "c": 3, "d": 4}
    map_value = map_type.from_python_std(d)
    assert map_value.to_python_std() == d
    assert map_type.from_flyte_idl(map_value.to_flyte_idl()) == map_value

    assert map_value.map.literals["a"].scalar.primitive.integer == 1
    assert map_value.map.literals["b"].scalar.primitive.integer == 2
    assert map_value.map.literals["c"].scalar.primitive.integer == 3
    assert map_value.map.literals["d"].scalar.primitive.integer == 4

    obj2 = map_type.from_string('{"a": 1, "b": 2, "c": 3, "d": 4}')
    assert obj2 == map_value

    with pytest.raises(_user_exceptions.FlyteTypeException):
        map_type.from_python_std(["a", "b", "c", "d"])

    with pytest.raises(_user_exceptions.FlyteTypeException):
        map_type.from_python_std({1: 1, "b": 2, "c": 3, "d": 4})

    with pytest.raises(_user_exceptions.FlyteTypeException):
        map_type.from_string('{a: 1, "b": 2, "c": 3, "d": 4}')


def test_nested_map():
    map_type = containers.Map(containers.Map(primitives.Integer))

    assert map_type.to_flyte_literal_type().simple is None
    assert map_type.to_flyte_literal_type().collection_type is None
    assert map_type.to_flyte_literal_type().schema is None
    assert map_type.to_flyte_literal_type().map_value_type.map_value_type.simple == literal_types.SimpleType.INTEGER

    d = {"a": {"a1": 1}, "b": {"b1": 1}}
    map_value = map_type.from_python_std(d)
    assert map_value.to_python_std() == d
    assert map_type.from_flyte_idl(map_value.to_flyte_idl()) == map_value

    assert map_value.map.literals["a"].map.literals["a1"].scalar.primitive.integer == 1
    assert map_value.map.literals["b"].map.literals["b1"].scalar.primitive.integer == 1

    obj = map_type.from_string('{"a": {"a1": 1}, "b": {"b1": 1}}')
    assert len(obj) == 2
    assert len(obj.map.literals["a"]) == 1
    assert len(obj.map.literals["b"]) == 1


def test_map_reprs():
    map_type = containers.Map(primitives.Integer)
    obj = map_type.from_python_std({"a": 1, "b": 2, "c": 3, "d": 4})
    assert (
        obj.short_string()
        == "Map<Text, Integer>(len=4, {'a': Integer(1), 'b': Integer(2), 'c': Integer(3), 'd': Integer(4)})"
    )
    assert (
        obj.verbose_string() == "Map<Text, Integer>(\n"
        "\tlen=4,\n"
        "\t{\n"
        "\t\t'a': Integer(1),\n"
        "\t\t'b': Integer(2),\n"
        "\t\t'c': Integer(3),\n"
        "\t\t'd': Integer(4)\n"
        "\t}\n"
        ")"
    )

    nested_map_type = containers.Map(containers.Map(primitives.Integer))
    nested_obj = nested_map_type.from_python_std({"a": {"a1": 1}, "b": {"b1": 1}})

    assert (
        nested_obj.short_string() == "Map<Text, Map<Text, Integer>>(len=2, "
        "{'a': Map<Text, Integer>(len=1, {'a1': Integer(1)}), 'b': Map<Text, Integer>(len=1, {'b1': Integer(1)})})"
    )
    assert (
        nested_obj.verbose_string() == "Map<Text, Map<Text, Integer>>(\n"
        "\tlen=2,\n"
        "\t{\n"
        "\t\t'a': Map<Text, Integer>(\n"
        "\t\t\tlen=1,\n"
        "\t\t\t{\n"
        "\t\t\t\t'a1': Integer(1)\n"
        "\t\t\t}\n"
        "\t\t),\n"
        "\t\t'b': Map<Text, Integer>(\n"
        "\t\t\tlen=1,\n"
        "\t\t\t{\n"
        "\t\t\t\t'b1': Integer(1)\n"
        "\t\t\t}\n"
        "\t\t)\n"
        "\t}\n"
        ")"
    )
