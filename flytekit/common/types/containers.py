import json as _json

import six as _six

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types as _base_sdk_types
from flytekit.models import literals as _literals
from flytekit.models import types as _idl_types


class CollectionType(_base_sdk_types.FlyteSdkType):
    pass


class TypedCollectionType(CollectionType):
    @property
    def sub_type(cls):
        """
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        return cls._sub_type

    def __eq__(cls, other):
        return hasattr(other, "sub_type") and cls.sub_type == other.sub_type

    def __hash__(cls):
        # Python 3 checks complain if hash isn't implemented at the same time as equals
        return super(TypedCollectionType, cls).__hash__()


def List(sdk_type):
    """
    :param flytekit.common.types.base_sdk_types.FlyteSdkType sdk_type:
    :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
    """

    class TList(TypedListImpl):
        _sub_type = sdk_type

    # TODO: Figure out generics and type-hinting
    return TList


class ListImpl(_base_sdk_types.FlyteSdkValue, metaclass=CollectionType):
    def __len__(self):
        return len(self.collection.literals)


class TypedListImpl(ListImpl, metaclass=TypedCollectionType):
    @classmethod
    def from_string(cls, string_value):
        """
        Load the list from a JSON formatted string.
        :param Text string_value:
        :rtype: ListImpl<T>
        """
        try:
            items = _json.loads(string_value)
        except ValueError:
            raise _user_exceptions.FlyteTypeException(
                _six.text_type, cls, additional_msg="String not parseable to json {}".format(string_value),
            )

        if type(items) != list:
            raise _user_exceptions.FlyteTypeException(
                _six.text_type, cls, additional_msg="String is not a list {}".format(string_value),
            )

        # Instead of recursively calling from_string(), we're changing to from_python_std() instead because json
        # loading naturally interprets all layers, not just the outer layer.
        return cls([cls.sub_type.from_python_std(i) for i in items])

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        if not isinstance(type(other), TypedListImpl):
            return False
        return cls.sub_type.is_castable_from(other.sub_type)

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        if not isinstance(t_value, list):
            raise _user_exceptions.FlyteTypeException(type(t_value), list, t_value)
        return cls([cls.sub_type.from_python_std(v) for v in t_value])

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(collection_type=cls.sub_type.to_flyte_literal_type())

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: TypedListImpl
        """
        return cls([cls.sub_type.from_flyte_idl(l.to_flyte_idl()) for l in literal_model.collection.literals])

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "List<{}>".format(cls.sub_type.short_class_string())

    def __init__(self, value):
        """
        :param list[flytekit.common.types.base_sdk_types.FlyteSdkValue] value: List value to wrap
        """
        super(TypedListImpl, self).__init__(collection=_literals.LiteralCollection(literals=value))

    def to_python_std(self):
        """
        :rtype: list[T]
        """
        return [type(self).sub_type.from_flyte_idl(l.to_flyte_idl()).to_python_std() for l in self.collection.literals]

    def short_string(self):
        """
        :rtype: Text
        """
        num_to_print = 5
        to_print = [v.short_string() for v in self.collection.literals[:num_to_print]]
        if len(self.collection.literals) > num_to_print:
            to_print.append("...")
        return "{}(len={}, [{}])".format(
            type(self).short_class_string(), len(self.collection.literals), ", ".join(to_print),
        )

    def verbose_string(self):
        """
        :rtype: Text
        """
        return "{}(\n\tlen={},\n\t[\n\t\t{}\n\t]\n)".format(
            type(self).short_class_string(),
            len(self.collection.literals),
            ",\n\t\t".join("\n\t\t".join(v.verbose_string().splitlines()) for v in self.collection.literals),
        )
