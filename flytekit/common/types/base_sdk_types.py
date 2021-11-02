import abc as _abc

from flyteidl.core.literals_pb2 import Literal

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models import common as _common_models
from flytekit.models import literals as _literal_models


class FlyteSdkType(_sdk_bases.ExtendedSdkType, metaclass=_common_models.FlyteABCMeta):
    @_abc.abstractmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        pass

    @_abc.abstractmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        pass

    @_abc.abstractmethod
    def from_string(cls, string_value):
        """
        :param Text string_value: It is up to each individual object to implement this.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        pass

    @_abc.abstractmethod
    def promote_from_model(cls, literal):
        """
        :param flytekit.models.literals.Literal literal:
        :rtype: FlyteSdkValue
        """
        pass

    @_abc.abstractmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        pass

    def __hash__(cls):
        return hash(cls.to_flyte_literal_type())


class FlyteSdkValue(_literal_models.Literal, metaclass=FlyteSdkType):
    @classmethod
    def from_flyte_idl(cls, pb2_object: Literal):
        """
        :param flyteidl.core.literals_pb2.Literal pb2_object:
        :rtype: FlyteSdkValue
        """
        literal = _literal_models.Literal.from_flyte_idl(pb2_object)
        if literal.scalar is not None and literal.scalar.none_type is not None:
            return Void()
        return cls.promote_from_model(literal)

    @_abc.abstractmethod
    def to_python_std(self):
        pass


class InstantiableType(FlyteSdkType, metaclass=_common_models.FlyteABCMeta):
    @_abc.abstractmethod
    def __call__(cls, *args, **kwargs):
        """
        TODO: Figure out generics for type hinting.

        :rtype: T
        """
        return super(InstantiableType, cls).__call__(*args, **kwargs)


class Void(FlyteSdkValue):
    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return True

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        return cls()

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        raise _user_exceptions.FlyteAssertion(
            "A Void type does not have a literal type and cannot be used in this " "manner."
        )

    @classmethod
    def promote_from_model(cls, _):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal _:
        :rtype: Void
        """
        return cls()

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "Void"

    def __init__(self):
        super(Void, self).__init__(scalar=_literal_models.Scalar(none_type=_literal_models.Void()))

    def to_python_std(self):
        """
        :rtype: NoneType
        """
        return None

    def short_string(self):
        """
        :rtype: Text
        """
        return "Void()"
