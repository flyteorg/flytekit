import abc as _abc


class TypeEngine(object, metaclass=_abc.ABCMeta):
    @_abc.abstractmethod
    def python_std_to_sdk_type(self, t):
        """
        Converts a standard format for specifying types in Python to the Flyte typing structure.
        :param T t: User input.  Usually of the form: Types.Integer, [Types.Integer], {Types.String:
        Types.Integer}, etc.
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        pass

    @_abc.abstractmethod
    def get_sdk_type_from_literal_type(self, literal_type):
        """
        Takes the Flyte spec language and converts to an SDK object.
        :param flytekit.models.types.LiteralType literal_type:
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        pass

    @_abc.abstractmethod
    def infer_sdk_type_from_literal(self, literal):
        """
        From a literal value, we infer the correct SDK type.
        :param flytekit.models.literals.Literal literal:
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        pass
