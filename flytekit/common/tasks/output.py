from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.types import base_sdk_types as _base_sdk_types


class OutputReference(object):
    def __init__(self, sdk_type):
        """
        :param flytekit.common.types.base_sdk_types.FlyteSdkType sdk_type:
        """
        self._raw_value = None
        self._sdk_type = sdk_type
        self._sdk_value = _base_sdk_types.Void()

    @property
    def value(self):
        """
        :rtype: T
        """
        return self._raw_value

    @property
    def sdk_value(self):
        """
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkValue
        """
        return self._sdk_value

    @property
    def sdk_type(self):
        """
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        return self._sdk_type

    @_exception_scopes.system_entry_point
    def set(self, value):
        """
        This should be called to set the value for output.  The SDK will apply the appropriate type and value checking.
        It will raise an exception if necessary.
        :param T value:
        :raises: flytekit.common.exceptions.user.FlyteValueException
        """

        sdk_value = self._sdk_type.from_python_std(value)
        self._raw_value = value
        self._sdk_value = sdk_value
