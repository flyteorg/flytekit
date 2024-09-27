from flytekit.exceptions import base as _base_exceptions
from flytekit.exceptions.base import FlyteException


class FlyteSystemException(_base_exceptions.FlyteRecoverableException):
    _ERROR_CODE = "SYSTEM:Unknown"


class FlyteSystemUnavailableException(FlyteSystemException):
    _ERROR_CODE = "SYSTEM:Unavailable"

    def __str__(self):
        return "Flyte cluster is currently unavailable. Please make sure the cluster is up and running."


class FlyteNotImplementedException(FlyteSystemException, NotImplementedError):
    _ERROR_CODE = "SYSTEM:NotImplemented"


class FlyteEntrypointNotLoadable(FlyteSystemException):
    _ERROR_CODE = "SYSTEM:UnloadableCode"

    @classmethod
    def _create_verbose_message(cls, task_module, task_name=None, additional_msg=None):
        if task_name is None:
            return "Entrypoint is not loadable!  Could not load the module: '{task_module}'{additional_msg}".format(
                task_module=task_module,
                additional_msg=" due to error: {}".format(additional_msg) if additional_msg is not None else ".",
            )
        else:
            return (
                "Entrypoint is not loadable!  Could not find the task: '{task_name}' in '{task_module}'"
                "{additional_msg}".format(
                    task_module=task_module,
                    task_name=task_name,
                    additional_msg="." if additional_msg is None else " due to error: {}".format(additional_msg),
                )
            )

    def __init__(self, task_module, task_name=None, additional_msg=None):
        super(FlyteSystemException, self).__init__(
            self._create_verbose_message(task_module, task_name=task_name, additional_msg=additional_msg)
        )


class FlyteSystemAssertion(FlyteSystemException, AssertionError):
    _ERROR_CODE = "SYSTEM:AssertionError"


class FlyteAgentNotFound(FlyteSystemException, AssertionError):
    _ERROR_CODE = "SYSTEM:AgentNotFound"


class FlyteDownloadDataException(FlyteSystemException):
    _ERROR_CODE = "SYSTEM:DownloadDataError"


class FlyteUploadDataException(FlyteSystemException):
    _ERROR_CODE = "SYSTEM:UploadDataError"


class FlyteNonRecoverableSystemException(FlyteException):
    _ERROR_CODE = "USER:NonRecoverableSystemError"

    def __init__(self, exc_value: Exception):
        """
        FlyteNonRecoverableSystemException is thrown when a system code raises an exception.

        :param exc_value: The exception that was raised from system code.
        """
        self._exc_value = exc_value
        super().__init__(str(exc_value))

    @property
    def value(self):
        return self._exc_value
