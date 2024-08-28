import typing

from flytekit.exceptions.base import FlyteException as _FlyteException
from flytekit.exceptions.base import FlyteRecoverableException as _Recoverable

if typing.TYPE_CHECKING:
    from flytekit.core.base_task import Task
    from flytekit.core.workflow import WorkflowBase


class FlyteUserException(_FlyteException):
    _ERROR_CODE = "USER:Unknown"


class FlyteTypeException(FlyteUserException, TypeError):
    _ERROR_CODE = "USER:TypeError"

    @staticmethod
    def _is_a_container(value):
        return isinstance(value, list) or isinstance(value, tuple) or isinstance(value, set)

    @classmethod
    def _create_verbose_message(cls, received_type, expected_type, received_value=None, additional_msg=None):
        if received_value is not None:
            return "Type error!  Received: {} with value: {}, Expected{}: {}. {}".format(
                received_type,
                received_value,
                " one of" if FlyteTypeException._is_a_container(expected_type) else "",
                expected_type,
                additional_msg or "",
            )
        else:
            return "Type error!  Received: {}, Expected{}: {}. {}".format(
                received_type,
                " one of" if FlyteTypeException._is_a_container(expected_type) else "",
                expected_type,
                additional_msg or "",
            )

    def __init__(self, received_type, expected_type, additional_msg=None, received_value=None):
        super(FlyteTypeException, self).__init__(
            self._create_verbose_message(
                received_type,
                expected_type,
                received_value=received_value,
                additional_msg=additional_msg,
            )
        )


class FlyteValueException(FlyteUserException, ValueError):
    _ERROR_CODE = "USER:ValueError"

    @classmethod
    def _create_verbose_message(cls, received_value, error_message):
        return "Value error!  Received: {}. {}".format(received_value, error_message)

    def __init__(self, received_value, error_message):
        super(FlyteValueException, self).__init__(self._create_verbose_message(received_value, error_message))


class FlyteDataNotFoundException(FlyteValueException):
    def __init__(self, path: str):
        super(FlyteDataNotFoundException, self).__init__(path, "File not found")


class FlyteEntityNotFoundException(FlyteValueException):
    def __init__(self, module_name: str, entity_name: str):
        self._module_name = module_name
        self._entity_name = entity_name

    def __str__(self):
        return f"Task/Workflow '{self._entity_name}' not found in module '{self._module_name}'"


class FlyteAssertion(FlyteUserException, AssertionError):
    _ERROR_CODE = "USER:AssertionError"


class FlyteValidationException(FlyteAssertion):
    _ERROR_CODE = "USER:ValidationError"


class FlyteFailureNodeInputMismatchException(FlyteAssertion):
    _ERROR_CODE = "USER:FailureNodeInputMismatch"

    def __init__(self, failure_node_node: typing.Union["WorkflowBase", "Task"], workflow: "WorkflowBase"):
        self.failure_node_node = failure_node_node
        self.workflow = workflow

    def __str__(self):
        return (
            f"Mismatched Inputs Detected\n"
            f"The failure node `{self.failure_node_node.name}` has inputs that do not align with those expected by the workflow `{self.workflow.name}`.\n"
            f"Failure Node's Inputs: {self.failure_node_node.python_interface.inputs}\n"
            f"Workflow's Inputs: {self.workflow.python_interface.inputs}\n"
            "Action Required:\n"
            "Please ensure that all input arguments in the failure node are provided and match the expected arguments specified in the workflow."
        )


class FlyteDisapprovalException(FlyteAssertion):
    _ERROR_CODE = "USER:ResultNotApproved"


class FlyteEntityAlreadyExistsException(FlyteAssertion):
    _ERROR_CODE = "USER:EntityAlreadyExists"


class FlyteEntityNotExistException(FlyteAssertion):
    _ERROR_CODE = "USER:EntityNotExist"


class FlyteTimeout(FlyteAssertion):
    _ERROR_CODE = "USER:Timeout"


class FlyteRecoverableException(FlyteUserException, _Recoverable):
    _ERROR_CODE = "USER:Recoverable"


class FlyteAuthenticationException(FlyteAssertion):
    _ERROR_CODE = "USER:AuthenticationError"


class FlyteInvalidInputException(FlyteUserException):
    _ERROR_CODE = "USER:BadInputToAPI"

    def __init__(self, request: typing.Any):
        self.request = request
        super().__init__()


class FlytePromiseAttributeResolveException(FlyteAssertion):
    _ERROR_CODE = "USER:PromiseAttributeResolveError"


class FlyteCompilationException(FlyteUserException):
    _ERROR_CODE = "USER:CompileError"

    def __init__(self, fn: typing.Callable, param_name: typing.Optional[str] = None):
        self.fn = fn
        self.param_name = param_name


class FlyteMissingTypeException(FlyteCompilationException):
    _ERROR_CODE = "USER:MissingTypeError"

    def __str__(self):
        return f"'{self.param_name}' has no type. Please add a type annotation to the input parameter."


class FlyteMissingReturnValueException(FlyteCompilationException):
    _ERROR_CODE = "USER:MissingReturnValueError"

    def __str__(self):
        return f"{self.fn.__name__} function must return a value. Please add a return statement at the end of the function."
