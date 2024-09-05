import pytest

from flytekit.core import context_manager
from flytekit.exceptions import scopes, system, user
from flytekit.exceptions.scopes import system_error_handler, user_error_handler
from flytekit.exceptions.system import FlyteNonRecoverableSystemException
from flytekit.exceptions.user import FlyteUserRuntimeException
from flytekit.models.core import errors as _error_models


@scopes.user_entry_point
def _user_func(ex_to_raise):
    raise ex_to_raise


@scopes.system_entry_point
def _system_func(ex_to_raise):
    raise ex_to_raise


def test_base_scope():
    with pytest.raises(ValueError) as e:
        _user_func(ValueError("Bad value"))
    assert "Bad value" in str(e.value)

    with pytest.raises(ValueError) as e:
        _system_func(ValueError("Bad value"))
    assert "Bad value" in str(e.value)

    with pytest.raises(user.FlyteAssertion) as e:
        _user_func(user.FlyteAssertion("Bad assert"))
    assert "Bad assert" in str(e.value)

    with pytest.raises(system.FlyteSystemAssertion) as e:
        _user_func(system.FlyteSystemAssertion("Bad assert"))
    assert "Bad assert" in str(e.value)

    with pytest.raises(user.FlyteAssertion) as e:
        _system_func(user.FlyteAssertion("Bad assert"))
    assert "Bad assert" in str(e.value)

    with pytest.raises(system.FlyteSystemAssertion) as e:
        _system_func(system.FlyteSystemAssertion("Bad assert"))
    assert "Bad assert" in str(e.value)


@scopes.user_entry_point
def test_intercepted_scope_non_flyte_exception():
    value_error = ValueError("Bad value")
    with pytest.raises(scopes.FlyteScopedUserException) as e:
        _user_func(value_error)

    e = e.value
    assert e.value == value_error
    assert "Bad value" in e.verbose_message
    assert "User error." in e.verbose_message
    assert "ValueError:" in e.verbose_message
    assert e.error_code == "USER:Unknown"
    assert e.kind == _error_models.ContainerError.Kind.NON_RECOVERABLE

    with pytest.raises(scopes.FlyteScopedSystemException) as e:
        _system_func(value_error)

    e = e.value
    assert e.value == value_error
    assert "Bad value" in e.verbose_message
    assert "SYSTEM ERROR!" in e.verbose_message
    assert "ValueError:" in e.verbose_message
    assert e.error_code == "SYSTEM:Unknown"
    assert e.kind == _error_models.ContainerError.Kind.RECOVERABLE


@scopes.user_entry_point
def test_intercepted_scope_flyte_user_exception():
    assertion_error = user.FlyteAssertion("Bad assert")
    with pytest.raises(scopes.FlyteScopedUserException) as e:
        _user_func(assertion_error)

    e = e.value
    assert e.value == assertion_error
    assert "Bad assert" in e.verbose_message
    assert "User error." in e.verbose_message
    assert "FlyteAssertion:" in e.verbose_message
    assert e.error_code == "USER:AssertionError"
    assert e.kind == _error_models.ContainerError.Kind.NON_RECOVERABLE

    with pytest.raises(scopes.FlyteScopedUserException) as e:
        _system_func(assertion_error)

    e = e.value
    assert e.value == assertion_error
    assert "Bad assert" in e.verbose_message
    assert "User error." in e.verbose_message
    assert "FlyteAssertion:" in e.verbose_message
    assert e.error_code == "USER:AssertionError"
    assert e.kind == _error_models.ContainerError.Kind.NON_RECOVERABLE


@scopes.user_entry_point
def test_intercepted_scope_flyte_system_exception():
    assertion_error = system.FlyteSystemAssertion("Bad assert")
    with pytest.raises(scopes.FlyteScopedSystemException) as e:
        _user_func(assertion_error)

    e = e.value
    assert e.value == assertion_error
    assert "Bad assert" in e.verbose_message
    assert "SYSTEM ERROR!" in e.verbose_message
    assert "FlyteSystemAssertion:" in e.verbose_message
    assert e.kind == _error_models.ContainerError.Kind.RECOVERABLE
    assert e.error_code == "SYSTEM:AssertionError"

    with pytest.raises(scopes.FlyteScopedSystemException) as e:
        _system_func(assertion_error)

    e = e.value
    assert e.value == assertion_error
    assert "Bad assert" in e.verbose_message
    assert "SYSTEM ERROR!" in e.verbose_message
    assert "FlyteSystemAssertion:" in e.verbose_message
    assert e.error_code == "SYSTEM:AssertionError"
    assert e.kind == _error_models.ContainerError.Kind.RECOVERABLE


def test_system_error_handler():
    def t1():
        raise ValueError("Bad value")

    ctx = context_manager.FlyteContext.current_context()

    with pytest.raises(FlyteNonRecoverableSystemException) as e:
        system_error_handler(t1)()

    assert isinstance(e.value.value, ValueError)

    with pytest.raises(ValueError):
        with context_manager.FlyteContextManager.with_context(
                ctx.with_execution_state(
                    ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.LOCAL_TASK_EXECUTION)
                )
        ):
            system_error_handler(t1)()


def test_user_error_handler():
    def t1():
        raise ValueError("Bad value")

    with pytest.raises(FlyteUserRuntimeException) as e:
        user_error_handler(t1)()

    assert isinstance(e.value.value, ValueError)

    ctx = context_manager.FlyteContext.current_context()
    with pytest.raises(ValueError):
        with context_manager.FlyteContextManager.with_context(
                ctx.with_execution_state(
                    ctx.execution_state.with_params(mode=context_manager.ExecutionState.Mode.LOCAL_TASK_EXECUTION)
                )
        ):
            system_error_handler(t1)()