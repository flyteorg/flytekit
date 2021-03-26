from sys import exc_info as _exc_info
from traceback import format_tb as _format_tb

from six import reraise as _reraise
from wrapt import decorator as _decorator

from flytekit.common.exceptions import base as _base_exceptions
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models.core import errors as _error_model


class FlyteScopedException(Exception):
    def __init__(self, context, exc_type, exc_value, exc_tb, top_trim=0, bottom_trim=0, kind=None):
        self._exc_type = exc_type
        self._exc_value = exc_value
        self._exc_tb = exc_tb
        self._top_trim = top_trim
        self._bottom_trim = bottom_trim
        self._context = context
        self._kind = kind
        super(FlyteScopedException, self).__init__(str(self.value))

    @property
    def verbose_message(self):
        tb = self.traceback
        to_trim = self._top_trim
        while to_trim > 0 and tb.tb_next is not None:
            tb = tb.tb_next

        top_tb = tb
        limit = 0
        while tb is not None:
            limit += 1
            tb = tb.tb_next
        limit = max(0, limit - self._bottom_trim)

        lines = _format_tb(top_tb, limit=limit)
        lines = [line.rstrip() for line in lines]
        lines = "\n".join(lines).split("\n")
        traceback_str = "\n    ".join([""] + lines)

        format_str = "Traceback (most recent call last):\n" "{traceback}\n" "\n" "Message:\n" "\n" "    {message}"
        return format_str.format(traceback=traceback_str, message=str(self.value))

    def __str__(self):
        return str(self.value)

    @property
    def value(self):
        if isinstance(self._exc_value, FlyteScopedException):
            return self._exc_value.value
        return self._exc_value

    @property
    def traceback(self):
        if isinstance(self._exc_value, FlyteScopedException):
            return self._exc_value.traceback
        return self._exc_tb

    @property
    def type(self):
        if isinstance(self._exc_value, FlyteScopedException):
            return self._exc_value.type
        return self._exc_type

    @property
    def error_code(self):
        """
        :rtype: Text
        """
        if isinstance(self._exc_value, FlyteScopedException):
            return self._exc_value.error_code

        if hasattr(type(self._exc_value), "error_code"):
            return type(self._exc_value).error_code
        return "{}:Unknown".format(self._context)

    @property
    def kind(self):
        """
        :rtype: int
        """
        if self._kind is not None:
            # If kind is overriden, return it.
            return self._kind
        elif isinstance(self._exc_value, FlyteScopedException):
            # Otherwise, go lower in the scope to find the kind of exception.
            return self._exc_value.kind
        elif isinstance(self._exc_value, _base_exceptions.FlyteRecoverableException):
            # If it is an exception that is recoverable, we return it as such.
            return _error_model.ContainerError.Kind.RECOVERABLE
        else:
            # The remaining exceptions are considered unrecoverable.
            return _error_model.ContainerError.Kind.NON_RECOVERABLE


class FlyteScopedSystemException(FlyteScopedException):
    def __init__(self, exc_type, exc_value, exc_tb, **kwargs):
        super(FlyteScopedSystemException, self).__init__("SYSTEM", exc_type, exc_value, exc_tb, **kwargs)

    @property
    def verbose_message(self):
        """
        :rtype: Text
        """
        base_msg = super(FlyteScopedSystemException, self).verbose_message
        base_msg += "\n\nSYSTEM ERROR! Contact platform administrators."
        return base_msg


class FlyteScopedUserException(FlyteScopedException):
    def __init__(self, exc_type, exc_value, exc_tb, **kwargs):
        super(FlyteScopedUserException, self).__init__("USER", exc_type, exc_value, exc_tb, **kwargs)

    @property
    def verbose_message(self):
        """
        :rtype: Text
        """
        base_msg = super(FlyteScopedUserException, self).verbose_message
        base_msg += "\n\nUser error."
        return base_msg


_NULL_CONTEXT = 0
_USER_CONTEXT = 1
_SYSTEM_CONTEXT = 2

# Keep the stack with a null-context so we never have to range check when peeking back.
_CONTEXT_STACK = [_NULL_CONTEXT]


def _is_base_context():
    return _CONTEXT_STACK[-2] == _NULL_CONTEXT


@_decorator
def system_entry_point(wrapped, instance, args, kwargs):
    """
    Decorator for wrapping functions that enter a system context.  This should decorate every method a user might
    call.  This will allow us to add differentiation between what is a user error and what is a system failure.
    Furthermore, we will clean the exception trace so as to make more sense to the user--allowing them to know if they
    should take action themselves or pass on to the platform owners.  We will dispatch metrics and such appropriately.
    """
    try:
        _CONTEXT_STACK.append(_SYSTEM_CONTEXT)
        if _is_base_context():
            try:
                return wrapped(*args, **kwargs)
            except FlyteScopedException as ex:
                _reraise(ex.type, ex.value, ex.traceback)
        else:
            try:
                return wrapped(*args, **kwargs)
            except FlyteScopedException:
                # Just pass-on the exception that is already wrapped and scoped
                _reraise(*_exc_info())
            except _user_exceptions.FlyteUserException:
                # Re-raise from here.
                _reraise(
                    FlyteScopedUserException,
                    FlyteScopedUserException(*_exc_info()),
                    _exc_info()[2],
                )
            except Exception:
                # System error, raise full stack-trace all the way up the chain.
                _reraise(
                    FlyteScopedSystemException,
                    FlyteScopedSystemException(*_exc_info(), kind=_error_model.ContainerError.Kind.RECOVERABLE),
                    _exc_info()[2],
                )
    finally:
        _CONTEXT_STACK.pop()


@_decorator
def user_entry_point(wrapped, instance, args, kwargs):
    """
    Decorator for wrapping functions that enter into a user context.  This will help us differentiate user-created
    failures even when it is re-entrant into system code.

    Note: a user_entry_point can ONLY ever be called from within a @system_entry_point wrapped function, therefore,
    we can always ensure we will hit a system_entry_point to correctly reformat our exceptions.  Also, any exception
    we create here will only be handled within our system code so we don't need to worry about leaking weird exceptions
    to the user.
    """
    try:
        _CONTEXT_STACK.append(_USER_CONTEXT)
        if _is_base_context():
            try:
                return wrapped(*args, **kwargs)
            except FlyteScopedException as ex:
                _reraise(ex.type, ex.value, ex.traceback)
        else:
            try:
                return wrapped(*args, **kwargs)
            except FlyteScopedException:
                # Just pass on the already wrapped and scoped exception
                _reraise(*_exc_info())
            except _user_exceptions.FlyteUserException:
                _reraise(
                    FlyteScopedUserException,
                    FlyteScopedUserException(*_exc_info()),
                    _exc_info()[2],
                )
            except _system_exceptions.FlyteSystemException:
                _reraise(
                    FlyteScopedSystemException,
                    FlyteScopedSystemException(*_exc_info()),
                    _exc_info()[2],
                )
            except Exception:
                # Any non-platform raised exception is a user exception.
                # This will also catch FlyteUserException re-raised by the system_entry_point handler
                _reraise(
                    FlyteScopedUserException,
                    FlyteScopedUserException(*_exc_info()),
                    _exc_info()[2],
                )
    finally:
        _CONTEXT_STACK.pop()
