from __future__ import absolute_import

import six as _six

from flytekit.common.exceptions import scopes as _exception_scopes, user as _user_exceptions
from flytekit.common.tasks.mixins.executable_traits import common as _common
from flytekit.common.types import helpers as _type_helpers

try:
    from inspect import getfullargspec as _getargspec
except ImportError:
    from inspect import getargspec as _getargspec


class WrappedFunctionTask(_common.ExecutableTaskMixin):
    """
    This assumes to be mixed in with a flytekit.common.task.sdk_runnable.SdkRunnableTask
    """
    def __init__(self, task_function=None, **kwargs):
        """
        TODO:
        :param task_function:
        :param kwargs:
        """
        self._task_function = task_function
        super(WrappedFunctionTask, self).__init__(**kwargs)
        self.id._name = "{}.{}".format(self.task_function_module, self.task_function_name)

    @property
    def task_function(self):
        """
        TODO:
        :return:
        """
        return self._task_function

    @property
    def task_function_name(self):
        """
        :rtype: Text
        """
        return self.task_function.__name__

    @property
    def task_function_module(self):
        """
        :rtype: Text
        """
        return self._task_function.__module__

    def _validate_inputs(self, inputs):
        """
        Checks that the input exists in the function correctly.
        """
        super(WrappedFunctionTask, self)._validate_inputs(inputs)
        for k, v in _six.iteritems(inputs):
            if not self._is_argname_in_function_definition(k):
                raise _user_exceptions.FlyteValidationException(
                    "The input '{}' was not specified in the task function.  Therefore, this input cannot be "
                    "provided to the task.".format(v)
                )
            if _type_helpers.get_sdk_type_from_literal_type(v.type) in type(self)._banned_inputs:
                raise _user_exceptions.FlyteValidationException(
                    "The input '{}' is not an accepted input type.".format(v)
                )

    def _validate_outputs(self, outputs):
        """
        Checks if the output exists in the function correctly.
        """
        super(WrappedFunctionTask, self)._validate_outputs(outputs)
        for k, v in _six.iteritems(outputs):
            if not self._is_argname_in_function_definition(k):
                raise _user_exceptions.FlyteValidationException(
                    "The output named '{}' was not specified in the task function.  Therefore, this output cannot be "
                    "provided to the task."
                )
            if _type_helpers.get_sdk_type_from_literal_type(v.type) in type(self)._banned_outputs:
                raise _user_exceptions.FlyteValidationException(
                    "The output '{}' is not an accepted output type.".format(v)
                )

    def _get_kwarg_inputs(self):
        # Trim off first parameter as it is reserved for workflow_parameters
        return set(_getargspec(self.task_function).args[1:])

    def _is_argname_in_function_definition(self, key):
        return key in self._get_kwarg_inputs()

    def _missing_mapped_inputs_outputs(self):
        # Trim off first parameter as it is reserved for workflow_parameters
        args = self._get_kwarg_inputs()
        inputs_and_outputs = set(self.interface.outputs.keys()) | set(self.interface.inputs.keys())
        return args ^ inputs_and_outputs

    def _execute_user_code(self, context, vargs, inputs, outputs):
        full_args = inputs.copy()
        full_args.update(outputs)
        return _exception_scopes.user_entry_point(self.task_function)(*vargs, **full_args)
