from __future__ import absolute_import

from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks.mixins.executable_traits import common as _common


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
        self.id._name = "{}.{}".format(self.task_module, self.task_function_name)

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
    def task_module(self):
        """
        :rtype: Text
        """
        return self._task_function.__module__

    def _execute_user_code(self, context, vargs, inputs, outputs):
        """
        See
        """
        full_args = inputs.copy()
        full_args.update(outputs)
        return _exception_scopes.user_entry_point(self.task_function)(*vargs, **full_args)
