from __future__ import absolute_import
import abc as _abc
import six as _six


class ExecutableTaskMixin(_six.with_metclass(_abc.ABCMeta, object)):
    """
    The following interface must be defined in order to mixin with a SdkRunnableTask. It is also possible to override
    SdkRunnableTask methods and attributes (SdkRunnableTask will be reached by super()), but should be done with care.
    """

    @_abc.abstractmethod
    def _execute_user_code(self, context, vargs, inputs, outputs):
        """
        Mixins override this method to determine how to execute the user-provided code.

        :param flytekit.engines.common.EngineContext context:
        :param list[Any] vargs: Positional args to be provided.
        :param dict[Text,Any] inputs: This is the unpacked values for inputs to user code as defined by the type
            engine.
        :param dict[Text,OutputReferences] outputs: The outputs to be set by user code.
        :rtype: Any: the returned object from user code.
        """
        pass
