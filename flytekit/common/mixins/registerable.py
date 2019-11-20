from __future__ import absolute_import
import abc as _abc
import inspect as _inspect
import os as _os
import six as _six
from flytekit.common import sdk_bases as _sdk_bases


class _InstanceTracker(_sdk_bases.ExtendedSdkType):
    """
    This is either genius or terrible.  Some of our tools iterate over modules and try to find Flyte entities
    (Tasks, Workflows, Launch Plans) and then register them.  However, if a task is imported via a command like:

        from package.module import some_task

    It is possible we will find a task reference twice, but then how do we know where it was defined?  Ideally, we would
    like to only register a task once and do so with the name where it is defined.  This metaclass allows us to do this
    by inspecting the call stack when __call__ is called on the metaclass (thus instantiating an object).
    """
    @staticmethod
    def _find_instance_module_and_file():
        frame = _inspect.currentframe()
        while frame:
            if frame.f_code.co_name == '<module>':
                return frame.f_globals['__name__'], _os.path.abspath(frame.f_globals['__file__'])
            frame = frame.f_back
        return None, None

    def __call__(cls, *args, **kwargs):
        instantiated_in, instantiated_in_file = _InstanceTracker._find_instance_module_and_file()
        return super(_InstanceTracker, cls).__call__(
            _instantiated_in=instantiated_in,
            _instantiated_in_file=instantiated_in_file,
            *args,
            **kwargs
        )


class RegisterableEntity(_six.with_metaclass(_InstanceTracker, object)):

    def __init__(self, _instantiated_in=None, _instantiated_in_file=None, *args, **kwargs):
        self._instantiated_in = _instantiated_in
        self._instantiated_in_file = _instantiated_in_file
        super(RegisterableEntity, self).__init__(*args, **kwargs)

    @_abc.abstractmethod
    def register(self, project, domain, name, version):
        """
        :param Text project: The project in which to register this task.
        :param Text domain: The domain in which to register this task.
        :param Text name: The name to give this task.
        :param Text version: The version in which to register this task.
        """
        pass

    @_abc.abstractproperty
    def resource_type(self):
        """
        Integer from _identifier.ResourceType enum
        :rtype: int
        """
        pass

    @_abc.abstractproperty
    def entity_type_text(self):
        """
        :rtype: Text
        """
        pass

    @property
    def upstream_entities(self):
        """
        Task, workflow, and launch plan that need to be registered in advance of this workflow.
        :rtype: set[RegisterableEntity]
        """
        return self._upstream_entities

    @property
    def instantiated_in(self):
        """
        If found, we try to specify the module where the task was first instantiated.
        :rtype: Optional[Text]
        """
        return self._instantiated_in

    @property
    def instantiated_in_file(self):
        """
        If found, we try to specify the file where the task was first instantiated.
        :rtype: Optional[Text]
        """
        return self._instantiated_in_file
