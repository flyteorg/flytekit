from __future__ import absolute_import
import abc as _abc
import inspect as _inspect
import six as _six
import importlib as _importlib
import logging as _logging

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common import utils as _utils


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
    def _find_instance_module():
        frame = _inspect.currentframe()
        while frame:
            if frame.f_code.co_name == '<module>':
                return frame.f_globals['__name__']
            frame = frame.f_back
        return None

    def __call__(cls, *args, **kwargs):
        o = super(_InstanceTracker, cls).__call__(*args, **kwargs)
        o._instantiated_in = _InstanceTracker._find_instance_module()
        return o


class RegisterableEntity(_six.with_metaclass(_InstanceTracker, object)):

    def __init__(self, *args, **kwargs):
        self._platform_valid_name = None
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
    def has_valid_name(self):
        """
        :rtype: bool
        """
        return self._platform_valid_name is not None and self._platform_valid_name != ""

    @property
    def platform_valid_name(self):
        """
        :rtype: Text
        """
        return self._platform_valid_name

    def auto_assign_name(self):
        """
        This function is a bit of trickster Python code that goes hand in hand with the _InstanceTracker metaclass
        defined above. Thanks @matthewphsmith for this bit of ingenuity.

        For instance, if a user has code that looks like this:

            from some.other.module import wf
            my_launch_plan = wf.create_launch_plan()

            @dynamic_task
            def sample_task(wf_params):
                yield my_launch_plan()

        This code means that we should have a launch plan with a name ending in "my_launch_plan", since that is the
        name of the variable that the created launch plan gets assigned to. That is also the name that the launch plan
        would be registered with.

        However, when the create_launch_plan() function runs, the Python interpreter has no idea where the created
        object will be assigned to. It has no idea that the output of the create_launch_plan call is to be paired up
        with a variable named "my_launch_plan". This function basically does this after the fact. Leveraging the
        _instantiated_in field provided by the _InstanceTracker class above, this code will re-import the
        module (ie Python file) that the object is in. Since it's already loaded, it's just retrieved from memory.
        It then scans all objects in the module, and when an object match is found, it knows it's found the right
        variable name.

        Just to drive the point home, this function is mostly needed for Launch Plans. Assuming that user code has:

            @python_task
            def some_task()

        When Flytekit calls the module loader and loads the task, the name of the task is the name of the function
        itself.  It's known at time of creation. In contrast, when

            xyz = SomeWorflow.create_launch_plan()

        is called, the name of the launch plan isn't known until after creation, it's not "SomeWorkflow", it's "xyz"
        """
        _logging.debug("Running name auto assign")
        m = _importlib.import_module(self.instantiated_in)

        for k in dir(m):
            if getattr(m, k) == self:
                self._platform_valid_name = _utils.fqdn(m.__name__, k, entity_type=self.resource_type)
                _logging.debug("Auto-assigning name to {}".format(self._platform_valid_name))
                return

        _logging.error("Could not auto-assign name")
        raise _system_exceptions.FlyteSystemException("Error looking for object while auto-assigning name.")
