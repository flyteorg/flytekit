import abc as _abc
import importlib as _importlib
import inspect as _inspect
import logging as _logging
from typing import Set

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common import utils as _utils
from flytekit.common.exceptions import system as _system_exceptions


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
            if frame.f_code.co_name == "<module>":
                return frame.f_globals["__name__"]
            frame = frame.f_back
        return None

    def __call__(cls, *args, **kwargs):
        o = super(_InstanceTracker, cls).__call__(*args, **kwargs)
        o._instantiated_in = _InstanceTracker._find_instance_module()
        return o


class FlyteEntity(object, metaclass=_sdk_bases.ExtendedSdkType):
    @property
    @_abc.abstractmethod
    def resource_type(self):
        """
        Integer from _identifier.ResourceType enum
        :rtype: int
        """
        pass

    @property
    @_abc.abstractmethod
    def entity_type_text(self):
        """
        TODO: Rename to resource type text
        :rtype: Text
        """
        pass


class TrackableEntity(FlyteEntity, metaclass=_InstanceTracker):
    def __init__(self, *args, **kwargs):
        self._platform_valid_name = None
        super(TrackableEntity, self).__init__(*args, **kwargs)

    @property
    def instantiated_in(self):
        """
        If found, we try to specify the module where the task was first instantiated.
        :rtype: Optional[Text]
        """
        return self._instantiated_in  # Set in metaclass

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

    def assign_name(self, name):
        self._platform_valid_name = name

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

            xyz = SomeWorkflow.create_launch_plan()

        is called, the name of the launch plan isn't known until after creation, it's not "SomeWorkflow", it's "xyz"
        """
        _logging.debug("Running name auto assign")
        m = _importlib.import_module(self.instantiated_in)

        for k in dir(m):
            try:
                if getattr(m, k) is self:
                    self._platform_valid_name = _utils.fqdn(m.__name__, k, entity_type=self.resource_type)
                    _logging.debug("Auto-assigning name to {}".format(self._platform_valid_name))
                    return
            except ValueError as err:
                # Empty pandas dataframes behave weirdly here such that calling `m.df` raises:
                # ValueError: The truth value of a {type(self).__name__} is ambiguous. Use a.empty, a.bool(), a.item(),
                #   a.any() or a.all()
                # Since dataframes aren't registrable entities to begin with we swallow any errors they raise and
                # continue looping through m.
                _logging.warning("Caught ValueError {} while attempting to auto-assign name".format(err))
                pass

        _logging.error("Could not auto-assign name")
        raise _system_exceptions.FlyteSystemException("Error looking for object while auto-assigning name.")


class RegisterableEntity(TrackableEntity):
    def __init__(self, *args, **kwargs):
        self._has_registered = False
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

    @_abc.abstractmethod
    def serialize(self):
        """
        Registerable entities also are required to be serialized. This allows flytekit to separate serialization from
        the network call to Admin (mostly at least, if a Launch Plan is fetched for instance as part of another
        workflow, it will still hit Admin).
        """
        pass

    @property
    def has_registered(self) -> bool:
        return self._has_registered


class HasDependencies(object):
    """
    This interface is meant to describe Flyte entities that can have upstream dependencies. For instance, currently a
    launch plan depends on the underlying workflow, and a workflow is dependent on its tasks, and other launch plans,
    and subworkflows.
    """

    def __init__(self, *args, **kwargs):
        self._upstream_entities = set()
        super(HasDependencies, self).__init__(*args, **kwargs)

    @property
    def upstream_entities(self) -> Set[RegisterableEntity]:
        """
        Task, workflow, and launch plan that need to be registered in advance of this workflow.
        :rtype: set[RegisterableEntity]
        """
        return self._upstream_entities
