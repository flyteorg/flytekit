from __future__ import absolute_import

import importlib as _importlib
import logging as _logging

import six as _six

from flytekit.common import utils as _utils
from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.mixins.registerable import _InstanceTracker


class SelfNaming(_six.with_metaclass(_InstanceTracker, object)):

    def __init__(self, *args, **kwargs):
        self._platform_valid_name = None
        super(SelfNaming, self).__init__(*args, **kwargs)

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

            xyz = SomeWorkflow.create_launch_plan()

        is called, the name of the launch plan isn't known until after creation, it's not "SomeWorkflow", it's "xyz"
        """
        _logging.debug("Running name auto assign")
        m = _importlib.import_module(self.instantiated_in)

        for k in dir(m):
            try:
                if getattr(m, k) == self:
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
