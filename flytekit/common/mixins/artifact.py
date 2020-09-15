import abc as _abc
import datetime as _datetime
import time as _time

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models import common as _common_models


class ExecutionArtifact(object, metaclass=_common_models.FlyteABCMeta):
    @_abc.abstractproperty
    def inputs(self):
        """
        Returns the inputs to the execution in the standard Python format as dictated by the type engine.
        :rtype:  dict[Text, T]
        """
        pass

    @_abc.abstractproperty
    def outputs(self):
        """
        Returns the outputs to the execution in the standard Python format as dictated by the type engine.  If the
        execution ended in error or the execution is in progress, an exception will be raised.
        :rtype:  dict[Text, T]
        """
        pass

    @_abc.abstractproperty
    def error(self):
        """
        If execution is in progress, raise an exception.  Otherwise, return None if no error was present upon
        reaching completion.
        :rtype: flytekit.models.core.execution.ExecutionError or None
        """
        pass

    @_abc.abstractproperty
    def is_complete(self):
        """
        Dictates whether or not the execution is complete.
        :rtype: bool
        """
        pass

    @_abc.abstractmethod
    def sync(self):
        """
        Syncs the state of the underlying execution artifact with the state observed by the platform.
        :rtype: None
        """
        pass

    @_abc.abstractmethod
    def _sync_closure(self):
        """
        Syncs the closure of the underlying execution artifact with the state observed by the platform.
        :rtype: None
        """
        pass

    def wait_for_completion(self, timeout=None, poll_interval=None):
        """
        :param datetime.timedelta timeout: Amount of time to wait until the execution has completed before timing
            out. If not set or set to None, this method will wait for infinite.
        :param datetime.timedelta poll_interval: Duration to wait between polling for a completion update.
        :rtype: None
        """
        poll_interval = poll_interval or _datetime.timedelta(seconds=30)
        if timeout is None:
            time_to_give_up = _datetime.datetime.max
        else:
            time_to_give_up = _datetime.datetime.utcnow() + timeout

        self._sync_closure()
        while _datetime.datetime.utcnow() < time_to_give_up:
            if self.is_complete:
                self.sync()
                return
            _time.sleep(poll_interval.total_seconds())
            self._sync_closure()
        raise _user_exceptions.FlyteTimeout("Execution {} did not complete before timeout.".format(self))
