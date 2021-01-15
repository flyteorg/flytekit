import abc as _abc
import datetime as _datetime
import logging as _logging
import sys as _sys
import time as _time
import traceback as _traceback

import six as _six


class Sensor(object, metaclass=_abc.ABCMeta):
    def __init__(self, evaluation_interval=None, max_failures=0):
        """
        :param datetime.timedelta evaluation_interval: This is the time to wait between evaluation attempts of this
            sensor. If the sensor takes longer to evaluate than the poll_interval, it will immediately begin
            evaluation again.
        :param int max_failures: This is the maximum number of failures that can happen while attempting to sense
            before perma-failing.
        """
        if evaluation_interval is None:
            evaluation_interval = _datetime.timedelta(seconds=30)
        self._evaluation_interval = evaluation_interval
        self._max_failures = max_failures
        self._failures = 0
        self._exc_info = None
        self._last_executed_time = _datetime.datetime(year=1990, month=6, day=30)  # Arbitrary date in the past.
        self._sensed = False

    @_abc.abstractmethod
    def _do_poll(self):
        """
        :rtype: (bool, Optional[datetime.timedelta])
        """
        pass

    def sense_with_wait_hint(self):
        """
        Attempts to sense based on the lambda expression.  The method will return the last sensed result.  If the
        rate of sensing is exceeded for the sensor, the timedelta in the returned tuple will tell the caller how long it
        should sleep before trying again.
        :rtype: (bool, Optional[datetime.timedelta])
        """
        # Return cached success.  This simplifies code for the conditional sensors.
        if self._sensed:
            return self._sensed, self._evaluation_interval

        # Perma-fail to prevent abuse of sensed objects.
        if self._failures > self._max_failures:
            _six.reraise(*self._exc_info)

        now = _datetime.datetime.utcnow()

        time_to_wait_eval_period = self._evaluation_interval - (now - self._last_executed_time)
        if time_to_wait_eval_period > _datetime.timedelta():
            return self._sensed, time_to_wait_eval_period

        try:
            self._sensed, time_to_wait = self._do_poll()
            time_to_wait = time_to_wait or self._evaluation_interval
        except BaseException:
            self._failures += 1
            self._exc_info = _sys.exc_info()
            if self._failures > self._max_failures:
                _logging.error(
                    "{} failed (with no remaining retries) due to:\n\n{}".format(self, _traceback.format_exc()),
                )
                raise
            else:
                _logging.warn("{} failed (but will retry) due to:\n\n{}".format(self, _traceback.format_exc()))
            time_to_wait = self._evaluation_interval

        self._last_executed_time = _datetime.datetime.utcnow()

        return self._sensed, time_to_wait

    def sense(self, timeout=None):
        """
        Attempts
        :param datetime.timedelta timeout:
        :rtype: bool
        """
        started = _datetime.datetime.utcnow()
        while True:
            sensed, time_to_wait = self.sense_with_wait_hint()
            if sensed:
                return True
            if time_to_wait:
                _time.sleep(time_to_wait.total_seconds())
            if timeout is not None and (_datetime.datetime.utcnow() - started) > timeout:
                return False
