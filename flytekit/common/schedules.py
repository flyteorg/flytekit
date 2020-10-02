import datetime as _datetime
import re as _re

import croniter as _croniter

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models import schedule as _schedule_models


class _ExtendedSchedule(_schedule_models.Schedule):
    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.admin.schedule_pb2.Schedule proto:
        :rtype: _ExtendedSchedule
        """
        return cls.promote_from_model(_schedule_models.Schedule.from_flyte_idl(proto))


class CronSchedule(_ExtendedSchedule, metaclass=_sdk_bases.ExtendedSdkType):
    _VALID_CRON_ALIASES = [
        "hourly",
        "hours",
        "@hourly",
        "daily",
        "days",
        "@daily",
        "weekly",
        "weeks",
        "@weekly",
        "monthly",
        "months",
        "@monthly",
        "annually",
        "@annually",
        "yearly",
        "years",
        "@yearly",
    ]

    # Not a perfect regex but good enough and simple to reason about
    _OFFSET_PATTERN = _re.compile("([-+]?)P([-+0-9YMWD]+)?(T([-+0-9HMS.,]+)?)?")

    def __init__(self, cron_expression=None, schedule=None, offset=None, kickoff_time_input_arg=None):
        """
        :param Text cron_expression:
        :param Text schedule:
        :param Text offset:
        :param Text kickoff_time_input_arg:
        """
        if cron_expression is None and schedule is None:
            raise _user_exceptions.FlyteAssertion("Either `cron_expression` or `schedule` should be specified.")

        if cron_expression is not None and offset is not None:
            raise _user_exceptions.FlyteAssertion("Only `schedule` is supported when specifying `offset`.")

        if cron_expression is not None:
            CronSchedule._validate_expression(cron_expression)

        if schedule is not None:
            CronSchedule._validate_schedule(schedule)

        if offset is not None:
            CronSchedule._validate_offset(offset)

        super(CronSchedule, self).__init__(
            kickoff_time_input_arg,
            cron_expression=cron_expression,
            cron_schedule=_schedule_models.Schedule.CronSchedule(schedule, offset) if schedule is not None else None,
        )

    @staticmethod
    def _validate_expression(cron_expression):
        """
        Ensures that the set value is a valid cron string.  We use the format used in Cloudwatch and the best
        explanation can be found here:
            https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html#CronExpressions
        :param Text cron_expression: cron expression
        """
        # We use the croniter lib to validate our cron expression.  Since on the admin side we use Cloudwatch,
        # we have a couple checks in order to line up Cloudwatch with Croniter.
        tokens = cron_expression.split()
        if len(tokens) != 6:
            raise _user_exceptions.FlyteAssertion(
                "Cron expression is invalid.  A cron expression must have 6 fields.  Cron expressions are in the "
                "format of: `minute hour day-of-month month day-of-week year`.  "
                "Use `schedule` for 5 fields cron expression.  Received: `{}`".format(cron_expression)
            )

        if tokens[2] != "?" and tokens[4] != "?":
            raise _user_exceptions.FlyteAssertion(
                "Scheduled string is invalid.  A cron expression must have a '?' for either day-of-month or "
                "day-of-week.  Please specify '?' for one of those fields.  Cron expressions are in the format of: "
                "minute hour day-of-month month day-of-week year.\n\n"
                "For more information: "
                "https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html#CronExpressions"
            )

        try:
            # Cut to 5 fields and just assume year field is good because croniter treats the 6th field as seconds.
            # TODO: Parse this field ourselves and check
            _croniter.croniter(" ".join(cron_expression.replace("?", "*").split()[:5]))
        except Exception:
            raise _user_exceptions.FlyteAssertion(
                "Scheduled string is invalid.  The cron expression was found to be invalid."
                " Provided cron expr: {}".format(cron_expression)
            )

    @staticmethod
    def _validate_schedule(schedule):
        if schedule.lower() not in CronSchedule._VALID_CRON_ALIASES:
            try:
                _croniter.croniter(schedule)
            except Exception:
                raise _user_exceptions.FlyteAssertion(
                    "Schedule is invalid. It must be set to either a cron alias or valid cron expression."
                    " Provided schedule: {}".format(schedule)
                )

    @staticmethod
    def _validate_offset(offset):
        if CronSchedule._OFFSET_PATTERN.fullmatch(offset) is None:
            raise _user_exceptions.FlyteAssertion(
                "Offset is invalid. It must be an ISO 8601 duration. Provided offset: {}".format(offset)
            )

    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param flytekit.models.schedule.Schedule base_model:
        :rtype: CronSchedule
        """
        return cls(
            cron_expression=base_model.cron_expression,
            schedule=base_model.cron_schedule.schedule if base_model.cron_schedule is not None else None,
            offset=base_model.cron_schedule.offset if base_model.cron_schedule is not None else None,
            kickoff_time_input_arg=base_model.kickoff_time_input_arg,
        )


class FixedRate(_ExtendedSchedule, metaclass=_sdk_bases.ExtendedSdkType):
    def __init__(self, duration, kickoff_time_input_arg=None):
        """
        :param datetime.timedelta duration:
        :param Text kickoff_time_input_arg:
        """
        super(FixedRate, self).__init__(kickoff_time_input_arg, rate=self._translate_duration(duration))

    @staticmethod
    def _translate_duration(duration):
        """
        :param datetime.timedelta duration: timedelta between runs
        :rtype: flytekit.models.schedule.Schedule.FixedRate
        """
        _SECONDS_TO_MINUTES = 60
        _SECONDS_TO_HOURS = _SECONDS_TO_MINUTES * 60
        _SECONDS_TO_DAYS = _SECONDS_TO_HOURS * 24

        if duration.microseconds != 0 or duration.seconds % _SECONDS_TO_MINUTES != 0:
            raise _user_exceptions.FlyteAssertion(
                "Granularity of less than a minute is not supported for FixedRate schedules.  Received: {}".format(
                    duration
                )
            )
        elif int(duration.total_seconds()) % _SECONDS_TO_DAYS == 0:
            return _schedule_models.Schedule.FixedRate(
                int(duration.total_seconds() / _SECONDS_TO_DAYS), _schedule_models.Schedule.FixedRateUnit.DAY,
            )
        elif int(duration.total_seconds()) % _SECONDS_TO_HOURS == 0:
            return _schedule_models.Schedule.FixedRate(
                int(duration.total_seconds() / _SECONDS_TO_HOURS), _schedule_models.Schedule.FixedRateUnit.HOUR,
            )
        else:
            return _schedule_models.Schedule.FixedRate(
                int(duration.total_seconds() / _SECONDS_TO_MINUTES), _schedule_models.Schedule.FixedRateUnit.MINUTE,
            )

    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param flytekit.models.schedule.Schedule base_model:
        :rtype: FixedRate
        """
        if base_model.rate.unit == _schedule_models.Schedule.FixedRateUnit.DAY:
            duration = _datetime.timedelta(days=base_model.rate.value)
        elif base_model.rate.unit == _schedule_models.Schedule.FixedRateUnit.HOUR:
            duration = _datetime.timedelta(hours=base_model.rate.value)
        else:
            duration = _datetime.timedelta(minutes=base_model.rate.value)

        return cls(duration, kickoff_time_input_arg=base_model.kickoff_time_input_arg)
