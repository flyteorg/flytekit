import flyteidl_rust as flyteidl

from flytekit.models import common


class Schedule(common.FlyteIdlEntity):
    class FixedRateUnit(object):
        MINUTE = int(flyteidl.admin.FixedRateUnit.Minute)
        HOUR = int(flyteidl.admin.FixedRateUnit.Hour)
        DAY = int(flyteidl.admin.FixedRateUnit.Day)

        @classmethod
        def enum_to_string(cls, int_value):
            """
            :param int_value:
            :rtype: Text
            """
            if int_value == cls.MINUTE:
                return "MINUTE"
            elif int_value == cls.HOUR:
                return "HOUR"
            elif int_value == cls.DAY:
                return "DAY"
            else:
                return "{}".format(int_value)

    class FixedRate(common.FlyteIdlEntity):
        def __init__(self, value, unit):
            """
            :param int value:
            :param int unit: enum value from FixedRateUnit
            """
            self._value = value
            self._unit = unit

        @property
        def value(self):
            """
            :rtype: int
            """
            return self._value

        @property
        def unit(self):
            """
            :rtype: int
            """
            return self._unit

        def to_flyte_idl(self):
            """
            :rtype: flyteidl.admin.schedule_pb2.FixedRate
            """
            return flyteidl.admin.FixedRate(value=self.value, unit=self.unit)

        @classmethod
        def from_flyte_idl(cls, pb2_object):
            """
            :param flyteidl.admin.schedule_pb2.FixedRate pb2_object:
            :rtype: Schedule.FixedRate
            """
            return cls(pb2_object.value, pb2_object.unit)

    class CronSchedule(common.FlyteIdlEntity):
        def __init__(self, schedule, offset):
            """
            :param Text schedule: cron expression or aliases
            :param Text offset: ISO_8601 Duration
            """
            self._schedule = schedule
            self._offset = offset

        @property
        def schedule(self):
            """
            :rtype: Text
            """
            return self._schedule

        @property
        def offset(self):
            """
            :rtype: Text
            """
            return self._offset

        def to_flyte_idl(self):
            """
            :rtype: flyteidl.admin.schedule_pb2.FixedRate
            """
            return flyteidl.admin.CronSchedule(schedule=self.schedule, offset=self.offset)

        @classmethod
        def from_flyte_idl(cls, pb2_object):
            """
            :param flyteidl.admin.schedule_pb2.CronSchedule pb2_object:
            :rtype: Schedule.CronSchedule
            """
            return cls(pb2_object.schedule or None, pb2_object.offset or None)

    def __init__(self, kickoff_time_input_arg, cron_expression=None, rate=None, cron_schedule=None):
        """
        One of cron_expression or fixed rate must be specified.

        :param Text kickoff_time_input_arg:
        :param Text cron_expression: [Optional]
        :param Schedule.FixedRate rate: [Optional]
        :param Schedule.CronSchedule cron_schedule: [Optional]
        """
        self._kickoff_time_input_arg = kickoff_time_input_arg
        self._cron_expression = cron_expression
        self._rate = rate
        self._cron_schedule = cron_schedule

    @property
    def kickoff_time_input_arg(self):
        return self._kickoff_time_input_arg

    @property
    def cron_expression(self):
        """
        :rtype: Text
        """
        return self._cron_expression

    @property
    def rate(self):
        """
        :rtype: Schedule.FixedRate
        """
        return self._rate

    @property
    def cron_schedule(self):
        """
        :rtype: Schedule.CronSchedule
        """
        return self._cron_schedule

    @property
    def schedule_expression(self):
        return self.cron_expression or self.rate or self.cron_schedule

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.schedule_pb2.Schedule
        """
        schedule_expression = None
        if self.cron_expression:
            schedule_expression = flyteidl.schedule.ScheduleExpression.CronExpression(self.cron_expression)
        elif self.rate:
            schedule_expression = flyteidl.schedule.ScheduleExpression.Rate(self.rate.to_flyte_idl())
        elif self.cron_schedule:
            schedule_expression = flyteidl.schedule.ScheduleExpression.CronSchedule(self.cron_schedule.to_flyte_idl())

        return flyteidl.admin.Schedule(
            kickoff_time_input_arg=self.kickoff_time_input_arg,
            schedule_expression=schedule_expression,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.schedule_pb2.Schedule pb2_object:
        :rtype: Schedule
        """
        
        # Explicitly instantiate a Schedule model rather than a potential sub-class.
        return Schedule(
            pb2_object.kickoff_time_input_arg,
            cron_expression=pb2_object.schedule_expression[0] if isinstance(pb2_object.schedule_expression, flyteidl.schedule.ScheduleExpression.CronExpression) else None,
            rate=Schedule.FixedRate.from_flyte_idl(pb2_object.schedule_expression[0]) if isinstance(pb2_object.schedule_expression, flyteidl.schedule.ScheduleExpression.Rate) else None,
            cron_schedule=Schedule.CronSchedule.from_flyte_idl(pb2_object.schedule_expression[0]) if isinstance(pb2_object.schedule_expression, flyteidl.schedule.ScheduleExpression.CronSchedule) else None
        )
