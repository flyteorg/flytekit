import six as _six

from flyteidl.admin import common_pb2 as _common_pb2

from flytekit.models import common as _common


class Sort(_common.FlyteIdlEntity):
    class Direction(object):
        DESCENDING = _common_pb2.Sort.DESCENDING
        ASCENDING = _common_pb2.Sort.ASCENDING

    def __init__(self, key, direction):
        """
        :param Text key: field to sort on
        :param int direction: From flytekit.models.admin.common.Sort.Direction enum
        """
        self._key = key
        self._direction = direction

    @property
    def key(self):
        """
        :rtype: Text
        """
        return self._key

    @property
    def direction(self):
        """
        :rtype: int
        """
        return self._direction

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.common_pb2.Sort
        """
        return _common_pb2.Sort(key=self.key, direction=self.direction)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.common_pb2.Sort pb2_object:
        :rtype: Sort
        """
        return cls(key=pb2_object.key, direction=pb2_object.direction)

    @classmethod
    def from_python_std(cls, text):
        """
        :param Text text:
        :rtype: Sort
        """
        text = text.strip()
        if text[-1] != ")":
            raise ValueError(
                "Could not parse string.  Must be in format 'asc(key)' or 'desc(key)'.  '{}' did not "
                "end with ')'.".format(text)
            )
        if text.startswith("asc("):
            direction = Sort.Direction.ASCENDING
            key = text[len("asc(") : -1].strip()
        elif text.startswith("desc("):
            direction = Sort.Direction.DESCENDING
            key = text[len("desc(") : -1].strip()
        else:
            raise ValueError(
                "Could not parse string.  Must be in format 'asc(key)' or 'desc(key)'.  '{}' did not "
                "start with 'asc(' or 'desc'.".format(text)
            )
        return cls(key=key, direction=direction)


class EmailNotification(_common.FlyteIdlEntity):
    def __init__(self, recipients_email):
        """
        :param list[Text] recipients_email:
        """
        self._recipients_email = recipients_email

    @property
    def recipients_email(self):
        """
        :rtype: list[Text]
        """
        return self._recipients_email

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.common_pb2.EmailNotification
        """
        return _common_pb2.EmailNotification(recipients_email=self.recipients_email)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.common_pb2.EmailNotification pb2_object:
        :rtype: EmailNotification
        """
        return cls(pb2_object.recipients_email)


class SlackNotification(_common.FlyteIdlEntity):
    def __init__(self, recipients_email):
        """
        :param list[Text] recipients_email:
        """
        self._recipients_email = recipients_email

    @property
    def recipients_email(self):
        """
        :rtype: list[Text]
        """
        return self._recipients_email

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.common_pb2.SlackNotification
        """
        return _common_pb2.SlackNotification(recipients_email=self.recipients_email)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.common_pb2.SlackNotification pb2_object:
        :rtype: EmailNotification
        """
        return cls(pb2_object.recipients_email)


class PagerDutyNotification(_common.FlyteIdlEntity):
    def __init__(self, recipients_email):
        """
        :param list[Text] recipients_email:
        """
        self._recipients_email = recipients_email

    @property
    def recipients_email(self):
        """
        :rtype: list[Text]
        """
        return self._recipients_email

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.common_pb2.PagerDutyNotification
        """
        return _common_pb2.PagerDutyNotification(recipients_email=self.recipients_email)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.common_pb2.PagerDutyNotification pb2_object:
        :rtype: EmailNotification
        """
        return cls(pb2_object.recipients_email)


class Notification(_common.FlyteIdlEntity):
    def __init__(
        self,
        phases,
        email: EmailNotification = None,
        pager_duty: PagerDutyNotification = None,
        slack: SlackNotification = None,
    ):
        """
        Represents a structure for notifications based on execution status.
        :param list[int] phases: A list of phases to which users can associate the notifications.
        :param EmailNotification email: [Optional] Specify this for an email notification.
        :param PagerDutyNotification email: [Optional] Specify this for a PagerDuty notification.
        :param SlackNotification email: [Optional] Specify this for a Slack notification.
        """
        self._phases = phases
        self._email = email
        self._pager_duty = pager_duty
        self._slack = slack

    @property
    def phases(self):
        """
        A list of phases to which users can associate the notifications.
        :rtype: list[int]
        """
        return self._phases

    @property
    def email(self):
        """
        :rtype: EmailNotification
        """
        return self._email

    @property
    def pager_duty(self):
        """
        :rtype: PagerDutyNotification
        """
        return self._pager_duty

    @property
    def slack(self):
        """
        :rtype: SlackNotification
        """
        return self._slack

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.common_pb2.Notification
        """
        return _common_pb2.Notification(
            phases=self.phases,
            email=self.email.to_flyte_idl() if self.email else None,
            pager_duty=self.pager_duty.to_flyte_idl() if self.pager_duty else None,
            slack=self.slack.to_flyte_idl() if self.slack else None,
        )

    @classmethod
    def from_flyte_idl(cls, p):
        """
        :param flyteidl.admin.common_pb2.Notification p:
        :rtype: Notification
        """
        return cls(
            p.phases,
            email=EmailNotification.from_flyte_idl(p.email) if p.HasField("email") else None,
            pager_duty=PagerDutyNotification.from_flyte_idl(p.pager_duty) if p.HasField("pager_duty") else None,
            slack=SlackNotification.from_flyte_idl(p.slack) if p.HasField("slack") else None,
        )


class Labels(_common.FlyteIdlEntity):
    def __init__(self, values):
        """
        Label values to be applied to a workflow execution resource.

        :param dict[Text, Text] values:
        """
        self._values = values

    @property
    def values(self):
        return self._values

    def to_flyte_idl(self):
        """
        :rtype: dict[Text, Text]
        """
        return _common_pb2.Labels(values={k: v for k, v in _six.iteritems(self.values)})

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.common_pb2.Labels pb2_object:
        :rtype: Labels
        """
        return cls({k: v for k, v in _six.iteritems(pb2_object.values)})


class Annotations(_common.FlyteIdlEntity):
    def __init__(self, values):
        """
        Annotation values to be applied to a workflow execution resource.

        :param dict[Text, Text] values:
        """
        self._values = values

    @property
    def values(self):
        return self._values

    def to_flyte_idl(self):
        """
        :rtype: _common_pb2.Annotations
        """
        return _common_pb2.Annotations(values={k: v for k, v in _six.iteritems(self.values)})

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.admin.common_pb2.Annotations pb2_object:
        :rtype: Annotations
        """
        return cls({k: v for k, v in _six.iteritems(pb2_object.values)})


class UrlBlob(_common.FlyteIdlEntity):
    def __init__(self, url, bytes):
        """
        :param Text url:
        :param int bytes:
        """
        self._url = url
        self._bytes = bytes

    @property
    def url(self):
        """
        :rtype: Text
        """
        return self._url

    @property
    def bytes(self):
        """
        :rtype: int
        """
        return self._bytes

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.common_pb2.UrlBlob
        """
        return _common_pb2.UrlBlob(url=self.url, bytes=self.bytes)

    @classmethod
    def from_flyte_idl(cls, pb):
        """
        :param flyteidl.admin.common_pb2.UrlBlob pb:
        :rtype: UrlBlob
        """
        return cls(url=pb.url, bytes=pb.bytes)


class RawOutputDataConfig(_common.FlyteIdlEntity):
    def __init__(self, output_location_prefix):
        """
        :param Text output_location_prefix: Location of offloaded data for things like S3, etc.
        """
        self._output_location_prefix = output_location_prefix

    @property
    def output_location_prefix(self):
        return self._output_location_prefix

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.admin.common_pb2.Auth
        """
        return _common_pb2.RawOutputDataConfig(output_location_prefix=self.output_location_prefix)

    @classmethod
    def from_flyte_idl(cls, pb2):
        return cls(output_location_prefix=pb2.output_location_prefix)
