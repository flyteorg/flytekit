import datetime
from datetime import timezone, timedelta
import textwrap

from flytekit import LiteralType
from flytekit.models import common as _common
from flytekit.models.core import execution as _execution

from flytekit.models.execution import ExecutionClosure

from flytekit.models.execution import LiteralMapBlob
from flytekit.models.literals import LiteralMap, Scalar, Primitive, Literal, RetryStrategy, LiteralCollection, Union
from flytekit.models.core.execution import WorkflowExecutionPhase
from flytekit.models.task import TaskMetadata, RuntimeMetadata
from flytekit.models.project import Project
from flytekit.models.types import SimpleType


def test_notification_email():
    obj = _common.EmailNotification(["a", "b", "c"])
    assert obj.recipients_email == ["a", "b", "c"]
    obj2 = _common.EmailNotification.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj


def test_notification_pagerduty():
    obj = _common.PagerDutyNotification(["a", "b", "c"])
    assert obj.recipients_email == ["a", "b", "c"]
    obj2 = _common.PagerDutyNotification.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj


def test_notification_slack():
    obj = _common.SlackNotification(["a", "b", "c"])
    assert obj.recipients_email == ["a", "b", "c"]
    obj2 = _common.SlackNotification.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj


def test_notification():
    phases = [
        _execution.WorkflowExecutionPhase.FAILED,
        _execution.WorkflowExecutionPhase.SUCCEEDED,
    ]
    recipients = ["a", "b", "c"]

    obj = _common.Notification(phases, email=_common.EmailNotification(recipients))
    assert obj.phases == phases
    assert obj.email.recipients_email == recipients
    obj2 = _common.Notification.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.phases == phases
    assert obj2.email.recipients_email == recipients

    obj = _common.Notification(phases, pager_duty=_common.PagerDutyNotification(recipients))
    assert obj.phases == phases
    assert obj.pager_duty.recipients_email == recipients
    obj2 = _common.Notification.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.phases == phases
    assert obj2.pager_duty.recipients_email == recipients

    obj = _common.Notification(phases, slack=_common.SlackNotification(recipients))
    assert obj.phases == phases
    assert obj.slack.recipients_email == recipients
    obj2 = _common.Notification.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.phases == phases
    assert obj2.slack.recipients_email == recipients


def test_labels():
    obj = _common.Labels({"my": "label"})
    assert obj.values == {"my": "label"}
    obj2 = _common.Labels.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj


def test_annotations():
    obj = _common.Annotations({"my": "annotation"})
    assert obj.values == {"my": "annotation"}
    obj2 = _common.Annotations.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj


def test_auth_role():
    obj = _common.AuthRole(assumable_iam_role="rollie-pollie")
    assert obj.assumable_iam_role == "rollie-pollie"
    assert not obj.kubernetes_service_account
    obj2 = _common.AuthRole.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2

    obj = _common.AuthRole(kubernetes_service_account="service-account-name")
    assert obj.kubernetes_service_account == "service-account-name"
    assert not obj.assumable_iam_role
    obj2 = _common.AuthRole.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2

    obj = _common.AuthRole(assumable_iam_role="rollie-pollie", kubernetes_service_account="service-account-name")
    assert obj.assumable_iam_role == "rollie-pollie"
    assert obj.kubernetes_service_account == "service-account-name"
    obj2 = _common.AuthRole.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2


def test_raw_output_data_config():
    obj = _common.RawOutputDataConfig("s3://bucket")
    assert obj.output_location_prefix == "s3://bucket"
    obj2 = _common.RawOutputDataConfig.from_flyte_idl(obj.to_flyte_idl())
    assert obj2 == obj


def test_auth_role_empty():
    # This test is here to ensure we can serialize launch plans with an empty auth role.
    # Auth roles are empty because they are filled in at registration time.
    obj = _common.AuthRole()
    x = obj.to_flyte_idl()
    y = _common.AuthRole.from_flyte_idl(x)
    assert y == obj


def test_short_string_raw_output_data_config():
    obj = _common.RawOutputDataConfig("s3://bucket")
    assert "Flyte Serialized object (RawOutputDataConfig):" in obj.short_string()
    assert "Flyte Serialized object (RawOutputDataConfig):" in repr(obj)


def test_html_repr_data_config():
    obj = _common.RawOutputDataConfig("s3://bucket")

    out = obj._repr_html_()
    assert "output_location_prefix: s3://bucket" in out
    assert "<h4>RawOutputDataConfig</h4>" in out


def test_short_string_entities_ExecutionClosure():
    _OUTPUT_MAP = LiteralMap(
        {"b": Literal(scalar=Scalar(primitive=Primitive(integer=2)))}
    )

    test_datetime = datetime.datetime(year=2022, month=1, day=1, tzinfo=timezone.utc)
    test_timedelta = datetime.timedelta(seconds=10)
    test_outputs = LiteralMapBlob(values=_OUTPUT_MAP, uri="http://foo/")

    obj = ExecutionClosure(
        phase=WorkflowExecutionPhase.SUCCEEDED,
        started_at=test_datetime,
        duration=test_timedelta,
        outputs=test_outputs,
        created_at=None,
        updated_at=test_datetime,
    )
    expected_result = textwrap.dedent("""\
    Flyte Serialized object (ExecutionClosure):
      outputs:
        uri: http://foo/
      phase: 4
      started_at:
        seconds: 1640995200
      duration:
        seconds: 10
      updated_at:
        seconds: 1640995200""")

    assert repr(obj) == expected_result
    assert obj.short_string() == expected_result


def test_short_string_entities_Primitive():
    obj = Primitive(integer=1)
    expected_result = textwrap.dedent("""\
    Flyte Serialized object (Primitive):
      integer: 1""")

    assert repr(obj) == expected_result
    assert obj.short_string() == expected_result


def test_short_string_literal():
    int_literal = Literal(scalar=Scalar(primitive=Primitive(integer=1)))
    assert repr(int_literal) == int_literal.short_string()
    assert repr(int_literal) == "Flyte Serialized object (int): 1"

    datetime_literal = Literal(scalar=Scalar(primitive=Primitive(datetime=timedelta(days=1))))
    assert repr(datetime_literal) == datetime_literal.short_string()
    assert repr(datetime_literal) == "Flyte Serialized object (datetime): 1970-01-01 00:00:00+00:00"

    str_literal = Literal(scalar=Scalar(primitive=Primitive(string_value="hello")))
    assert repr(str_literal) == str_literal.short_string()
    assert repr(str_literal) == "Flyte Serialized object (str): hello"

    small_list = Literal(collection=LiteralCollection([Literal(scalar=Scalar(primitive=Primitive(integer=1))), Literal(scalar=Scalar(primitive=Primitive(integer=2)))]))
    assert repr(small_list) == small_list.short_string()
    assert repr(small_list) == "Flyte Serialized object (Collection[int]): [1, 2]"

    large_list = Literal(collection=LiteralCollection([Literal(scalar=Scalar(primitive=Primitive(integer=i))) for i in range(100)]))
    assert repr(large_list) == large_list.short_string()
    assert repr(large_list) == "Flyte Serialized object (Collection[int]): [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, [...]"

    small_dict = Literal(map=LiteralMap({"a": Literal(scalar=Scalar(primitive=Primitive(integer=1))), "b": Literal(scalar=Scalar(primitive=Primitive(integer=2)))}))
    assert repr(small_dict) == small_dict.short_string()
    assert repr(small_dict) == "Flyte Serialized object (Map[str, int]): {'a': 1, 'b': 2}"

    large_dict = Literal(map=LiteralMap({str(i): Literal(scalar=Scalar(primitive=Primitive(integer=i))) for i in range(100)}))
    assert repr(large_dict) == large_dict.short_string()
    assert repr(large_dict) == "Flyte Serialized object (Map[str, int]): {'0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, [...]"

    union_literal = Literal(scalar=Scalar(union=Union(large_list, LiteralType(collection_type=SimpleType.INTEGER))))
    assert repr(union_literal) == union_literal.short_string()
    assert repr(union_literal) == "Flyte Serialized object (Union[Collection[int]]): [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, [...]"


def test_short_string_entities_TaskMetadata():
    obj = TaskMetadata(
        True,
        RuntimeMetadata(RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
        (),
    )

    expected_result = textwrap.dedent("""\
    Flyte Serialized object (TaskMetadata):
      discoverable: True
      runtime:
        type: 1
        version: 1.0.0
        flavor: python
      timeout:
        seconds: 86400
      retries:
        retries: 3
      discovery_version: 0.1.1b0
      deprecated_error_message: This is deprecated!
      interruptible: True
      cache_serializable: True
      pod_template_name: A""")
    assert repr(obj) == expected_result
    assert obj.short_string() == expected_result


def test_short_string_entities_Project():
    obj = Project("project_id", "project_name", "project_description")
    expected_result = textwrap.dedent("""\
    Flyte Serialized object (Project):
      id: project_id
      name: project_name
      description: project_description""")

    assert repr(obj) == expected_result
    assert obj.short_string() == expected_result
