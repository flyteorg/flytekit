from __future__ import absolute_import

import os

from flyteidl.core import errors_pb2
from mock import MagicMock, patch, PropertyMock

from flytekit.common import constants, utils
from flytekit.common.exceptions import scopes
from flytekit.configuration import TemporaryConfiguration
from flytekit.engines.flyte import engine
from flytekit.models import literals, execution as _execution_models, common as _common_models, launch_plan as \
    _launch_plan_models
from flytekit.models.core import errors, identifier


@scopes.system_entry_point
def _raise_system_exception(*args, **kwargs):
    raise ValueError("errorERRORerror")


@scopes.user_entry_point
def _raise_user_exception(*args, **kwargs):
    raise ValueError("userUSERuser")


@scopes.system_entry_point
def test_task_system_failure():
    with TemporaryConfiguration(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../common/configs/local.config'),
        internal_overrides={
            'image': 'myflyteimage:{}'.format(
                os.environ.get('IMAGE_VERSION', 'sha')
            ),
            'project': 'myflyteproject',
            'domain': 'development'
        }
    ):
        m = MagicMock()
        m.execute = _raise_system_exception

        with utils.AutoDeletingTempDir("test") as tmp:
            engine.FlyteTask(m).execute(None, {'output_prefix': tmp.name})

            doc = errors.ErrorDocument.from_flyte_idl(
                utils.load_proto_from_file(errors_pb2.ErrorDocument, os.path.join(tmp.name, constants.ERROR_FILE_NAME))
            )
            assert doc.error.code == "SYSTEM:Unknown"
            assert doc.error.kind == errors.ContainerError.Kind.RECOVERABLE
            assert "errorERRORerror" in doc.error.message


@scopes.system_entry_point
def test_task_user_failure():
    with TemporaryConfiguration(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../common/configs/local.config'),
        internal_overrides={
            'image': 'flyteimage:{}'.format(
                os.environ.get('IMAGE_VERSION', 'sha')
            ),
            'project': 'myflyteproject',
            'domain': 'development'
        }
    ):
        m = MagicMock()
        m.execute = _raise_user_exception

        with utils.AutoDeletingTempDir("test") as tmp:
            engine.FlyteTask(m).execute(None, {'output_prefix': tmp.name})

            doc = errors.ErrorDocument.from_flyte_idl(
                utils.load_proto_from_file(errors_pb2.ErrorDocument, os.path.join(tmp.name, constants.ERROR_FILE_NAME))
            )
            assert doc.error.code == "USER:Unknown"
            assert doc.error.kind == errors.ContainerError.Kind.NON_RECOVERABLE
            assert "userUSERuser" in doc.error.message


@patch.object(engine._FlyteClientManager, '_CLIENT', new_callable=PropertyMock)
def test_execution_notification_overrides(mock_client_factory):
    mock_client = MagicMock()
    mock_client.create_execution = MagicMock(return_value=identifier.WorkflowExecutionIdentifier('xp', 'xd', 'xn'))
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.Identifier(
            identifier.ResourceType.LAUNCH_PLAN,
            "project",
            "domain",
            "name",
            "version"
        )
    )

    engine.FlyteLaunchPlan(m).execute(
        'xp', 'xd', 'xn', literals.LiteralMap({}), notification_overrides=[]
    )

    mock_client.create_execution.assert_called_once_with(
        'xp',
        'xd',
        'xn',
        _execution_models.ExecutionSpec(
            identifier.Identifier(
                identifier.ResourceType.LAUNCH_PLAN,
                "project",
                "domain",
                "name",
                "version"
            ),
            literals.LiteralMap({}),
            _execution_models.ExecutionMetadata(
                _execution_models.ExecutionMetadata.ExecutionMode.MANUAL,
                'sdk',
                0
            ),
            disable_all=True,
        )
    )


@patch.object(engine._FlyteClientManager, '_CLIENT', new_callable=PropertyMock)
def test_execution_notification_soft_overrides(mock_client_factory):
    mock_client = MagicMock()
    mock_client.create_execution = MagicMock(return_value=identifier.WorkflowExecutionIdentifier('xp', 'xd', 'xn'))
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.Identifier(
            identifier.ResourceType.LAUNCH_PLAN,
            "project",
            "domain",
            "name",
            "version"
        )
    )

    notification = _common_models.Notification([0, 1, 2], email=_common_models.EmailNotification(["me@place.com"]))

    engine.FlyteLaunchPlan(m).execute(
        'xp', 'xd', 'xn', literals.LiteralMap({}), notification_overrides=[notification]
    )

    mock_client.create_execution.assert_called_once_with(
        'xp',
        'xd',
        'xn',
        _execution_models.ExecutionSpec(
            identifier.Identifier(
                identifier.ResourceType.LAUNCH_PLAN,
                "project",
                "domain",
                "name",
                "version"
            ),
            literals.LiteralMap({}),
            _execution_models.ExecutionMetadata(
                _execution_models.ExecutionMetadata.ExecutionMode.MANUAL,
                'sdk',
                0
            ),
            notifications=_execution_models.NotificationList([notification]),
        )
    )


@patch.object(engine._FlyteClientManager, '_CLIENT', new_callable=PropertyMock)
def test_execution_label_overrides(mock_client_factory):
    mock_client = MagicMock()
    mock_client.create_execution = MagicMock(return_value=identifier.WorkflowExecutionIdentifier('xp', 'xd', 'xn'))
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.Identifier(
            identifier.ResourceType.LAUNCH_PLAN,
            "project",
            "domain",
            "name",
            "version"
        )
    )

    labels = _common_models.Labels({"my": "label"})
    engine.FlyteLaunchPlan(m).execute(
        'xp', 'xd', 'xn', literals.LiteralMap({}), notification_overrides=[], label_overrides=labels
    )

    mock_client.create_execution.assert_called_once_with(
        'xp',
        'xd',
        'xn',
        _execution_models.ExecutionSpec(
            identifier.Identifier(
                identifier.ResourceType.LAUNCH_PLAN,
                "project",
                "domain",
                "name",
                "version"
            ),
            literals.LiteralMap({}),
            _execution_models.ExecutionMetadata(
                _execution_models.ExecutionMetadata.ExecutionMode.MANUAL,
                'sdk',
                0
            ),
            disable_all=True,
            labels=labels,
        )
    )


@patch.object(engine._FlyteClientManager, '_CLIENT', new_callable=PropertyMock)
def test_execution_annotation_overrides(mock_client_factory):
    mock_client = MagicMock()
    mock_client.create_execution = MagicMock(return_value=identifier.WorkflowExecutionIdentifier('xp', 'xd', 'xn'))
    mock_client_factory.return_value = mock_client

    m = MagicMock()
    type(m).id = PropertyMock(
        return_value=identifier.Identifier(
            identifier.ResourceType.LAUNCH_PLAN,
            "project",
            "domain",
            "name",
            "version"
        )
    )

    annotations = _common_models.Annotations({"my": "annotation"})
    engine.FlyteLaunchPlan(m).execute(
        'xp', 'xd', 'xn', literals.LiteralMap({}), notification_overrides=[], annotation_overrides=annotations
    )

    mock_client.create_execution.assert_called_once_with(
        'xp',
        'xd',
        'xn',
        _execution_models.ExecutionSpec(
            identifier.Identifier(
                identifier.ResourceType.LAUNCH_PLAN,
                "project",
                "domain",
                "name",
                "version"
            ),
            literals.LiteralMap({}),
            _execution_models.ExecutionMetadata(
                _execution_models.ExecutionMetadata.ExecutionMode.MANUAL,
                'sdk',
                0
            ),
            disable_all=True,
            annotations=annotations,
        )
    )


@patch.object(engine._FlyteClientManager, '_CLIENT', new_callable=PropertyMock)
def test_fetch_launch_plan(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_launch_plan = MagicMock(
        return_value=_launch_plan_models.LaunchPlan(
            identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p1", "d1", "n1", "v1"),
            MagicMock(),
            MagicMock(),
        )
    )
    mock_client_factory.return_value = mock_client

    lp = engine.FlyteEngineFactory().fetch_launch_plan(
        identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p", "d", "n", "v")
    )
    assert lp.id == identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p1", "d1", "n1", "v1")

    mock_client.get_launch_plan.assert_called_once_with(
        identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p", "d", "n", "v")
    )


@patch.object(engine._FlyteClientManager, '_CLIENT', new_callable=PropertyMock)
def test_fetch_active_launch_plan(mock_client_factory):
    mock_client = MagicMock()
    mock_client.get_active_launch_plan = MagicMock(
        return_value=_launch_plan_models.LaunchPlan(
            identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p1", "d1", "n1", "v1"),
            MagicMock(),
            MagicMock(),
        )
    )
    mock_client_factory.return_value = mock_client

    lp = engine.FlyteEngineFactory().fetch_launch_plan(
        identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p", "d", "n", "")
    )
    assert lp.id == identifier.Identifier(identifier.ResourceType.LAUNCH_PLAN, "p1", "d1", "n1", "v1")

    mock_client.get_active_launch_plan.assert_called_once_with(
        _common_models.NamedEntityIdentifier("p", "d", "n")
    )
