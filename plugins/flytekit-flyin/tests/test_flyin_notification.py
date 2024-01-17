import mock
from flytekitplugins.flyin import (NotifierExecutor, SendgridConfig,
                                   SendgridNotifier, SlackConfig,
                                   SlackNotifier)


def test_notifier_and_config():
    mocked_token = "mocked-sendgrid-api-key"
    mocked_context = mock.patch("flytekit.current_context", autospec=True).start()
    mocked_context.return_value.secrets.get.return_value = mocked_token

    sendgrid_notifier = SendgridNotifier(
        sendgrid_conf=SendgridConfig(
            from_email="flyte-flyin-from@gmail.com",
            to_email="flyte-flyin-to@gmail.com",
            secret_group="sendgrid-api",
            secret_key="token",
            subject="VSCode Server Notification",
        )
    )

    assert sendgrid_notifier.get_notification_secret() == "mocked-sendgrid-api-key"
    assert sendgrid_notifier.sendgrid_conf.from_email == "flyte-flyin-from@gmail.com"
    assert sendgrid_notifier.sendgrid_conf.to_email == "flyte-flyin-to@gmail.com"
    assert sendgrid_notifier.sendgrid_conf.secret_group == "sendgrid-api"
    assert sendgrid_notifier.sendgrid_conf.secret_key == "token"
    assert sendgrid_notifier.sendgrid_conf.subject == "VSCode Server Notification"

    slack_notifier = SlackNotifier(
        slack_conf=SlackConfig(
            channel="demo",
            secret_group="slack-api",
            secret_key="token",
        )
    )

    assert slack_notifier.slack_conf.channel == "demo"
    assert slack_notifier.slack_conf.secret_group == "slack-api"
    assert slack_notifier.slack_conf.secret_key == "token"

    mocked_send_notification = mock.patch("sendgrid.SendGridAPIClient.send", autospec=True).start()
    mocked_send_notification.return_value = None
    sendgrid_notifier.send_notification("This is a test message")
    mocked_send_notification.assert_called_once()

    # Use sendgrid notifier to test NotifierExecutor
    notifier = NotifierExecutor(
        sendgrid_notifier, max_idle_seconds=60, warning_seconds_before_termination=20
    )
    notifier.handle(idle_time=60)
    assert mocked_send_notification.call_count == 3
