import http
from dataclasses import dataclass
from typing import Optional

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

import flytekit
from flytekit.loggers import logger

from .base_notifier import BaseNotifier


@dataclass
class SendgridConfig(object):
    from_email: str
    to_email: str
    secret_group: str
    secret_key: str
    subject: Optional[str] = None

    def __post_init__(self):
        if self.subject is None:
            self.subject = "VSCode Server Notification"


class SendgridNotifier(BaseNotifier):
    def __init__(self, sendgrid_conf: SendgridConfig):
        self.sendgrid_conf = sendgrid_conf

    def send_notification(self, message: str):
        try:
            token = self.get_notification_secret()
            sg = SendGridAPIClient(token)

            message = Mail(
                from_email=self.sendgrid_conf.from_email,
                to_emails=self.sendgrid_conf.to_email,
                subject="VSCode Server Notification",
                plain_text_content=message,
            )

            response = sg.send(message)

            if response.status_code != http.HTTPStatus.ACCEPTED:
                logger.error(
                    f"Failed to send email notification.\n\
                        Status Code: {response.status_code},\
                        Response Body: {response.body},\
                        Response Headers: {response.headers}"
                )

            logger.info("Email notification sent successfully!")
        except Exception as e:
            logger.error(
                f"Failed to send email notification, please check the variable in the sendgrid_conf and the sendgrid-api token.\n\
                    Error: {e}"
            )

    def get_notification_secret(self) -> str:
        return flytekit.current_context().secrets.get(self.sendgrid_conf.secret_group, self.sendgrid_conf.secret_key)
