import http

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from flytekit.loggers import logger

from .base_notification import BaseNotifier, get_notification_secret


class SendgridNotifier(BaseNotifier):
    def send_notification(self, message: str, notification_conf: dict[str, str]):
        try:
            token = get_notification_secret("sendgrid-api")
            sg = SendGridAPIClient(token)
            message = Mail(
                from_email=notification_conf["from_email"],
                to_emails=notification_conf["to_email"],
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
                f"Failed to send email notification, please check the variable in sendgrid_conf and the sendgrid-api token.\n\
                    Error: {e}"
            )
