from .base_notification import BaseNotifier
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from flytekit.loggers import logger
import flytekit
import http

# todo: add a init function to tell the users the arguments should be passed
class SendgridNotifier(BaseNotifier):
    def send_notification(self, message:str, notification_config: dict[str, str]):
        try:
            logger.info("@@@ start send_notification")
            sg = SendGridAPIClient(self.get_notification_secret("sendgrid-api"))
            message = Mail(
                from_email=notification_config["from_email"],
                to_emails=notification_config["to_email"],
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
        except:
            logger.error(
                "Failed to send email notification, please check the variable in sendgrid_conf and the sendgrid token."
            )

    def get_notification_secret(self, notification_type: str) -> str:
        try:
            return flytekit.current_context().secrets.get(notification_type, "token")
        except:
            logger.error(f"Cannot find the {notification_type} notification secret")
            return ""

