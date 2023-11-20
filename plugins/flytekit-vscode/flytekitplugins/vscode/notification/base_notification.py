"""
routing mechanism (hard, need discussion)
1. route to the notification class
ex: sendgrid, slack. pageduty, etc.

// maybe use the __module__ in python!

methods:
1. send_notification
2. check_if_config_is_valid
3. get notification secret from flytekit

error handling:
1. use try... except... to catch the error

"""
from flytekit.loggers import logger

def get_notifier(notification_type: str):
    try:
        # return None if notification_type is empty
        if not notification_type:
            return None

        logger.info("@@@ using get_notifier")
        module_name = f"{notification_type}_notification"
        module = __import__(module_name)
        return getattr(module, f"{notification_type.capitalize()}Notifier")
    except:
        logger.error(f"Cannot find the notification class for {notification_type}")
        return None

class BaseNotifier:
    def send_notification(self, message, token, notification_config: dict[str, str]):
        raise NotImplementedError("send_notification function should be implemented by subclasses.")

    def get_notification_secret(self, notification_type: str) -> str:
        raise NotImplementedError("get_notification_secret function should be implemented by subclasses.")

